package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/franco-bianco/go-hyperliquid/hyperliquid"
	"hyper-trade/internal/hlapi"
	"hyper-trade/internal/hltrade_native"
)

// # 暂停 BTCUSDT
// curl "http://localhost:2112/control/pause?symbol=BTCUSDT"

// # 恢复
// curl "http://localhost:2112/control/resume?symbol=BTCUSDT"

// # 查看黑名单
// curl "http://localhost:2112/control/list"

// screen -S trade-btc
// # 运行策略
// go run ./binance_ws.go
// 分离：Ctrl+A 再按 D
// 返回：screen -r trade-btc
// 列表：screen -ls
// 结束：在会话内 Ctrl+C 或 screen -X -S trade-btc quit

/********** 策略参数 **********/
const (
	defaultWindowMs    = 500
	defaultThreshold   = 0.0035
	defaultCooldownSec = 10.0

	readDeadline = 65 * time.Second

	// 主动ping：默认关闭（不必需）。若网络/NAT需要，可设10s（注意5 msg/s入站限制）。
	clientPingInterval = 0 * time.Second

	// Binance 24h 强制断线；我们在此时间点前后“无缝滚动重连”
	reconnectTarget     = 23*time.Hour + 50*time.Minute // 23h50m
	reconnectJitterSpan = 4 * time.Minute               // ±2m 抖动窗口
)

/********** WebSocket payloads **********/
type tradeMsg struct {
	EventType string `json:"e"` // "trade"
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Qty       string `json:"q"`
	TradeTime int64  `json:"T"` // 毫秒
	IsMaker   bool   `json:"m"`
	Ignore    bool   `json:"M"`
}

/********** 单调队列（O(1) 维护窗口极值） **********/
type node struct {
	ts    int64
	price float64
}
type monoDeque struct {
	isMin bool
	q     []node
}

func newMonoDeque(isMin bool, capHint int) *monoDeque {
	return &monoDeque{isMin: isMin, q: make([]node, 0, capHint)}
}
func (d *monoDeque) push(n node) {
	for l := len(d.q); l > 0; l = len(d.q) {
		last := d.q[l-1]
		if d.isMin {
			if last.price <= n.price {
				break
			}
		} else {
			if last.price >= n.price {
				break
			}
		}
		d.q = d.q[:l-1]
	}
	d.q = append(d.q, n)
}
func (d *monoDeque) popExpired(cutoff int64) {
	for len(d.q) > 0 && d.q[0].ts < cutoff {
		d.q = d.q[1:]
	}
}
func (d *monoDeque) front() (node, bool) {
	if len(d.q) == 0 {
		return node{}, false
	}
	return d.q[0], true
}

/********** Prometheus Metrics **********/
var (
	wsUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "ws_connection_up", Help: "WebSocket up(1)/down(0) per symbol"},
		[]string{"symbol"},
	)
	wsReconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "ws_reconnects_total", Help: "Reconnects per symbol"},
		[]string{"symbol"},
	)
	wsReadErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "ws_read_errors_total", Help: "WebSocket read errors per symbol"},
		[]string{"symbol"},
	)
	ticksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "ticks_total", Help: "ticks received per symbol"},
		[]string{"symbol"},
	)
	lastTickMs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "last_tick_ms", Help: "last trade time (ms) from exchange per symbol"},
		[]string{"symbol"},
	)
	tickLagMs = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tick_lag_ms",
			Help:    "now()-trade_time(ms) per symbol",
			Buckets: []float64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233}, // 斐波那契型毫秒桶
		},
		[]string{"symbol"},
	)
	signalsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "signals_total", Help: "signals by direction per symbol"},
		[]string{"symbol", "direction"},
	)
)

var pausedSymbols sync.Map

func init() {
	prometheus.MustRegister(wsUp, wsReconnects, wsReadErrors, ticksTotal, lastTickMs, tickLagMs, signalsTotal)
}

func isSymbolPaused(symbol string) bool {
	_, ok := pausedSymbols.Load(strings.ToUpper(symbol))
	return ok
}

func pauseSymbol(symbol string) {
	if symbol == "" {
		return
	}
	pausedSymbols.Store(strings.ToUpper(symbol), struct{}{})
	log.Printf("[control] paused %s", strings.ToUpper(symbol))
}

func resumeSymbol(symbol string) {
	if symbol == "" {
		return
	}
	pausedSymbols.Delete(strings.ToUpper(symbol))
	log.Printf("[control] resumed %s", strings.ToUpper(symbol))
}

/********** Hyperliquid order execution **********/
type tradeConfig struct {
	PrivateKey          string   `json:"privateKey"`
	AccountAddress      string   `json:"address"`
	Testnet             bool     `json:"testnet"`
	DefaultNotionalUSDC float64  `json:"defaultNotionalUSDC"`
	WindowMs            int      `json:"windowMs"`
	AmplitudeThreshold  float64  `json:"amplitudeThreshold"`
	CooldownSeconds     float64  `json:"cooldownSeconds"`
	TakeProfitPct       float64  `json:"tp"`
	TakeProfitUSDC      float64  `json:"tpUsd"`
	StopLossPct         float64  `json:"sl"`
	StopLossUSDC        float64  `json:"slUsd"`
	TakeProfitMkt       bool     `json:"tpMarket"`
	StopLossMkt         bool     `json:"slMarket"`
	ReduceOnly          bool     `json:"reduceOnly"`
	VaultAddress        string   `json:"vaultAddress"`
	Blacklist           []string `json:"blacklist"`
}

type orderExecutor struct {
	client     *hltrade_native.Client
	infoClient *hyperliquid.InfoAPI
	ws         *hyperliquid.WebSocketAPI
	cfg        tradeConfig

	midMu sync.RWMutex
	mids  map[string]float64

	bboMu   sync.RWMutex
	bbo     map[string]bestQuote
	bboOnce map[string]*sync.Once
}

type bestQuote struct {
	Bid float64
	Ask float64
}

func newOrderExecutor(cfg tradeConfig) (*orderExecutor, error) {
	if cfg.PrivateKey == "" || cfg.AccountAddress == "" {
		return nil, fmt.Errorf("config missing private key or account address")
	}
	signer, err := hltrade_native.NewExchangeSignerFromHex(cfg.PrivateKey, cfg.AccountAddress, cfg.Testnet)
	if err != nil {
		return nil, err
	}
	clientCfg := hltrade_native.Config{
		AccountAddress:      signer.Address(),
		IsTestnet:           cfg.Testnet,
		DefaultNotionalUSDC: cfg.DefaultNotionalUSDC,
		VaultAddress:        cfg.VaultAddress,
	}
	client := hltrade_native.NewClient(clientCfg, signer)
	infoAPI := hyperliquid.NewInfoAPI(!cfg.Testnet)
	infoAPI.SetAccountAddress(signer.Address())

	exec := &orderExecutor{
		client:     client,
		infoClient: infoAPI,
		cfg:        cfg,
		mids:       make(map[string]float64),
		bbo:        make(map[string]bestQuote),
		bboOnce:    make(map[string]*sync.Once),
	}

	wsAPI := hyperliquid.NewWebSocketAPI(!cfg.Testnet)
	if err := wsAPI.Connect(); err != nil {
		return nil, fmt.Errorf("connect hyperliquid ws: %w", err)
	}
	if err := wsAPI.SubscribeToAllMids(exec.handleWsMids); err != nil {
		return nil, fmt.Errorf("subscribe allMids: %w", err)
	}
	exec.ws = wsAPI

	// Initial snapshot via REST to warm cache.
	if err := exec.refreshMids(); err != nil {
		log.Printf("[order] initial mid refresh failed: %v", err)
	}

	return exec, nil
}

func (o *orderExecutor) handleSignal(sig signal) {
	coin := mapSymbolToCoin(sig.Symbol)
	if coin == "" {
		log.Printf("[signal] ignored unsupported symbol %s", sig.Symbol)
		return
	}

	o.ensureBBO(coin)
	mid, err := o.midPrice(coin)
	if err != nil {
		log.Printf("[signal] skip %s, mid price unavailable: %v", coin, err)
		return
	}

	notional := o.cfg.DefaultNotionalUSDC
	if notional < 0.0001 {
		notional = 10
	}

	params := hltrade_native.PlaceParams{
		Coin:           coin,
		SideBuy:        sig.Direction == "LONG",
		LimitPx:        o.pickPrice(coin, sig.Direction == "LONG", mid),
		NotionalUSDC:   notional,
		ReduceOnly:     o.cfg.ReduceOnly,
		TakeProfitPct:  o.cfg.TakeProfitPct,
		TakeProfitUSDC: o.cfg.TakeProfitUSDC,
		StopLossPct:    o.cfg.StopLossPct,
		StopLossUSDC:   o.cfg.StopLossUSDC,
		TakeProfitMkt:  o.cfg.TakeProfitMkt,
		StopLossMkt:    o.cfg.StopLossMkt,
		VaultAccountID: o.cfg.VaultAddress,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	submitStart := time.Now()
	var signalDelay, emitDelay, tickDelay int64
	if !sig.ReceivedTime.IsZero() {
		signalDelay = durationMs(submitStart.Sub(sig.ReceivedTime))
	}
	if !sig.EmitTime.IsZero() {
		emitDelay = durationMs(submitStart.Sub(sig.EmitTime))
	}
	if !sig.TickTime.IsZero() {
		tickDelay = durationMs(submitStart.Sub(sig.TickTime))
	}
	log.Printf("[latency] %s %s request start signalDelay=%dms emitDelay=%dms tickDelay=%dms",
		coin, sig.Direction, signalDelay, emitDelay, tickDelay)

	res, err := o.client.PlaceLimitIOCAndTPSL(ctx, params)
	if err != nil {
		submitElapsed := durationMs(time.Since(submitStart))
		log.Printf("[order] %s %s %.2fUSDC @%.2f FAILED after %dms: %v",
			coin, sig.Direction, notional, mid, submitElapsed, err)
		return
	}
	submitElapsed := durationMs(time.Since(submitStart))
	var totalSinceEmit, totalSinceTick int64
	if !sig.EmitTime.IsZero() {
		totalSinceEmit = durationMs(time.Since(sig.EmitTime))
	}
	if !sig.TickTime.IsZero() {
		totalSinceTick = durationMs(time.Since(sig.TickTime))
	}
	log.Printf("[order] %s %s qty=%.6f avg=%.4f oid=%d tp=%s sl=%s http=%dms totalEmit=%dms totalTick=%dms",
		coin, sig.Direction, res.FilledSz, res.AvgPx, res.OrderID, res.TakeProfitCloid, res.StopLossCloid,
		submitElapsed, totalSinceEmit, totalSinceTick)

	go o.watchPosition(context.Background(), coin, sig.Direction == "LONG", res.OrderID, sig.EmitTime)
}

func mapSymbolToCoin(symbol string) string {
	const suffix = "USDT"
	if strings.HasSuffix(symbol, suffix) {
		return strings.TrimSuffix(symbol, suffix)
	}
	return symbol
}

func loadTradeConfig(path string) (tradeConfig, error) {
	var cfg tradeConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.DefaultNotionalUSDC <= 0 {
		cfg.DefaultNotionalUSDC = 10
	}
	if cfg.WindowMs <= 0 {
		cfg.WindowMs = defaultWindowMs
	}
	if cfg.AmplitudeThreshold <= 0 {
		cfg.AmplitudeThreshold = defaultThreshold
	}
	if cfg.CooldownSeconds <= 0 {
		cfg.CooldownSeconds = defaultCooldownSec
	}
	return cfg, nil
}

func (o *orderExecutor) handleWsMids(mids hyperliquid.AllMids) {
	o.midMu.Lock()
	for coin, pxStr := range mids.Mids {
		if px, err := strconv.ParseFloat(pxStr, 64); err == nil && px > 0 {
			o.mids[coin] = px
		}
	}
	o.midMu.Unlock()
}

func (o *orderExecutor) refreshMids() error {
	midsPtr, err := o.infoClient.GetAllMids()
	if err != nil || midsPtr == nil {
		return err
	}

	o.midMu.Lock()
	for k, v := range *midsPtr {
		if price, err := strconv.ParseFloat(v, 64); err == nil && price > 0 {
			o.mids[k] = price
		}
	}
	o.midMu.Unlock()
	return nil
}

func (o *orderExecutor) midPrice(coin string) (float64, error) {
	o.midMu.RLock()
	price, ok := o.mids[coin]
	o.midMu.RUnlock()
	if ok && price > 0 {
		return price, nil
	}

	// cache miss: refresh synchronously once
	if err := o.refreshMids(); err != nil {
		return 0, err
	}
	o.midMu.RLock()
	price, ok = o.mids[coin]
	o.midMu.RUnlock()
	if !ok || price <= 0 {
		return 0, fmt.Errorf("mid not available")
	}
	return price, nil
}

func (o *orderExecutor) ensureBBO(coin string) {
	o.bboMu.Lock()
	once, ok := o.bboOnce[coin]
	if !ok {
		once = &sync.Once{}
		o.bboOnce[coin] = once
	}
	o.bboMu.Unlock()

	once.Do(func() {
		if err := o.ws.SubscribeToBbo(coin, o.handleBbo); err != nil {
			log.Printf("[ws] subscribe bbo %s failed: %v", coin, err)
		}
	})
}

func (o *orderExecutor) handleBbo(bbo hyperliquid.WsBbo) {
	var best bestQuote
	if len(bbo.Bbo) >= 2 {
		// Hyperliquid sends [ask, bid] ordering for BBO levels.
		if bbo.Bbo[0] != nil {
			if ask, err := strconv.ParseFloat(bbo.Bbo[0].Px, 64); err == nil && ask > 0 {
				best.Ask = ask
			}
		}
		if bbo.Bbo[1] != nil {
			if bid, err := strconv.ParseFloat(bbo.Bbo[1].Px, 64); err == nil && bid > 0 {
				best.Bid = bid
			}
		}
	}
	o.bboMu.Lock()
	o.bbo[bbo.Coin] = best
	o.bboMu.Unlock()
}

func (o *orderExecutor) bestBidAsk(coin string) (bid, ask float64, ok bool) {
	o.bboMu.RLock()
	best, exist := o.bbo[coin]
	o.bboMu.RUnlock()
	if !exist {
		return 0, 0, false
	}
	if best.Bid <= 0 || best.Ask <= 0 {
		return 0, 0, false
	}
	return best.Bid, best.Ask, true
}

func (o *orderExecutor) pickPrice(coin string, buy bool, fallback float64) float64 {
	if bid, ask, ok := o.bestBidAsk(coin); ok {
		if buy {
			return ask
		}
		return bid
	}
	return fallback
}

func (o *orderExecutor) watchPosition(parentCtx context.Context, coin string, isLong bool, orderID int64, signalTime time.Time) {
	const (
		waitDuration  = 1 * time.Minute
		retryInterval = 5 * time.Second
		maxAttempts   = 10
	)
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	timeout := time.NewTimer(waitDuration)
	defer timeout.Stop()

	select {
	case <-parentCtx.Done():
		return
	case <-timeout.C:
	}

	var signalAge int64
	if !signalTime.IsZero() {
		signalAge = durationMs(time.Since(signalTime))
	}
	log.Printf("[risk] %s timed close triggered (order=%d, signalAge=%dms)", coin, orderID, signalAge)

	var lastSize float64
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if parentCtx.Err() != nil {
			return
		}

		state, err := o.infoClient.GetAccountState()
		if err != nil {
			log.Printf("[risk] fetch account state error (attempt %d %s): %v", attempt, coin, err)
			continue
		}

		var size float64
		for _, ap := range state.AssetPositions {
			pos := ap.Position
			if strings.EqualFold(pos.Coin, coin) && pos.Szi != 0 {
				if (isLong && pos.Szi > 0) || (!isLong && pos.Szi < 0) {
					size = math.Abs(pos.Szi)
					break
				}
			}
		}
		if size == 0 {
			log.Printf("[risk] %s position already flat before attempt %d", coin, attempt)
			return
		}
		lastSize = size

		o.ensureBBO(coin)
		mid, err := o.midPrice(coin)
		if err != nil {
			log.Printf("[risk] %s mid unavailable on attempt %d: %v", coin, attempt, err)
			continue
		}
		price := o.pickPrice(coin, !isLong, mid)

		ctxOrder, cancelOrder := context.WithTimeout(context.Background(), 5*time.Second)
		params := hltrade_native.PlaceParams{
			Coin:          coin,
			SideBuy:       !isLong,
			LimitPx:       price,
			NotionalUSDC:  size * mid,
			ReduceOnly:    true,
			TakeProfitPct: 0,
			StopLossPct:   0,
		}
		log.Printf("[risk] %s timed close attempt %d qty=%.6f px=%.4f", coin, attempt, size, price)
		_, err = o.client.PlaceLimitIOCAndTPSL(ctxOrder, params)
		cancelOrder()
		if err != nil {
			log.Printf("[risk] %s timed close attempt %d failed: %v", coin, attempt, err)
		} else {
			log.Printf("[risk] %s timed close attempt %d submitted", coin, attempt)
		}

		select {
		case <-parentCtx.Done():
			return
		case <-ticker.C:
			continue
		}
	}

	// fallback: place market-style order by using wide price.
	log.Printf("[risk] %s entering market fallback after %d timed close attempts", coin, maxAttempts)
	ctxOrder, cancelOrder := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelOrder()

	o.ensureBBO(coin)
	mid, err := o.midPrice(coin)
	if err != nil {
		log.Printf("[risk] forced close fallback mid unavailable: %v", err)
		return
	}

	if lastSize <= 0 {
		return
	}

	fallbackPx := o.pickPrice(coin, !isLong, mid)
	if fallbackPx <= 0 {
		fallbackPx = mid
	}
	if isLong {
		fallbackPx *= 0.95
	} else {
		fallbackPx *= 1.05
	}

	params := hltrade_native.PlaceParams{
		Coin:         coin,
		SideBuy:      !isLong,
		LimitPx:      fallbackPx,
		NotionalUSDC: lastSize * mid,
		ReduceOnly:   true,
	}
	if _, err := o.client.PlaceLimitIOCAndTPSL(ctxOrder, params); err != nil {
		log.Printf("[risk] %s fallback close failed (qty=%.6f px=%.4f): %v", coin, lastSize, fallbackPx, err)
	} else {
		log.Printf("[risk] %s fallback close submitted (qty=%.6f px=%.4f)", coin, lastSize, fallbackPx)
	}
}

// --- worker & signal handling ---
type signal struct {
	Symbol       string
	Direction    string  // "LONG" / "SHORT"
	Amplitude    float64 // 0.0035 = 0.35%
	TickTime     time.Time
	EmitTime     time.Time
	ReceivedTime time.Time
}

type SymbolWorker struct {
	symbol string
	out    chan<- signal
	logger *log.Logger

	// 内部状态
	stopCtx context.Context
	stopFn  context.CancelFunc

	// 滚动重连
	connIdx int64 // 递增连接编号，仅用于日志

	windowMs  int
	threshold float64
	cooldown  time.Duration
}

// 启动一个币种的长期 worker
func NewSymbolWorker(symbol string, out chan<- signal, logger *log.Logger, cfg tradeConfig) *SymbolWorker {
	ctx, cancel := context.WithCancel(context.Background())
	window := cfg.WindowMs
	if window <= 0 {
		window = defaultWindowMs
	}
	threshold := cfg.AmplitudeThreshold
	if threshold <= 0 {
		threshold = defaultThreshold
	}
	cooldown := cfg.CooldownSeconds
	if cooldown <= 0 {
		cooldown = defaultCooldownSec
	}
	return &SymbolWorker{
		symbol:    strings.ToUpper(symbol),
		out:       out,
		logger:    logger,
		stopCtx:   ctx,
		stopFn:    cancel,
		windowMs:  window,
		threshold: threshold,
		cooldown:  time.Duration(cooldown * float64(time.Second)),
	}
}

func (w *SymbolWorker) Start() {
	go w.supervise()
}

func (w *SymbolWorker) Stop() {
	w.stopFn()
}

func (w *SymbolWorker) supervise() {
	for {
		if isSymbolPaused(w.symbol) {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		select {
		case <-w.stopCtx.Done():
			w.logger.Printf("[%s] stop requested", w.symbol)
			return
		default:
		}
		// 建立主连接
		if err := w.runOneSession(); err != nil {
			wsReadErrors.WithLabelValues(w.symbol).Inc()
			w.logger.Printf("[%s] session error: %v", w.symbol, err)
			// 指数退避
			time.Sleep(backoffDuration())
		}
	}
}

// 单次会话：包含滚动重连逻辑
func (w *SymbolWorker) runOneSession() error {
	if isSymbolPaused(w.symbol) {
		time.Sleep(500 * time.Millisecond)
		return nil
	}
	connA, url, err := w.dialOnce()
	if err != nil {
		return err
	}
	if isSymbolPaused(w.symbol) {
		_ = connA.Close()
		return nil
	}
	idxA := atomic.AddInt64(&w.connIdx, 1)
	w.logger.Printf("[%s][conn#%d] WS connected -> %s", w.symbol, idxA, url)
	wsUp.WithLabelValues(w.symbol).Set(1)

	// 读循环上下文
	_, cancelA := context.WithCancel(w.stopCtx)
	defer cancelA()

	// （可选）客户端 ping
	var pingStopA chan struct{}
	if clientPingInterval > 0 {
		pingStopA = make(chan struct{})
		go clientPingLoop(connA, pingStopA)
	}

	// 启动 A 的读循环（返回时说明A结束）
	msgChA := make(chan error, 1)
	go func() { msgChA <- w.readLoop(connA, idxA) }()

	// 计划滚动重连时间（23h50m ± 抖动）
	reconnectAt := time.Now().Add(reconnectTarget).Add(randJitter(reconnectJitterSpan))
	timer := time.NewTimer(time.Until(reconnectAt))
	defer timer.Stop()

	for {
		select {
		case <-w.stopCtx.Done():
			// 结束：关闭A
			w.logger.Printf("[%s][conn#%d] closing (stop)", w.symbol, idxA)
			closePing(pingStopA)
			_ = connA.Close()
			wsUp.WithLabelValues(w.symbol).Set(0)
			return nil

		case err := <-msgChA:
			// A 读出错/结束，直接退出本 session，由 supervise 重启
			closePing(pingStopA)
			_ = connA.Close()
			wsUp.WithLabelValues(w.symbol).Set(0)
			return err

		case <-timer.C:
			// 到滚动时间：先建新连接B
			connB, urlB, err := w.dialOnce()
			if err != nil {
				// 失败就稍后重试滚动
				w.logger.Printf("[%s][conn#%d] roll dial error: %v; retry in 30s", w.symbol, idxA, err)
				timer.Reset(30 * time.Second)
				continue
			}
			if isSymbolPaused(w.symbol) {
				_ = connB.Close()
				timer.Reset(30 * time.Second)
				continue
			}
			idxB := atomic.AddInt64(&w.connIdx, 1)
			w.logger.Printf("[%s][conn#%d] WS connected (rolling) -> %s", w.symbol, idxB, urlB)
			wsReconnects.WithLabelValues(w.symbol).Inc()

			// B 的 ping
			var pingStopB chan struct{}
			if clientPingInterval > 0 {
				pingStopB = make(chan struct{})
				go clientPingLoop(connB, pingStopB)
			}

			// 启动 B 的读循环
			msgChB := make(chan error, 1)
			go func() { msgChB <- w.readLoop(connB, idxB) }()

			// 等待 B 收到首条 tick（由 readLoop 打首条日志即可认为OK）
			// 我们这里给一个“宽限时间”，如果B在8秒内没报错，认为已接管，关A
			select {
			case <-time.After(8 * time.Second):
				if isSymbolPaused(w.symbol) {
					closePing(pingStopB)
					_ = connB.Close()
					timer.Reset(30 * time.Second)
					continue
				}
				w.logger.Printf("[%s][conn#%d] rolling: close old conn#%d", w.symbol, idxB, idxA)
				closePing(pingStopA)
				_ = connA.Close()
				wsUp.WithLabelValues(w.symbol).Set(1) // 连接仍up，由B承接
				// 切换到等待B结束
				connA, idxA, pingStopA, msgChA = connB, idxB, pingStopB, msgChB
				// 重新调度下一次滚动
				reconnectAt = time.Now().Add(reconnectTarget).Add(randJitter(reconnectJitterSpan))
				timer.Reset(time.Until(reconnectAt))
			case err := <-msgChB:
				// B 直接报错：放弃B，保持A，稍后再滚动
				closePing(pingStopB)
				_ = connB.Close()
				w.logger.Printf("[%s][conn#%d] rolling new conn error: %v", w.symbol, idxB, err)
				timer.Reset(30 * time.Second)
			case err := <-msgChA:
				// A 提前报错：切回 supervise 重启
				closePing(pingStopB)
				_ = connB.Close()
				wsUp.WithLabelValues(w.symbol).Set(0)
				return err
			}
		}
	}
}

func (w *SymbolWorker) dialOnce() (*websocket.Conn, string, error) {
	s := strings.ToLower(w.symbol)
	url := "wss://stream.binance.com:9443/ws/" + s + "@trade"

	d := *websocket.DefaultDialer
	d.ReadBufferSize = 64 * 1024
	d.WriteBufferSize = 8 * 1024
	d.HandshakeTimeout = 10 * time.Second

	conn, resp, err := d.Dial(url, nil)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		return nil, url, fmt.Errorf("dial failed (http=%d): %w", code, err)
	}

	// 读超时 + ping/pong
	_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
	conn.SetPingHandler(func(appData string) error {
		// 收到ping -> 立刻原样pong（复制payload）
		deadline := time.Now().Add(5 * time.Second)
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), deadline)
		if err == nil {
			_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		}
		return err
	})
	conn.SetPongHandler(func(_ string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})

	return conn, url, nil
}

// 单连接读循环 + 振幅逻辑
func (w *SymbolWorker) readLoop(conn *websocket.Conn, idx int64) error {
	minQ := newMonoDeque(true, 256)
	maxQ := newMonoDeque(false, 256)
	var lastSignal time.Time
	firstLogged := false

	for {
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			wsReadErrors.WithLabelValues(w.symbol).Inc()
			return fmt.Errorf("read (conn#%d): %w", idx, err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(readDeadline))

		var m tradeMsg
		if json.Unmarshal(msg, &m) != nil {
			continue
		}

		price, err := strconv.ParseFloat(m.Price, 64)
		if err != nil || price <= 0 {
			continue
		}

		if !firstLogged {
			firstLogged = true
			log.Printf("[%s][conn#%d] first tick ok: ts=%d price=%s", w.symbol, idx, m.TradeTime, m.Price)
		}

		// metrics
		now := time.Now()
		tickTime := time.UnixMilli(m.TradeTime)
		ticksTotal.WithLabelValues(w.symbol).Inc()
		lastTickMs.WithLabelValues(w.symbol).Set(float64(m.TradeTime))
		lag := float64(now.UnixMilli() - m.TradeTime)
		if lag >= 0 {
			tickLagMs.WithLabelValues(w.symbol).Observe(lag)
		}

		// 窗口极值
		cur := node{ts: m.TradeTime, price: price}
		minQ.push(cur)
		maxQ.push(cur)
		cutoff := m.TradeTime - int64(w.windowMs)
		minQ.popExpired(cutoff)
		maxQ.popExpired(cutoff)

		minN, okMin := minQ.front()
		maxN, okMax := maxQ.front()
		if !okMin || !okMax {
			continue
		}

		winMin, winMax := minN.price, maxN.price
		last := price

		var ampUp, ampDn float64
		if winMin > 0 {
			ampUp = (last - winMin) / winMin
		}
		if winMax > 0 {
			ampDn = (winMax - last) / winMax
		}

		cooldownElapsed := time.Since(lastSignal)
		if cooldownElapsed < w.cooldown {
			continue
		}

		if ampUp >= w.threshold {
			lastSignal = now
			signalsTotal.WithLabelValues(w.symbol, "LONG").Inc()
			calcDelay := now.Sub(tickTime)
			if !isSymbolPaused(w.symbol) {
				w.out <- signal{
					Symbol:    w.symbol,
					Direction: "LONG",
					Amplitude: ampUp,
					TickTime:  tickTime,
					EmitTime:  now,
				}
				log.Printf("[signal] %s LONG amp=%.3f%% (min=%.6f last=%.6f) calcDelay=%dms", w.symbol, ampUp*100, winMin, last, durationMs(calcDelay))
			} else {
				log.Printf("[signal] %s LONG suppressed (paused) amp=%.3f%% calcDelay=%dms", w.symbol, ampUp*100, durationMs(calcDelay))
			}
			continue
		}
		if ampDn >= w.threshold {
			lastSignal = now
			signalsTotal.WithLabelValues(w.symbol, "SHORT").Inc()
			calcDelay := now.Sub(tickTime)
			if !isSymbolPaused(w.symbol) {
				w.out <- signal{
					Symbol:    w.symbol,
					Direction: "SHORT",
					Amplitude: ampDn,
					TickTime:  tickTime,
					EmitTime:  now,
				}
				log.Printf("[signal] %s SHORT amp=%.3f%% (max=%.6f last=%.6f) calcDelay=%dms", w.symbol, ampDn*100, winMax, last, durationMs(calcDelay))
			} else {
				log.Printf("[signal] %s SHORT suppressed (paused) amp=%.3f%% calcDelay=%dms", w.symbol, ampDn*100, durationMs(calcDelay))
			}
		}
	}
}

/********** utils **********/
func backoffDuration() time.Duration {
	// 简单退避 + 抖动
	base := 700 * time.Millisecond
	jit := time.Duration(rand.Int63n(int64(400 * time.Millisecond)))
	return base + jit
}

func randJitter(span time.Duration) time.Duration {
	if span <= 0 {
		return 0
	}
	return time.Duration(int64(span)/2 - rand.Int63n(int64(span)))
}

func clientPingLoop(conn *websocket.Conn, stop chan struct{}) {
	t := time.NewTicker(clientPingInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			// 主动ping的payload建议留空（文档允许，但不能替代服务端的保活判断）
			_ = conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(3*time.Second))
		}
	}
}

func closePing(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func durationMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

/********** Demo main：多币种独立连接 + /metrics **********/
func main() {
	rand.Seed(time.Now().UnixNano())

	cfgPath := os.Getenv("HL_CONFIG_PATH")
	if strings.TrimSpace(cfgPath) == "" {
		cfgPath = "config.json"
	}
	tradeCfg, err := loadTradeConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config %s failed: %v", cfgPath, err)
	}
	executor, err := newOrderExecutor(tradeCfg)
	if err != nil {
		log.Fatalf("init hyperliquid client failed: %v", err)
	}
	log.Printf("hyperliquid: account=%s testnet=%v defaultNotional=%.2f", tradeCfg.AccountAddress, tradeCfg.Testnet, tradeCfg.DefaultNotionalUSDC)

	// 启动 /metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/control/pause", func(w http.ResponseWriter, r *http.Request) {
			sym := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
			if sym == "" {
				http.Error(w, "symbol required", http.StatusBadRequest)
				return
			}
			pauseSymbol(sym)
			fmt.Fprintf(w, "paused %s\n", sym)
		})
		http.HandleFunc("/control/resume", func(w http.ResponseWriter, r *http.Request) {
			sym := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("symbol")))
			if sym == "" {
				http.Error(w, "symbol required", http.StatusBadRequest)
				return
			}
			resumeSymbol(sym)
			fmt.Fprintf(w, "resumed %s\n", sym)
		})
		http.HandleFunc("/control/list", func(w http.ResponseWriter, r *http.Request) {
			var paused []string
			pausedSymbols.Range(func(key, value any) bool {
				if s, ok := key.(string); ok {
					paused = append(paused, s)
				}
				return true
			})
			if len(paused) == 0 {
				fmt.Fprintln(w, "no paused symbols")
				return
			}
			fmt.Fprintln(w, strings.Join(paused, ","))
		})
		addr := ":2112" // 可改为从环境变量读取
		log.Printf("metrics/control serving at http://localhost%s", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	// 1) 从 Hyperliquid 拉取永续标的，并自动拼接 USDT 以适配 Binance
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	symbols, err := hlapi.FetchPerpSymbolsWithUSDT(ctx, "")
	if err != nil {
		// 失败兜底：给几个种子，避免整程序起不来
		log.Printf("fetch hyperliquid symbols failed: %v; fallback to BTC/ETH", err)
		symbols = []string{"BTCUSDT", "ETHUSDT"}
	}
	log.Printf("loaded %d symbols", len(symbols))
	// 如果你只想跑其中一个子集，可在这里做筛选/限制数量

	sigCh := make(chan signal, 128)

	blacklist := make(map[string]struct{}, len(tradeCfg.Blacklist))
	for _, sym := range tradeCfg.Blacklist {
		blacklist[strings.ToUpper(sym)] = struct{}{}
	}

	// 启动每个币种的 worker
	var wg sync.WaitGroup
	for _, sym := range symbols {
		if _, skip := blacklist[strings.ToUpper(sym)]; skip {
			log.Printf("skip blacklisted symbol %s", sym)
			continue
		}
		wg.Add(1)
		w := NewSymbolWorker(sym, sigCh, log.Default(), tradeCfg)
		w.Start()
		go func() {
			defer wg.Done()
			<-w.stopCtx.Done()
		}()
	}

	// 信号消费：只输出 币种 方向 振幅
	go func() {
		for s := range sigCh {
			if isSymbolPaused(s.Symbol) {
				continue
			}
			recvAt := time.Now()
			var queueLatency, tickToRecv int64
			if !s.EmitTime.IsZero() {
				queueLatency = durationMs(recvAt.Sub(s.EmitTime))
			}
			if !s.TickTime.IsZero() {
				tickToRecv = durationMs(recvAt.Sub(s.TickTime))
			}
			log.Printf("[latency] %s %s signal received queue=%dms tickToRecv=%dms", s.Symbol, s.Direction, queueLatency, tickToRecv)
			fmt.Printf("SIGNAL %s %s %.3f%%\n", s.Symbol, s.Direction, s.Amplitude*100)
			s.ReceivedTime = recvAt
			go executor.handleSignal(s)
		}
	}()

	// Demo: 运行到 Ctrl+C
	select {}
}
