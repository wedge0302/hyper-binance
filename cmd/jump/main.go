package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/franco-bianco/go-hyperliquid/hyperliquid"

	"hyper-trade/internal/hltrade_native"
)

const (
	defaultJumpInterval  = "3m"
	defaultGapThreshold  = 0.0025
	defaultAggressiveBps = 5.0
	defaultNotionalUSDC  = 10.0
	requestTimeout       = 10 * time.Second
)

type jumpConfig struct {
	PrivateKey       string   `json:"privateKey"`
	AccountAddress   string   `json:"address"`
	Testnet          bool     `json:"testnet"`
	Symbols          []string `json:"symbols"`
	CandleInterval   string   `json:"candleInterval"`
	GapThreshold     float64  `json:"gapThreshold"`
	NotionalUSDC     float64  `json:"notionalUSDC"`
	AggressiveBps    float64  `json:"aggressiveBps"`
	TakeProfitPct    float64  `json:"takeProfitPct"`
	StopLossPct      float64  `json:"stopLossPct"`
	TakeProfitUSDC   float64  `json:"takeProfitUSDC"`
	StopLossUSDC     float64  `json:"stopLossUSDC"`
	TakeProfitMarket bool     `json:"takeProfitMarket"`
	StopLossMarket   bool     `json:"stopLossMarket"`
	ReduceOnly       bool     `json:"reduceOnly"`
	VaultAddress     string   `json:"vaultAddress"`
}

func loadJumpConfig(path string) (jumpConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return jumpConfig{}, err
	}
	var cfg jumpConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return jumpConfig{}, err
	}
	cfg.Symbols = normalizeSymbols(cfg.Symbols)
	if len(cfg.Symbols) == 0 {
		return jumpConfig{}, fmt.Errorf("config missing symbols list")
	}
	if cfg.CandleInterval == "" {
		cfg.CandleInterval = defaultJumpInterval
	}
	if cfg.GapThreshold <= 0 {
		cfg.GapThreshold = defaultGapThreshold
	}
	if cfg.NotionalUSDC <= 0 {
		cfg.NotionalUSDC = defaultNotionalUSDC
	}
	if cfg.AggressiveBps < 0 {
		cfg.AggressiveBps = defaultAggressiveBps
	}
	return cfg, nil
}

func normalizeSymbols(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, sym := range symbols {
		s := strings.TrimSpace(strings.ToUpper(sym))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

type orderExecutor struct {
	cfg        jumpConfig
	client     *hltrade_native.Client
	infoClient *hyperliquid.InfoAPI
	ws         *hyperliquid.WebSocketAPI

	bboMu   sync.RWMutex
	bbo     map[string]bestQuote
	bboOnce map[string]*sync.Once

	midMu sync.RWMutex
	mids  map[string]float64
}

type bestQuote struct {
	Bid float64
	Ask float64
}

func newOrderExecutor(cfg jumpConfig) (*orderExecutor, error) {
	if strings.TrimSpace(cfg.PrivateKey) == "" || strings.TrimSpace(cfg.AccountAddress) == "" {
		return nil, fmt.Errorf("privateKey and address are required")
	}

	signer, err := hltrade_native.NewExchangeSignerFromHex(cfg.PrivateKey, cfg.AccountAddress, cfg.Testnet)
	if err != nil {
		return nil, fmt.Errorf("init signer: %w", err)
	}

	clientCfg := hltrade_native.Config{
		AccountAddress:      signer.Address(),
		IsTestnet:           cfg.Testnet,
		DefaultNotionalUSDC: cfg.NotionalUSDC,
		VaultAddress:        cfg.VaultAddress,
	}
	client := hltrade_native.NewClient(clientCfg, signer)
	infoAPI := hyperliquid.NewInfoAPI(!cfg.Testnet)
	infoAPI.SetAccountAddress(signer.Address())

	wsAPI := hyperliquid.NewWebSocketAPI(!cfg.Testnet)
	if err := wsAPI.Connect(); err != nil {
		return nil, fmt.Errorf("connect hyperliquid ws: %w", err)
	}

	exec := &orderExecutor{
		cfg:        cfg,
		client:     client,
		infoClient: infoAPI,
		ws:         wsAPI,
		bbo:        make(map[string]bestQuote),
		bboOnce:    make(map[string]*sync.Once),
		mids:       make(map[string]float64),
	}

	if err := wsAPI.SubscribeToAllMids(exec.handleAllMids); err != nil {
		return nil, fmt.Errorf("subscribe allMids: %w", err)
	}
	if err := exec.refreshMids(); err != nil {
		log.Printf("[jump] initial mid refresh failed: %v", err)
	}

	return exec, nil
}

func (o *orderExecutor) close() {
	if o.ws != nil {
		_ = o.ws.Disconnect()
	}
}

func (o *orderExecutor) handleAllMids(mids hyperliquid.AllMids) {
	o.midMu.Lock()
	for coin, pxStr := range mids.Mids {
		if px, err := strconvParseFloat(pxStr); err == nil && px > 0 {
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
		if px, err := strconvParseFloat(v); err == nil && px > 0 {
			o.mids[k] = px
		}
	}
	o.midMu.Unlock()
	return nil
}

func (o *orderExecutor) midPrice(coin string) (float64, bool) {
	o.midMu.RLock()
	px, ok := o.mids[coin]
	o.midMu.RUnlock()
	return px, ok && px > 0
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
			log.Printf("[jump] subscribe bbo %s failed: %v", coin, err)
		}
	})
}

func (o *orderExecutor) handleBbo(bbo hyperliquid.WsBbo) {
	var best bestQuote
	if len(bbo.Bbo) >= 2 {
		if bbo.Bbo[0] != nil {
			if ask, err := strconvParseFloat(bbo.Bbo[0].Px); err == nil && ask > 0 {
				best.Ask = ask
			}
		}
		if bbo.Bbo[1] != nil {
			if bid, err := strconvParseFloat(bbo.Bbo[1].Px); err == nil && bid > 0 {
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

func (o *orderExecutor) aggressivePrice(coin string, sideBuy bool, fallback float64) float64 {
	bid, ask, ok := o.bestBidAsk(coin)
	price := fallback
	if ok {
		if sideBuy {
			price = ask
		} else {
			price = bid
		}
	}
	if price <= 0 {
		price = fallback
	}
	offset := o.cfg.AggressiveBps / 10000.0
	if price > 0 && offset > 0 {
		if sideBuy {
			price *= 1 + offset
		} else {
			price *= 1 - offset
		}
	}
	return price
}

func (o *orderExecutor) placeJumpOrder(coin string, sideBuy bool, reason string, gap float64, openPx float64, prevClose float64) error {
	o.ensureBBO(coin)
	mid, ok := o.midPrice(coin)
	if !ok || mid <= 0 {
		px, err := o.infoClient.GetMartketPx(coin)
		if err == nil && px > 0 {
			mid = px
		}
	}
	limitPx := o.aggressivePrice(coin, sideBuy, mid)
	if limitPx <= 0 {
		return fmt.Errorf("no valid price for %s", coin)
	}

	params := hltrade_native.PlaceParams{
		Coin:           coin,
		SideBuy:        sideBuy,
		LimitPx:        limitPx,
		NotionalUSDC:   o.cfg.NotionalUSDC,
		ReduceOnly:     o.cfg.ReduceOnly,
		TakeProfitPct:  o.cfg.TakeProfitPct,
		StopLossPct:    o.cfg.StopLossPct,
		TakeProfitUSDC: o.cfg.TakeProfitUSDC,
		StopLossUSDC:   o.cfg.StopLossUSDC,
		TakeProfitMkt:  o.cfg.TakeProfitMarket,
		StopLossMkt:    o.cfg.StopLossMarket,
		VaultAccountID: o.cfg.VaultAddress,
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dir := "LONG"
	if !sideBuy {
		dir = "SHORT"
	}

	start := time.Now()
	log.Printf("[jump] %s order start side=%s limit=%.6f notional=%.2f gap=%.3f%% reason=%s", coin, dir, limitPx, o.cfg.NotionalUSDC, gap*100, reason)

	res, err := o.client.PlaceLimitIOCAndTPSL(ctx, params)
	if err != nil {
		log.Printf("[jump] %s order failed side=%s elapsed=%dms: %v", coin, dir, durationMs(time.Since(start)), err)
		return err
	}

	log.Printf("[jump] %s order filled side=%s filled=%.6f avg=%.6f limit=%.6f elapsed=%dms gap=%.3f%% open=%.6f prevClose=%.6f reason=%s",
		coin, dir, res.FilledSz, res.AvgPx, limitPx, durationMs(time.Since(start)), gap*100, openPx, prevClose, reason)
	return nil
}

type symbolState struct {
	prevClose     float64
	lastCloseTime int64
	ready         bool
}

type jumpStrategy struct {
	cfg      jumpConfig
	exec     *orderExecutor
	interval time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.Mutex
	state   map[string]*symbolState

	channelMu sync.Mutex
	channels  map[string]chan hyperliquid.Candle
}

func newJumpStrategy(cfg jumpConfig, exec *orderExecutor) (*jumpStrategy, error) {
	dur, err := parseInterval(cfg.CandleInterval)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &jumpStrategy{
		cfg:      cfg,
		exec:     exec,
		interval: dur,
		ctx:      ctx,
		cancel:   cancel,
		state:    make(map[string]*symbolState, len(cfg.Symbols)),
		channels: make(map[string]chan hyperliquid.Candle, len(cfg.Symbols)),
	}, nil
}

func (s *jumpStrategy) start() {
	for _, coin := range s.cfg.Symbols {
		coin := coin
		s.exec.ensureBBO(coin)
		ch := make(chan hyperliquid.Candle, 64)
		s.channelMu.Lock()
		s.channels[coin] = ch
		s.channelMu.Unlock()
		go s.symbolLoop(coin, ch)

		err := s.exec.ws.SubscribeToCandle(coin, s.cfg.CandleInterval, func(candles []hyperliquid.Candle) {
			if len(candles) == 0 {
				return
			}
			sort.Slice(candles, func(i, j int) bool { return candles[i].T < candles[j].T })
			for _, candle := range candles {
				if !strings.EqualFold(candle.S, coin) {
					continue
				}
				if !strings.EqualFold(candle.I, s.cfg.CandleInterval) {
					continue
				}
				select {
				case ch <- candle:
				default:
					select {
					case ch <- candle:
					default:
						log.Printf("[jump] %s candle queue full; dropping candle t=%d", coin, candle.T)
					}
				}
			}
		})
		if err != nil {
			log.Printf("[jump] subscribe candle %s failed: %v", coin, err)
		}
	}
}

func (s *jumpStrategy) symbolLoop(coin string, ch <-chan hyperliquid.Candle) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case candle, ok := <-ch:
			if !ok {
				return
			}
			s.handleCandle(coin, candle)
		}
	}
}

func (s *jumpStrategy) stop() {
	s.cancel()
	s.channelMu.Lock()
	s.channels = make(map[string]chan hyperliquid.Candle)
	s.channelMu.Unlock()
}

func (s *jumpStrategy) handleCandle(coin string, candle hyperliquid.Candle) {
	s.stateMu.Lock()
	st, ok := s.state[coin]
	if !ok {
		st = &symbolState{}
		s.state[coin] = st
	}

	// Skip until we have a prior close reference.
	if !st.ready {
		st.prevClose = candle.C
		st.lastCloseTime = candle.T
		st.ready = candle.C > 0
		ready := st.ready
		prev := st.prevClose
		ts := st.lastCloseTime
		s.stateMu.Unlock()
		if ready {
			log.Printf("[jump] %s seed prevClose=%.6f closeTime=%d", coin, prev, ts)
		} else {
			log.Printf("[jump] %s seed waiting (close=%.6f)", coin, candle.C)
		}
		return
	}

	if candle.T <= st.lastCloseTime {
		s.stateMu.Unlock()
		return
	}

	prevClose := st.prevClose
	st.prevClose = candle.C
	st.lastCloseTime = candle.T
	s.stateMu.Unlock()

	if prevClose <= 0 {
		return
	}

	gapRatio := (candle.O - prevClose) / prevClose
	if math.Abs(gapRatio) < s.cfg.GapThreshold {
		return
	}

	sideBuy := gapRatio > 0
	reason := "gap-up"
	if !sideBuy {
		reason = "gap-down"
	}

	log.Printf("[jump] %s signal %s gap=%.3f%% open=%.6f prevClose=%.6f", coin, reason, gapRatio*100, candle.O, prevClose)

	if err := s.exec.placeJumpOrder(coin, sideBuy, reason, gapRatio, candle.O, prevClose); err != nil {
		log.Printf("[jump] order %s failed: %v", coin, err)
	}
}

func parseInterval(interval string) (time.Duration, error) {
	switch strings.ToLower(strings.TrimSpace(interval)) {
	case "1m":
		return time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "2h":
		return 2 * time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported candle interval %s", interval)
	}
}

func strconvParseFloat(v string) (float64, error) {
	return strconv.ParseFloat(v, 64)
}

func durationMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Milliseconds()
}

func main() {
	var (
		configPath = flag.String("config", "jump_config.json", "path to jump strategy config file")
	)
	flag.Parse()

	cfg, err := loadJumpConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	exec, err := newOrderExecutor(cfg)
	if err != nil {
		log.Fatalf("init executor: %v", err)
	}
	defer exec.close()

	log.Printf("[jump] loaded %d symbols from config", len(cfg.Symbols))

	strategy, err := newJumpStrategy(cfg, exec)
	if err != nil {
		log.Fatalf("init strategy: %v", err)
	}
	defer strategy.stop()

	strategy.start()

	log.Printf("[jump] strategy running (%d symbols, interval=%s, threshold=%.3f%%, notional=%.2f)",
		len(cfg.Symbols), cfg.CandleInterval, cfg.GapThreshold*100, cfg.NotionalUSDC)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[jump] shutting down")
}
