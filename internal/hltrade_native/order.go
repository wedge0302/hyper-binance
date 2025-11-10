package hltrade_native

import (
	"context"
	"fmt"
	"math"
	"strings"

	h "github.com/franco-bianco/go-hyperliquid/hyperliquid"
)

type (
	limitOrderType   = h.LimitOrderType
	triggerOrderType = h.TriggerOrderType
	orderTypeWire    = h.OrderTypeWire
	orderWire        = h.OrderWire
	placeOrderAction = h.PlaceOrderAction
	exchangeRequest  = h.ExchangeRequest
	orderResponse    = h.OrderResponse
	orderStatus      = h.StatusResponse
	restingStatus    = h.RestingStatus
	filledStatus     = h.FilledStatus
)

// PlaceLimitIOCAndTPSL places an IOC limit order and attaches optional TP/SL triggers.
func (c *Client) PlaceLimitIOCAndTPSL(ctx context.Context, p PlaceParams) (*PlaceResult, error) {
	if strings.TrimSpace(p.Coin) == "" {
		return nil, fmt.Errorf("coin is required")
	}
	notional := p.NotionalUSDC
	if notional <= 0 {
		notional = c.cfg.DefaultNotionalUSDC
	}
	if notional <= 0 {
		return nil, fmt.Errorf("notional must be positive")
	}

	mid, err := c.fetchMidPrice(ctx, p.Coin)
	if err != nil {
		return nil, fmt.Errorf("fetch mid price: %w", err)
	}
	if p.LimitPx <= 0 {
		p.LimitPx = mid
	}
	qty := notional / mid
	if qty <= 0 {
		return nil, fmt.Errorf("computed size is non-positive (mid %.6f, notional %.6f)", mid, notional)
	}

	meta, err := c.assetInfo(ctx, p.Coin)
	if err != nil {
		return nil, err
	}

	cloid := p.ClientOrderID
	if cloid == "" {
		cloid = newCloid("ioc")
	}

	parent := orderWire{
		Asset:      meta.assetID,
		IsBuy:      p.SideBuy,
		LimitPx:    priceToWire(p.LimitPx, perpMaxDecimals, meta.szDecimals),
		SizePx:     sizeToWire(qty, meta.szDecimals),
		ReduceOnly: p.ReduceOnly,
		OrderType: orderTypeWire{
			Limit: &limitOrderType{Tif: tifIoc},
		},
		Cloid: cloid,
	}

	action := placeOrderAction{
		Type:     "order",
		Grouping: groupingNa,
		Orders:   []orderWire{parent},
	}

	nonce := NextNonce()
	vaultAddr := c.cfg.VaultAddress
	if p.VaultAccountID != "" {
		vaultAddr = p.VaultAccountID
	}

	sig, err := c.signer.SignL1Action(action, nonce, &SignOptions{VaultAddress: vaultAddr})
	if err != nil {
		return nil, fmt.Errorf("sign L1 action: %w", err)
	}

	var vaultPtr *string
	if vaultAddr != "" {
		v := vaultAddr
		vaultPtr = &v
	}

	req := exchangeRequest{
		Action:       action,
		Nonce:        nonce,
		Signature:    *sig,
		VaultAddress: vaultPtr,
	}

	var resp orderResponse
	if err := c.postExchange(ctx, req, &resp); err != nil {
		return nil, err
	}
	status, err := parseSingleOrderStatus(resp)
	if err != nil {
		return nil, err
	}

	result := &PlaceResult{
		OrderID:          status.OrderID,
		ClientOrderID:    cloid,
		RequestedSz:      qty,
		AvgPx:            status.AvgPx,
		FilledSz:         status.FilledSz,
		RawOrderResponse: resp,
	}
	if result.AvgPx == 0 {
		result.AvgPx = p.LimitPx
	}

	tpID, slID, err := c.placeProtectiveOrders(ctx, meta, p, qty, p.LimitPx)
	if err != nil {
		return nil, err
	}
	result.TakeProfitCloid = tpID
	result.StopLossCloid = slID

	return result, nil
}

type orderExecutionResult struct {
	OrderID   int64
	AvgPx     float64
	FilledSz  float64
	ClientOID string
}

func parseSingleOrderStatus(resp orderResponse) (orderExecutionResult, error) {
	if strings.ToLower(resp.Status) != "ok" {
		return orderExecutionResult{}, fmt.Errorf("exchange response status %s", resp.Status)
	}
	if len(resp.Response.Data.Statuses) == 0 {
		return orderExecutionResult{}, fmt.Errorf("empty statuses in response")
	}
	status := resp.Response.Data.Statuses[0]
	if status.Error != "" {
		return orderExecutionResult{}, fmt.Errorf("order rejected: %s", status.Error)
	}

	var (
		orderID  int64
		avgPx    float64
		totalSz  float64
		cloid    string
		obtained bool
	)
	if status.Filled.OrderId != 0 || status.Filled.TotalSz != 0 || status.Filled.Cloid != "" {
		orderID = int64(status.Filled.OrderId)
		avgPx = status.Filled.AvgPx
		totalSz = status.Filled.TotalSz
		cloid = status.Filled.Cloid
		obtained = true
	}
	if status.Resting.OrderId != 0 || status.Resting.Cloid != "" {
		orderID = int64(status.Resting.OrderId)
		cloid = status.Resting.Cloid
		obtained = true
	}
	if !obtained {
		return orderExecutionResult{}, fmt.Errorf("unable to extract order status fields")
	}
	return orderExecutionResult{
		OrderID:   orderID,
		AvgPx:     avgPx,
		FilledSz:  totalSz,
		ClientOID: cloid,
	}, nil
}

func (c *Client) placeProtectiveOrders(ctx context.Context, meta assetMeta, p PlaceParams, qty float64, referencePx float64) (tpID, slID string, err error) {
	var orders []orderWire

	if tpPx, ok := computeTargetPx(referencePx, qty, p.SideBuy, true, p); ok {
		targetPx := tpPx
		cloid := newCloid("tp")
		orders = append(orders, triggerOrder(meta, !p.SideBuy, qty, targetPx, cloid, triggerTp, p.TakeProfitMkt))
		tpID = cloid
	}
	if slPx, ok := computeTargetPx(referencePx, qty, p.SideBuy, false, p); ok {
		targetPx := slPx
		cloid := newCloid("sl")
		orders = append(orders, triggerOrder(meta, !p.SideBuy, qty, targetPx, cloid, triggerSl, p.StopLossMkt))
		slID = cloid
	}

	if len(orders) == 0 {
		return "", "", nil
	}

	action := placeOrderAction{
		Type:     "order",
		Grouping: groupingTpSl,
		Orders:   orders,
	}

	nonce := NextNonce()
	vaultAddr := c.cfg.VaultAddress
	if p.VaultAccountID != "" {
		vaultAddr = p.VaultAccountID
	}
	sig, err := c.signer.SignL1Action(action, nonce, &SignOptions{VaultAddress: vaultAddr})
	if err != nil {
		return "", "", fmt.Errorf("sign protective orders: %w", err)
	}

	var vaultPtr *string
	if vaultAddr != "" {
		v := vaultAddr
		vaultPtr = &v
	}

	req := exchangeRequest{
		Action:       action,
		Nonce:        nonce,
		Signature:    *sig,
		VaultAddress: vaultPtr,
	}
	var resp orderResponse
	if err := c.postExchange(ctx, req, &resp); err != nil {
		return "", "", err
	}
	for i, status := range resp.Response.Data.Statuses {
		if status.Error != "" {
			return "", "", fmt.Errorf("protective order %d rejected: %s", i, status.Error)
		}
	}
	return tpID, slID, nil
}

func triggerOrder(meta assetMeta, isBuy bool, qty float64, px float64, cloid string, tpSl h.TpSl, isMarket bool) orderWire {
	priceStr := priceToWire(px, perpMaxDecimals, meta.szDecimals)
	return orderWire{
		Asset:      meta.assetID,
		IsBuy:      isBuy,
		LimitPx:    priceStr,
		SizePx:     sizeToWire(qty, meta.szDecimals),
		ReduceOnly: true,
		OrderType: orderTypeWire{
			Trigger: &triggerOrderType{
				IsMarket:  isMarket,
				TriggerPx: priceStr,
				TpSl:      tpSl,
			},
		},
		Cloid: cloid,
	}
}

func computeTargetPx(entry float64, qty float64, isLong bool, isTp bool, p PlaceParams) (float64, bool) {
	if entry <= 0 || qty <= 0 {
		return 0, false
	}

	if isTp {
		if p.TakeProfitUSDC > 0 {
			pnlPerUnit := p.TakeProfitUSDC / qty
			if isLong {
				return entry + pnlPerUnit, true
			}
			return entry - pnlPerUnit, true
		}
		if p.TakeProfitPct <= 0 {
			p.TakeProfitPct = defaultTakeProfitPct
		}
		mult := 1 + p.TakeProfitPct
		if !isLong {
			mult = 1 - p.TakeProfitPct
		}
		target := entry * mult
		if target <= 0 {
			target = math.Nextafter(0, 1)
		}
		return target, true
	}

	if p.StopLossUSDC > 0 {
		pnlPerUnit := p.StopLossUSDC / qty
		if isLong {
			return entry - pnlPerUnit, true
		}
		return entry + pnlPerUnit, true
	}
	if p.StopLossPct <= 0 {
		p.StopLossPct = defaultStopLossPct
	}
	mult := 1 - p.StopLossPct
	if !isLong {
		mult = 1 + p.StopLossPct
	}
	target := entry * mult
	if target <= 0 {
		target = math.Nextafter(0, 1)
	}
	return target, true
}

func newCloid(prefix string) string {
	return h.GetRandomCloid()
}
