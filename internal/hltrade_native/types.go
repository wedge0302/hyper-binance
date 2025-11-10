package hltrade_native

import (
	"time"

	h "github.com/franco-bianco/go-hyperliquid/hyperliquid"
)

// Config holds the client level configuration required to talk to Hyperliquid.
type Config struct {
	// AccountAddress is the public address controlling the account (lower-case 0x… recommended).
	AccountAddress string
	// IsTestnet toggles between mainnet and the Hyperliquid testnet endpoints.
	IsTestnet bool
	// DefaultNotionalUSDC is used when callers omit the notional amount while placing orders.
	DefaultNotionalUSDC float64
	// Dex allows selecting a specific perpetual DEX instance. Leave empty for the default.
	Dex string
	// VaultAddress optionally specifies the sub-account/vault address for requests that require it.
	VaultAddress string
}

// Signature represents an EIP-712 signature encoded in the format expected by Hyperliquid.
type Signature = h.RsvSignature

// Signer turns Hyperliquid actions into EIP-712 signatures.
type Signer interface {
	SignL1Action(action any, nonce uint64, opt *SignOptions) (*Signature, error)
}

// SignOptions allows callers to supply optional signing modifiers.
type SignOptions struct {
	VaultAddress string
	ExpiresAfter int64
}

// PlaceParams configures a limit IOC order with optional protective orders.
type PlaceParams struct {
	Coin           string  // e.g. "BTC"
	SideBuy        bool    // true: open long, false: open short
	LimitPx        float64 // limit price for the IOC parent order
	NotionalUSDC   float64 // notional amount in USDC; <=0 falls back to DefaultNotionalUSDC
	ReduceOnly     bool    // mark orders as reduce-only
	ClientOrderID  string  // optional custom client order id; auto generated when empty
	TakeProfitPct  float64 // relative take-profit offset (e.g. 0.01 for +1%); <=0 to skip
	StopLossPct    float64 // relative stop-loss offset (e.g. 0.03 for -3%); <=0 to skip
	TakeProfitMkt  bool    // use market order when take-profit triggers
	StopLossMkt    bool    // use market order when stop-loss triggers
	VaultAccountID string  // optional vault address override for this order
	TakeProfitUSDC float64 // absolute take-profit PnL target
	StopLossUSDC   float64 // absolute stop-loss PnL target
}

// PlaceResult captures the outcome of PlaceLimitIOCAndTPSL.
type PlaceResult struct {
	OrderID          int64
	ClientOrderID    string
	RequestedSz      float64
	FilledSz         float64
	AvgPx            float64
	TakeProfitCloid  string
	StopLossCloid    string
	RawOrderResponse any
}

// PositionSnapshot contains the relevant details for a single perpetual position.
type PositionSnapshot struct {
	Coin           string
	Size           float64
	EntryPx        float64
	LiquidationPx  float64
	Leverage       int
	Side           string
	UnrealizedPnl  float64
	ReturnOnEquity float64
	Raw            any
}

// AccountSnapshot summarises account level metrics along with open positions.
type AccountSnapshot struct {
	Time                       time.Time
	AccountValue               float64
	TotalMarginUsed            float64
	TotalNotionalPositions     float64
	Withdrawable               float64
	CrossMaintenanceMarginUsed float64
	Positions                  []PositionSnapshot
	Raw                        any
}
