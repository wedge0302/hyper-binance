package hltrade_native

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	h "github.com/franco-bianco/go-hyperliquid/hyperliquid"
)

// ExchangeSigner wraps the official Hyperliquid signer to guarantee parity with on-chain expectations.
type ExchangeSigner struct {
	api       *h.ExchangeAPI
	isTestnet bool
	address   string
}

// NewExchangeSignerFromHex instantiates a signer using the provided private key (with or without 0x prefix).
// When accountAddress is empty it will be derived from the private key.
func NewExchangeSignerFromHex(privateKey string, accountAddress string, isTestnet bool) (*ExchangeSigner, error) {
	api := h.NewExchangeAPI(!isTestnet) // parameter is isMainnet
	key := strings.TrimPrefix(strings.TrimSpace(privateKey), "0x")
	if key == "" {
		return nil, fmt.Errorf("private key required")
	}
	if err := api.SetPrivateKey(key); err != nil {
		return nil, fmt.Errorf("set private key: %w", err)
	}
	derived := strings.ToLower(api.KeyManager().PublicAddressHex())
	addr := strings.ToLower(accountAddress)
	if addr != "" && addr != derived {
		return nil, fmt.Errorf("account address does not match private key (got %s, derived %s)", accountAddress, derived)
	}
	if addr == "" {
		addr = derived
	}
	api.SetAccountAddress(addr)
	return &ExchangeSigner{
		api:       api,
		isTestnet: isTestnet,
		address:   addr,
	}, nil
}

// Address returns the signer account address.
func (s *ExchangeSigner) Address() string {
	return s.address
}

// SignL1Action delegates to Hyperliquid's SDK to ensure signature parity.
func (s *ExchangeSigner) SignL1Action(action any, nonce uint64, opt *SignOptions) (*Signature, error) {
	if opt != nil && opt.VaultAddress != "" {
		return nil, fmt.Errorf("vault signing not supported by ExchangeSigner")
	}
	v, r, sigS, err := s.api.SignL1Action(action, nonce)
	if err != nil {
		return nil, fmt.Errorf("sign l1 action: %w", err)
	}
	typed := h.ToTypedSig(r, sigS, v)
	return &Signature{R: typed.R, S: typed.S, V: typed.V}, nil
}

// NextNonce returns a strictly increasing millisecond timestamp nonce.
var nonceCounter = time.Now().UnixMilli()

func NextNonce() uint64 {
	now := time.Now().UnixMilli()
	for {
		curr := atomic.LoadInt64(&nonceCounter)
		if now > curr {
			if atomic.CompareAndSwapInt64(&nonceCounter, curr, now) {
				return uint64(now)
			}
			continue
		}
		next := curr + 1
		if atomic.CompareAndSwapInt64(&nonceCounter, curr, next) {
			return uint64(next)
		}
	}
}
