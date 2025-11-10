package hlapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	infoURL        = "https://api.hyperliquid.xyz/info"
	reqTimeout     = 8 * time.Second
	maxRetries     = 4
	initialBackoff = 300 * time.Millisecond
	backoffFactor  = 1.7
)

type metaReq struct {
	Type string `json:"type"`          // must be "meta"
	Dex  string `json:"dex,omitempty"` // optional: e.g. "test"
}

type universeItem struct {
	Name       string `json:"name"`
	IsDelisted bool   `json:"isDelisted,omitempty"`
}

type metaResp struct {
	Universe []universeItem `json:"universe"`
}

// FetchPerpSymbols 从 Hyperliquid Info API 获取永续合约标的列表（例如 "BTC", "ETH"...）
// dex 可为空（默认主网）；如 "test" 为测试网。
func FetchPerpSymbols(ctx context.Context, dex string) ([]string, error) {
	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		reqBody, _ := json.Marshal(metaReq{Type: "meta", Dex: dex})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, infoURL, bytes.NewReader(reqBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "hlapi-client/1.0")

		client := &http.Client{Timeout: reqTimeout}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				b, _ := io.ReadAll(resp.Body)
				lastErr = fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
			} else {
				var m metaResp
				if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
					lastErr = err
				} else {
					var syms []string
					for _, it := range m.Universe {
						if !it.IsDelisted && it.Name != "" {
							syms = append(syms, it.Name)
						}
					}
					sort.Strings(syms)
					return syms, nil
				}
			}
		}
		if attempt < maxRetries {
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * backoffFactor)
			continue
		}
	}

	if lastErr == nil {
		lastErr = errors.New("unknown error")
	}
	return nil, lastErr
}

// FetchPerpSymbolsWithUSDT 获取永续合约标的并在每个后面加上 "USDT"
func FetchPerpSymbolsWithUSDT(ctx context.Context, dex string) ([]string, error) {
	syms, err := FetchPerpSymbols(ctx, dex)
	if err != nil {
		return nil, err
	}
	withUSDT := make([]string, len(syms))
	for i, s := range syms {
		withUSDT[i] = s + "USDT"
	}
	return withUSDT, nil
}
