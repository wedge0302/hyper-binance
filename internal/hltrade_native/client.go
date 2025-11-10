package hltrade_native

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type assetMeta struct {
	assetID    int
	szDecimals int
}

// Client manages HTTP communication with Hyperliquid's info and exchange endpoints.
type Client struct {
	cfg     Config
	signer  Signer
	httpc   *http.Client
	baseURL string
	infoURL string

	metaOnce   sync.Once
	metaMu     sync.RWMutex
	meta       map[string]assetMeta
	metaErr    error
	metaExpiry time.Time
}

// NewClient constructs a Client with sane defaults.
func NewClient(cfg Config, signer Signer) *Client {
	base := mainnetAPIBase
	if cfg.IsTestnet {
		base = testnetAPIBase
	}
	return &Client{
		cfg:     cfg,
		signer:  signer,
		httpc:   &http.Client{Timeout: 8 * time.Second},
		baseURL: strings.TrimRight(base, "/"),
		infoURL: strings.TrimRight(base, "/") + infoEndpoint,
	}
}

// doPost marshals payload, executes the POST request and unmarshals into out if provided.
func (c *Client) doPost(ctx context.Context, url string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hyper-trade-native/1.0")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		d, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(d)))
	}

	if out == nil {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response failed: %w", err)
	}
	return nil
}

func (c *Client) postInfo(ctx context.Context, payload any, out any) error {
	return c.doPost(ctx, c.infoURL, payload, out)
}

func (c *Client) postExchange(ctx context.Context, payload any, out any) error {
	return c.doPost(ctx, c.baseURL+exchangeEndpoint, payload, out)
}

// ensureMeta fetches asset metadata and caches it for subsequent requests.
func (c *Client) ensureMeta(ctx context.Context) error {
	c.metaMu.RLock()
	meta := c.meta
	expiry := c.metaExpiry
	err := c.metaErr
	c.metaMu.RUnlock()

	if meta != nil && time.Now().Before(expiry) && err == nil {
		return nil
	}

	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if c.meta != nil && time.Now().Before(c.metaExpiry) && c.metaErr == nil {
		return nil
	}

	m, err := c.fetchMeta(ctx)
	if err != nil {
		c.metaErr = err
		return err
	}
	c.meta = m
	c.metaErr = nil
	c.metaExpiry = time.Now().Add(5 * time.Minute)
	return nil
}

func (c *Client) assetInfo(ctx context.Context, coin string) (assetMeta, error) {
	if err := c.ensureMeta(ctx); err != nil {
		return assetMeta{}, err
	}
	c.metaMu.RLock()
	defer c.metaMu.RUnlock()
	info, ok := c.meta[coin]
	if !ok {
		return assetMeta{}, fmt.Errorf("unknown coin %q", coin)
	}
	return info, nil
}
