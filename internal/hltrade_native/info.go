package hltrade_native

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

type infoRequest struct {
	Type string `json:"type"`
	User string `json:"user,omitempty"`
	Coin string `json:"coin,omitempty"`
	Dex  string `json:"dex,omitempty"`
}

type metaUniverseItem struct {
	Name       string `json:"name"`
	SzDecimals int    `json:"szDecimals"`
	IsDelisted bool   `json:"isDelisted,omitempty"`
}

type metaResponse struct {
	Universe []metaUniverseItem `json:"universe"`
}

func (c *Client) fetchMeta(ctx context.Context) (map[string]assetMeta, error) {
	req := infoRequest{Type: "meta", Dex: c.cfg.Dex}
	var resp metaResponse
	if err := c.postInfo(ctx, req, &resp); err != nil {
		return nil, err
	}
	result := make(map[string]assetMeta, len(resp.Universe))
	for idx, item := range resp.Universe {
		if item.Name == "" || item.IsDelisted {
			continue
		}
		result[item.Name] = assetMeta{
			assetID:    idx,
			szDecimals: item.SzDecimals,
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("meta response contained no active assets")
	}
	return result, nil
}

type midsResponse map[string]string

func (c *Client) fetchAllMids(ctx context.Context) (map[string]float64, error) {
	req := infoRequest{Type: "allMids", Dex: c.cfg.Dex}
	var resp midsResponse
	if err := c.postInfo(ctx, req, &resp); err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(resp))
	for coin, px := range resp {
		if px == "" {
			continue
		}
		val, err := strconv.ParseFloat(px, 64)
		if err != nil {
			return nil, fmt.Errorf("parse mid for %s: %w", coin, err)
		}
		out[coin] = val
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("allMids response empty")
	}
	return out, nil
}

type leveragePayload struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

type positionPayload struct {
	Coin           string          `json:"coin"`
	EntryPx        float64         `json:"entryPx,string"`
	LiquidationPx  float64         `json:"liquidationPx,string"`
	MarginUsed     float64         `json:"marginUsed,string"`
	PositionValue  float64         `json:"positionValue,string"`
	Szi            float64         `json:"szi,string"`
	UnrealizedPnl  float64         `json:"unrealizedPnl,string"`
	ReturnOnEquity float64         `json:"returnOnEquity,string"`
	Leverage       leveragePayload `json:"leverage"`
}

type assetPosition struct {
	Type     string          `json:"type"`
	Position positionPayload `json:"position"`
}

type marginSummary struct {
	AccountValue    float64 `json:"accountValue,string"`
	TotalMarginUsed float64 `json:"totalMarginUsed,string"`
	TotalNtlPos     float64 `json:"totalNtlPos,string"`
}

type userStateResponse struct {
	Time                       int64           `json:"time"`
	Withdrawable               float64         `json:"withdrawable,string"`
	CrossMaintenanceMarginUsed float64         `json:"crossMaintenanceMarginUsed,string"`
	AssetPositions             []assetPosition `json:"assetPositions"`
	MarginSummary              marginSummary   `json:"marginSummary"`
}

func (c *Client) fetchUserState(ctx context.Context, account string) (*userStateResponse, error) {
	if account == "" {
		return nil, fmt.Errorf("account address required")
	}
	req := infoRequest{Type: "userState", User: account, Dex: c.cfg.Dex}
	var resp userStateResponse
	if err := c.postInfo(ctx, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) buildAccountSnapshot(resp *userStateResponse) AccountSnapshot {
	snapshot := AccountSnapshot{
		Time:                       time.UnixMilli(resp.Time),
		AccountValue:               resp.MarginSummary.AccountValue,
		TotalMarginUsed:            resp.MarginSummary.TotalMarginUsed,
		TotalNotionalPositions:     resp.MarginSummary.TotalNtlPos,
		Withdrawable:               resp.Withdrawable,
		CrossMaintenanceMarginUsed: resp.CrossMaintenanceMarginUsed,
		Raw:                        resp,
	}
	for _, ap := range resp.AssetPositions {
		pos := ap.Position
		side := "flat"
		if pos.Szi > 0 {
			side = "long"
		} else if pos.Szi < 0 {
			side = "short"
		}
		snapshot.Positions = append(snapshot.Positions, PositionSnapshot{
			Coin:           pos.Coin,
			Size:           pos.Szi,
			EntryPx:        pos.EntryPx,
			LiquidationPx:  pos.LiquidationPx,
			Leverage:       pos.Leverage.Value,
			Side:           side,
			UnrealizedPnl:  pos.UnrealizedPnl,
			ReturnOnEquity: pos.ReturnOnEquity,
			Raw:            ap,
		})
	}
	return snapshot
}

// AccountSnapshot queries /info userState and converts it into a convenient structure.
func (c *Client) AccountSnapshot(ctx context.Context) (*AccountSnapshot, error) {
	resp, err := c.fetchUserState(ctx, c.cfg.AccountAddress)
	if err != nil {
		return nil, err
	}
	snapshot := c.buildAccountSnapshot(resp)
	return &snapshot, nil
}

// Positions returns the list of perpetual positions for the configured account.
func (c *Client) Positions(ctx context.Context) ([]PositionSnapshot, error) {
	resp, err := c.fetchUserState(ctx, c.cfg.AccountAddress)
	if err != nil {
		return nil, err
	}
	snapshot := c.buildAccountSnapshot(resp)
	return snapshot.Positions, nil
}

func (c *Client) fetchMidPrice(ctx context.Context, coin string) (float64, error) {
	mids, err := c.fetchAllMids(ctx)
	if err != nil {
		return 0, err
	}
	px, ok := mids[coin]
	if !ok {
		return 0, fmt.Errorf("mid price for %s not found", coin)
	}
	return px, nil
}
