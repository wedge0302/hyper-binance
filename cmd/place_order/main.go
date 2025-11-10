package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"hyper-trade/internal/hltrade_native"
)

func main() {
	var (
		configPath   = flag.String("config", "", "path to JSON config file containing credentials and defaults")
		privateKey   = flag.String("private-key", "", "hex encoded private key (0x...) used for signing requests")
		accountAddr  = flag.String("address", "", "account address that owns the Hyperliquid account (0x...)")
		isTestnet    = flag.Bool("testnet", true, "use Hyperliquid testnet endpoints")
		coin         = flag.String("coin", "BTC", "perpetual coin symbol, e.g. BTC")
		sideBuy      = flag.Bool("buy", true, "true = long, false = short")
		limitPx      = flag.Float64("limit", 0, "limit price; <=0 fetches mid price")
		notional     = flag.Float64("notional", 1, "order notional size in USDC")
		tpPct        = flag.Float64("tp", 0.01, "take-profit offset as fraction, e.g. 0.01 for +1%")
		slPct        = flag.Float64("sl", 0.03, "stop-loss offset as fraction, e.g. 0.03 for -3%")
		tpMarket     = flag.Bool("tp-market", false, "use market order on take-profit trigger")
		slMarket     = flag.Bool("sl-market", false, "use market order on stop-loss trigger")
		reduceOnly   = flag.Bool("reduce-only", false, "mark the parent order as reduce-only")
		vaultAddress = flag.String("vault", "", "optional vault address if using subaccounts")
		timeout      = flag.Duration("timeout", 15*time.Second, "request timeout")
	)
	flag.Parse()

	cfgFile, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if cfgFile.PrivateKey != "" && *privateKey == "" {
		*privateKey = cfgFile.PrivateKey
	}
	if cfgFile.AccountAddress != "" && *accountAddr == "" {
		*accountAddr = cfgFile.AccountAddress
	}
	if cfgFile.Testnet != nil {
		*isTestnet = *cfgFile.Testnet
	}
	if cfgFile.DefaultNotionalUSDC > 0 && (*notional == 0 || *notional == 1) {
		*notional = cfgFile.DefaultNotionalUSDC
	}
	if cfgFile.VaultAddress != "" && *vaultAddress == "" {
		*vaultAddress = cfgFile.VaultAddress
	}
	if cfgFile.Coin != "" && *coin == "BTC" {
		*coin = cfgFile.Coin
	}
	if cfgFile.SideBuy != nil {
		*sideBuy = *cfgFile.SideBuy
	}
	if cfgFile.TP != nil {
		*tpPct = *cfgFile.TP
	}
	if cfgFile.SL != nil {
		*slPct = *cfgFile.SL
	}
	if cfgFile.TPMarket != nil {
		*tpMarket = *cfgFile.TPMarket
	}
	if cfgFile.SLMarket != nil {
		*slMarket = *cfgFile.SLMarket
	}
	if cfgFile.ReduceOnly != nil {
		*reduceOnly = *cfgFile.ReduceOnly
	}

	if *privateKey == "" || *accountAddr == "" {
		flag.Usage()
		fmt.Println("\n--private-key and --address must be provided (directly or via config)")
		os.Exit(2)
	}

	signer, err := hltrade_native.NewExchangeSignerFromHex(*privateKey, *accountAddr, *isTestnet)
	if err != nil {
		log.Fatalf("init signer: %v", err)
	}

	cfg := hltrade_native.Config{
		AccountAddress:      *accountAddr,
		IsTestnet:           *isTestnet,
		DefaultNotionalUSDC: *notional,
		VaultAddress:        *vaultAddress,
	}

	client := hltrade_native.NewClient(cfg, signer)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	params := hltrade_native.PlaceParams{
		Coin:           *coin,
		SideBuy:        *sideBuy,
		LimitPx:        *limitPx,
		NotionalUSDC:   *notional,
		ReduceOnly:     *reduceOnly,
		TakeProfitPct:  *tpPct,
		StopLossPct:    *slPct,
		TakeProfitMkt:  *tpMarket,
		StopLossMkt:    *slMarket,
		VaultAccountID: *vaultAddress,
	}

	result, err := client.PlaceLimitIOCAndTPSL(ctx, params)
	if err != nil {
		log.Fatalf("place order failed: %v", err)
	}

	fmt.Println("Order placed successfully!")
	fmt.Printf("  Coin: %s\n", params.Coin)
	fmt.Printf("  Side: %s\n", sideLabel(params.SideBuy))
	fmt.Printf("  Requested size: %.6f\n", result.RequestedSz)
	fmt.Printf("  Filled size: %.6f\n", result.FilledSz)
	fmt.Printf("  Average price: %.6f\n", result.AvgPx)
	fmt.Printf("  Order ID: %d\n", result.OrderID)
	if result.TakeProfitCloid != "" {
		fmt.Printf("  Take-profit cloID: %s\n", result.TakeProfitCloid)
	}
	if result.StopLossCloid != "" {
		fmt.Printf("  Stop-loss cloID: %s\n", result.StopLossCloid)
	}
}

func sideLabel(isBuy bool) string {
	if isBuy {
		return "LONG"
	}
	return "SHORT"
}

type configFile struct {
	PrivateKey          string   `json:"privateKey"`
	AccountAddress      string   `json:"address"`
	Testnet             *bool    `json:"testnet"`
	DefaultNotionalUSDC float64  `json:"defaultNotionalUSDC"`
	VaultAddress        string   `json:"vaultAddress"`
	Coin                string   `json:"coin"`
	SideBuy             *bool    `json:"buy"`
	TP                  *float64 `json:"tp"`
	SL                  *float64 `json:"sl"`
	TPMarket            *bool    `json:"tpMarket"`
	SLMarket            *bool    `json:"slMarket"`
	ReduceOnly          *bool    `json:"reduceOnly"`
}

func loadConfig(path string) (configFile, error) {
	if strings.TrimSpace(path) == "" {
		return configFile{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return configFile{}, err
	}
	var cfg configFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return configFile{}, err
	}
	return cfg, nil
}
