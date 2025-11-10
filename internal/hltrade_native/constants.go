package hltrade_native

import h "github.com/franco-bianco/go-hyperliquid/hyperliquid"

const (
	mainnetAPIBase   = "https://api.hyperliquid.xyz"
	testnetAPIBase   = "https://api.hyperliquid-testnet.xyz"
	infoEndpoint     = "/info"
	exchangeEndpoint = "/exchange"

	hyperliquidChainID       = 1337
	arbitrumChainID          = 42161
	arbitrumTestnetChainID   = 421614
	verifyingContractAddress = "0x0000000000000000000000000000000000000000"

	groupingNa   = "na"
	groupingTpSl = "positionTpsl"

	tifIoc = "Ioc"
	tifGtc = "Gtc"
	tifAlo = "Alo"

	triggerTp = h.TriggerTp
	triggerSl = h.TriggerSl

	defaultTakeProfitPct = 0.01
	defaultStopLossPct   = 0.03

	perpMaxDecimals = 6
	spotMaxDecimals = 8
)
