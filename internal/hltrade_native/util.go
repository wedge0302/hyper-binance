package hltrade_native

import (
	"math"
	"strconv"
	"strings"
)

func priceToWire(x float64, maxDecimals, szDecimals int) string {
	if x == 0 {
		return "0"
	}
	if x == math.Trunc(x) {
		return strconv.FormatInt(int64(x), 10)
	}
	allowedTick := maxDecimals - szDecimals
	if allowedTick < 0 {
		allowedTick = 0
	}
	var allowedSig int
	if x >= 1 {
		digits := int(math.Floor(math.Log10(x))) + 1
		allowedSig = maxInt(5-digits, 0)
	} else {
		exponent := int(math.Ceil(-math.Log10(x)))
		allowedSig = 4 + exponent
	}
	allowedDecimals := maxInt(minInt(allowedTick, allowedSig), 0)
	factor := math.Pow10(allowedDecimals)
	rounded := math.Round(x*factor) / factor
	s := strconv.FormatFloat(rounded, 'f', allowedDecimals, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

func sizeToWire(x float64, szDecimals int) string {
	if szDecimals <= 0 {
		return strconv.FormatInt(int64(math.Trunc(x)), 10)
	}
	if x == math.Trunc(x) {
		return strconv.FormatInt(int64(x), 10)
	}
	factor := math.Pow10(szDecimals)
	rounded := math.Round(x*factor) / factor
	s := strconv.FormatFloat(rounded, 'f', szDecimals, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
