// =============================================================================
// SHARED DISTRIBUTION POOLS
// =============================================================================
// Dùng chung cho tất cả producer trong package.
// Giữ phân phối nhất quán với login-event-producer để data pipeline đồng bộ.
// =============================================================================

package main

// platformPool: ios(30%) android(40%) web(20%) unknown(10%)
// "unknown" để test silver normalization → ELSE 'unknown'
var platformPool = []string{
	"ios", "ios", "ios",
	"android", "android", "android", "android",
	"web", "web",
	"unknown",
}

// countryPool: VN(50%) US(10%) SG(10%) TH(10%) ID(10%) empty(10%)
// empty để test silver handling → UPPER(TRIM('')) = 'unknown'
var countryPool = []string{
	"VN", "VN", "VN", "VN", "VN",
	"US",
	"SG",
	"TH",
	"ID",
	"",
}
