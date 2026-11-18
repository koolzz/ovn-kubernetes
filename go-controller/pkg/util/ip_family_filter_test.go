// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	utilnet "k8s.io/utils/net"
)

// Retain the previous implementation as the compatibility and benchmark baseline.
func legacyMatchAllIPNetsStringFamily(ipv6 bool, prefixes []string) []string {
	var out []string
	for _, prefix := range prefixes {
		if utilnet.IsIPv6CIDRString(prefix) == ipv6 {
			out = append(out, prefix)
		}
	}
	return out
}

func TestMatchAllIPNetsStringFamilyCompatibility(t *testing.T) {
	input := []string{"10.0.0.1/24", "2001:db8::1/64", "10.0.0.1/24", "::ffff:192.0.2.1/120", "010.0.0.1/24", "invalid"}
	require.Equal(t, []string{input[0], input[2], input[3], input[4], input[5]}, MatchAllIPNetsStringFamily(false, input))
	require.Equal(t, []string{input[1]}, MatchAllIPNetsStringFamily(true, input))
	for _, ipv6 := range []bool{false, true} {
		require.Nil(t, MatchAllIPNetsStringFamily(ipv6, nil))
		require.Nil(t, MatchAllIPNetsStringFamily(ipv6, []string{}))
	}
	// Host bits, order, duplicate values and unusual input must not be normalized.
	addresses := []string{"0.0.0.0", "10.0.0.1", "192.168.001.001", "::", "2001:db8::42", "::ffff:192.0.2.1", "::ffff:c000:201", "::192.0.2.1", "64:ff9b::192.168.001.001", "fe80::1%eth0", "invalid", ""}
	masks := []string{"0", "1", "24", "32", "64", "96", "120", "128", "129", "-1", "+64", "064", "", "mask"}
	for _, address := range addresses {
		for _, mask := range masks {
			prefix := address + "/" + mask
			for _, ipv6 := range []bool{false, true} {
				require.Equal(t, legacyMatchAllIPNetsStringFamily(ipv6, []string{prefix}), MatchAllIPNetsStringFamily(ipv6, []string{prefix}), "prefix=%q ipv6=%t", prefix, ipv6)
			}
		}
	}
}

func TestMatchAllIPNetsStringFamilyOwnsOutput(t *testing.T) {
	input := []string{"10.0.0.1/24", "2001:db8::1/64"}
	first := MatchAllIPNetsStringFamily(false, input)
	second := MatchAllIPNetsStringFamily(false, input)
	first[0] = "changed"
	require.Equal(t, "10.0.0.1/24", input[0])
	require.Equal(t, "10.0.0.1/24", second[0])
}

func FuzzMatchAllIPNetsStringFamily(f *testing.F) {
	for _, prefix := range []string{"10.0.0.1/24", "2001:db8::1/64", "::/0", "::ffff:192.0.2.1/120", "::ffff:c000:201/32", "010.0.0.1/24", "64:ff9b::192.168.001.001/128", "fe80::1%eth0/64", "2001:db8::/129", "invalid", ""} {
		f.Add(prefix, false)
		f.Add(prefix, true)
	}
	f.Fuzz(func(t *testing.T, prefix string, ipv6 bool) {
		require.Equal(t, legacyMatchAllIPNetsStringFamily(ipv6, []string{prefix}), MatchAllIPNetsStringFamily(ipv6, []string{prefix}))
	})
}

var familyFilterBenchmarkResult []string

func BenchmarkMatchAllIPNetsStringFamily(b *testing.B) {
	for _, count := range []int{2, 64} {
		prefixes := make([]string, 0, count)
		for i := 0; i < count/2; i++ {
			prefixes = append(prefixes, fmt.Sprintf("10.0.%d.0/24", i), fmt.Sprintf("2001:db8:%x::/64", i))
		}
		for _, legacy := range []bool{true, false} {
			b.Run(fmt.Sprintf("prefixes=%d/legacy=%t", count, legacy), func(b *testing.B) {
				filter := MatchAllIPNetsStringFamily
				if legacy {
					filter = legacyMatchAllIPNetsStringFamily
				}
				b.ReportAllocs()
				for range b.N {
					familyFilterBenchmarkResult = filter(false, prefixes)
				}
			})
		}
	}
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprintf("legacy-notation/legacy=%t", legacy), func(b *testing.B) {
			prefixes := []string{"010.0.0.1/24", "64:ff9b::192.168.001.001/128"}
			filter := MatchAllIPNetsStringFamily
			if legacy {
				filter = legacyMatchAllIPNetsStringFamily
			}
			b.ReportAllocs()
			for range b.N {
				familyFilterBenchmarkResult = filter(false, prefixes)
			}
		})
	}
}
