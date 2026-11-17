// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// Both revisions use the public mapping API; setup and resolver keys are kept
// outside timing so the benchmark measures mapping rather than test formatting.
func BenchmarkQualifiedSelectionMapping(b *testing.B) {
	for _, mode := range []string{"warm", "cold", "oversized"} {
		for _, fanout := range []int{1, 8, 450} {
			b.Run(fmt.Sprintf("%s/controllers=%d", mode, fanout), func(b *testing.B) {
				var selections []string
				membership := make(map[string]string)
				for i := range 8 {
					name := fmt.Sprintf("net%d", i)
					selections = append(selections, fmt.Sprintf(`{"name":%q,"interface":%q}`, name, name))
					membership["tenant/"+name] = name
				}
				annotation := "[" + strings.Join(selections, ",") + "]"
				if mode == "oversized" {
					annotation += strings.Repeat(" ", networkSelectionCacheBytes/networkSelectionCacheEntries)
				}
				p := selectionPod("tenant", annotation)
				resolve := func(key string) string { return membership[key] }
				networks := make([]*userDefinedNetInfo, fanout)
				for i := range networks {
					networks[i] = &userDefinedNetInfo{netName: fmt.Sprintf("net%d", i)}
				}
				if _, _, err := GetUDNPodNADToNetworkMapping(p, networks[0], resolve); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				for b.Loop() {
					if mode == "cold" {
						parsedNetworkSelections = newNetworkSelectionCache(networkSelectionCacheEntries, networkSelectionCacheBytes)
					}
					for _, n := range networks {
						if _, _, err := GetUDNPodNADToNetworkMapping(p, n, resolve); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// A diagnostic, not a platform-dependent heap-size assertion. Run alone on each
// revision to compare the fully populated cache's retained-memory tradeoff.
func TestNetworkSelectionCacheFootprint(t *testing.T) {
	var selections []string
	for i := range 8 {
		selections = append(selections, fmt.Sprintf(`{"name":"network-%d","interface":"net%d"}`, i, i))
	}
	annotation := "[" + strings.Join(selections, ",") + "]"
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c := newNetworkSelectionCache(networkSelectionCacheEntries, networkSelectionCacheBytes)
	for i := range networkSelectionCacheEntries {
		if _, err := c.get(fmt.Sprintf("tenant-with-a-representative-namespace-%d", i), annotation); err != nil {
			t.Fatal(err)
		}
	}
	if c.cache.Len() != networkSelectionCacheEntries {
		t.Fatalf("expected full cache, got %d entries", c.cache.Len())
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(c)
	t.Logf("full-cache heap delta: %d bytes for %d entries", int64(after.HeapAlloc)-int64(before.HeapAlloc), networkSelectionCacheEntries)
}
