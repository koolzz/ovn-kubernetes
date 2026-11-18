// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var advertisementSetters = []struct {
	name string
	set  func(*DefaultNetInfo, map[string][]string)
	get  func(*DefaultNetInfo) map[string][]string
}{
	{"pod", (*DefaultNetInfo).SetPodNetworkAdvertisedVRFs, (*DefaultNetInfo).GetPodNetworkAdvertisedVRFs},
	{"egress-ip", (*DefaultNetInfo).SetEgressIPAdvertisedVRFs, (*DefaultNetInfo).GetEgressIPAdvertisedVRFs},
}

func TestVRFAdvertisementNormalization(t *testing.T) {
	for _, setter := range advertisementSetters {
		t.Run(setter.name, func(t *testing.T) {
			n := &DefaultNetInfo{}
			for _, input := range []map[string][]string{nil, {}} {
				setter.set(n, input)
				require.NotNil(t, setter.get(n))
				require.Empty(t, setter.get(n))
			}
			setter.set(n, map[string][]string{
				"nil":        nil,
				"empty":      {},
				"single":     {"default"},
				"empty-name": {""},
				"duplicates": {"blue", "blue"},
				"unsorted":   {"red", "blue", "red"},
			})
			require.Equal(t, map[string][]string{
				"nil":        {},
				"empty":      {},
				"single":     {"default"},
				"empty-name": {""},
				"duplicates": {"blue"},
				"unsorted":   {"blue", "red"},
			}, setter.get(n))
		})
	}
}

func TestVRFAdvertisementOwnership(t *testing.T) {
	for _, setter := range advertisementSetters {
		t.Run(setter.name, func(t *testing.T) {
			input := map[string][]string{"single": {"default"}, "multiple": {"red", "blue"}}
			first, second := &DefaultNetInfo{}, &DefaultNetInfo{}
			setter.set(first, input)
			setter.set(second, input)
			input["single"][0] = "changed"
			input["multiple"][0] = "changed"
			delete(input, "multiple")
			expected := map[string][]string{"single": {"default"}, "multiple": {"blue", "red"}}
			require.Equal(t, expected, setter.get(first))
			require.Equal(t, expected, setter.get(second))

			previous := setter.get(first)
			setter.set(first, map[string][]string{"single": {"replacement"}})
			require.Equal(t, expected, previous)
			setter.get(first)["single"][0] = "changed-again"
			require.Equal(t, expected, setter.get(second))
			require.Equal(t, expected, previous)
		})
	}
}

func BenchmarkVRFAdvertisementSetters(b *testing.B) {
	for _, setter := range advertisementSetters {
		for _, nodes := range []int{100, 4000} {
			for _, vrfs := range []int{1, 2, 8} {
				input := make(map[string][]string, nodes)
				for i := 0; i < nodes; i++ {
					values := make([]string, vrfs)
					for j := range values {
						values[j] = fmt.Sprintf("vrf-%d", vrfs-j)
					}
					input[fmt.Sprintf("node-%d", i)] = values
				}
				b.Run(fmt.Sprintf("%s/nodes=%d/vrfs=%d", setter.name, nodes, vrfs), func(b *testing.B) {
					n := &DefaultNetInfo{}
					b.ReportAllocs()
					for range b.N {
						setter.set(n, input)
					}
				})
			}
		}
	}
}
