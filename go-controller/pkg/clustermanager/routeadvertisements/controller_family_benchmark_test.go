// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeadvertisements

import (
	"context"
	"fmt"
	"testing"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	ovntest "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

// BenchmarkGenerateFRRFamilyFiltering measures whole configuration generation
// with cached inputs and dual-stack prefixes. API writes and setup are excluded.
func BenchmarkGenerateFRRFamilyFiltering(b *testing.B) {
	for _, nodeCount := range []int{100, 4000} {
		for _, neighborCount := range []int{1, 16} {
			b.Run(fmt.Sprintf("nodes=%d/neighbors=%d", nodeCount, neighborCount), func(b *testing.B) {
				oldDefault, oldFeatures, oldGateway := config.Default, config.OVNKubernetesFeature, config.Gateway
				oldV4, oldV6 := config.IPv4Mode, config.IPv6Mode
				b.Cleanup(func() {
					config.Default, config.OVNKubernetesFeature, config.Gateway = oldDefault, oldFeatures, oldGateway
					config.IPv4Mode, config.IPv6Mode = oldV4, oldV6
				})
				config.IPv4Mode, config.IPv6Mode = true, true
				config.Default.ClusterSubnets = []config.CIDRNetworkEntry{
					{CIDR: ovntest.MustParseIPNet("10.0.0.0/8"), HostSubnetLength: 24},
					{CIDR: ovntest.MustParseIPNet("fd00::/16"), HostSubnetLength: 64},
				}
				config.Default.Transport = ""
				config.OVNKubernetesFeature.EnableMultiNetwork = true
				config.OVNKubernetesFeature.EnableRouteAdvertisements = true
				config.OVNKubernetesFeature.EnableDynamicUDNAllocation = false
				config.Gateway.Mode = config.GatewayModeLocal
				clients := util.GetOVNClientset().GetClusterManagerClientset()
				wf, err := factory.NewClusterManagerWatchFactory(clients)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(wf.Shutdown)
				c := NewController(&networkmanager.FakeNetworkManager{}, wf, clients)
				b.Cleanup(c.Stop)
				nad := (&testNAD{Name: config.Default.ClusterDefaultNetworkNAD.Name, Namespace: config.Default.ClusterDefaultNetworkNAD.Namespace, Network: types.DefaultNetworkName}).NAD()
				nad.Annotations = map[string]string{types.OvnNetworkNameAnnotation: types.DefaultNetworkName}
				if err := wf.NADInformer().Informer().GetIndexer().Add(nad); err != nil {
					b.Fatal(err)
				}
				for i := 0; i < nodeCount; i++ {
					node := (&testNode{Name: fmt.Sprintf("node-%04d", i),
						PrimaryAddressAnnotation: fmt.Sprintf(`{"ipv4":"172.16.%d.%d/24"}`, i/250, i%250+1),
						SubnetsAnnotation:        fmt.Sprintf(`{"default":["10.%d.%d.0/24","fd00:%x::/64"]}`, i/256, i%256, i),
					}).Node()
					if err := wf.NodeCoreInformer().Informer().GetIndexer().Add(node); err != nil {
						b.Fatal(err)
					}
				}
				neighbors := make([]*testNeighbor, 0, neighborCount)
				for i := 0; i < neighborCount; i++ {
					address := fmt.Sprintf("192.0.2.%d", i+1)
					if i%2 != 0 {
						address = fmt.Sprintf("2001:db8:ffff::%x", i+1)
					}
					neighbors = append(neighbors, &testNeighbor{ASN: 64513, Address: address})
				}
				source := (&testFRRConfig{Name: "source", Namespace: "frr", Routers: []*testRouter{{ASN: 64512, Neighbors: neighbors}}}).FRRConfiguration()
				if err := wf.FRRConfigurationsInformer().Informer().GetIndexer().Add(source); err != nil {
					b.Fatal(err)
				}
				ra := (&testRA{Name: "benchmark", AdvertisePods: true, SelectsDefault: true}).RouteAdvertisements()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					generated, _, err := c.generateFRRConfigurations(context.Background(), ra)
					if err != nil {
						b.Fatal(err)
					}
					if len(generated) != nodeCount {
						b.Fatalf("generated %d configurations, want %d", len(generated), nodeCount)
					}
					if got := len(generated[0].Spec.BGP.Routers[0].Neighbors); got != neighborCount {
						b.Fatalf("generated %d neighbors, want %d", got, neighborCount)
					}
				}
			})
		}
	}
}
