// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"errors"
	"testing"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/networkmanager"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func TestBaseUserDefinedNetworkController_ClaimsNamespace(t *testing.T) {
	util.PrepareTestConfig()
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true
	config.IPv4Mode = true

	const myNetwork = "mine"
	const otherNetwork = "other"
	const myNAD = "ns-mine/nad"
	const otherNAD = "ns-other/nad"

	mineNetConf := &ovncnitypes.NetConf{
		NetConf:    cnitypes.NetConf{Name: myNetwork},
		Topology:   types.Layer3Topology,
		Role:       types.NetworkRolePrimary,
		NADName:    myNAD,
		Subnets:    "10.128.0.0/14",
		MTU:        1400,
	}
	otherNetConf := &ovncnitypes.NetConf{
		NetConf:    cnitypes.NetConf{Name: otherNetwork},
		Topology:   types.Layer3Topology,
		Role:       types.NetworkRolePrimary,
		NADName:    otherNAD,
		Subnets:    "10.132.0.0/14",
		MTU:        1400,
	}

	mineNetInfo, err := util.NewNetInfo(mineNetConf)
	require.NoError(t, err)
	otherNetInfo, err := util.NewNetInfo(otherNetConf)
	require.NoError(t, err)

	fnm := &networkmanager.FakeNetworkManager{
		PrimaryNetworks: map[string]util.NetInfo{
			// Happy paths: each namespace maps to a concrete primary
			// network. The fake resolves the namespace's primary NAD by
			// looking up NADNetworks for entries whose nadKey namespace
			// matches the namespace name.
			"ns-mine":  mineNetInfo,
			"ns-other": otherNetInfo,
			// Transient parse failure: nil value is the fake's signal
			// to return util.NewInvalidPrimaryNetworkError.
			"ns-pending": nil,
		},
		NADNetworks: map[string]util.NetInfo{
			myNAD:    mineNetInfo,
			otherNAD: otherNetInfo,
		},
	}

	oc := &BaseUserDefinedNetworkController{
		BaseNetworkController: BaseNetworkController{
			ReconcilableNetInfo: util.NewReconcilableNetInfo(mineNetInfo),
			networkManager:      fnm,
		},
	}

	t.Run("matching primary NAD → claims", func(t *testing.T) {
		claims, err := oc.ClaimsNamespace("ns-mine")
		require.NoError(t, err)
		assert.True(t, claims, "namespace whose primary NAD is this network must be claimed")
	})

	t.Run("different primary NAD → does not claim", func(t *testing.T) {
		claims, err := oc.ClaimsNamespace("ns-other")
		require.NoError(t, err)
		assert.False(t, claims, "namespace whose primary NAD belongs to a different network must not be claimed")
	})

	t.Run("default-network namespace → does not claim", func(t *testing.T) {
		// Namespace not present in PrimaryNetworks → FakeNetworkManager
		// returns types.DefaultNetworkName, which ClaimsNamespace must
		// treat as "not mine".
		claims, err := oc.ClaimsNamespace("ns-default-only")
		require.NoError(t, err)
		assert.False(t, claims, "default-network-only namespace must not be claimed by a UDN handler")
	})

	t.Run("transient lookup error → bubbles", func(t *testing.T) {
		// This is the load-bearing case for the new (bool, error)
		// signature: shouldFilterNamespace would have swallowed this
		// as "don't filter" (i.e. would have claimed the namespace),
		// causing the shared controller to force-add or keep cached
		// state on stale data.
		claims, err := oc.ClaimsNamespace("ns-pending")
		require.Error(t, err)
		assert.False(t, claims, "predicate must return false on error so callers cannot mistake stale data for a positive claim")
		var invalid *util.InvalidPrimaryNetworkError
		assert.True(t, errors.As(err, &invalid), "expected InvalidPrimaryNetworkError, got %T: %v", err, err)
	})
}

func TestBaseNetworkController_shouldWatchNamespaces(t *testing.T) {
	tests := []struct {
		name                                                 string
		netCfg                                               *ovncnitypes.NetConf
		enableNetSeg, enableMultiNetPolicies, expectedReturn bool
	}{
		{
			name: "should watch namespaces for default network",
			netCfg: &ovncnitypes.NetConf{
				NetConf: cnitypes.NetConf{Name: types.DefaultNetworkName},
			},
			expectedReturn: true,
		},
		{
			name: "should watch namespaces for primary network when network segmentation is enabled",
			netCfg: &ovncnitypes.NetConf{
				NetConf:  cnitypes.NetConf{Name: "primary"},
				Topology: types.Layer3Topology,
				Role:     types.NetworkRolePrimary,
			},
			enableNetSeg:   true,
			expectedReturn: true,
		},
		{
			name: "should watch namespaces for secondary network when multi NetworkPolicies are enabled",
			netCfg: &ovncnitypes.NetConf{
				NetConf:  cnitypes.NetConf{Name: "secondary"},
				Topology: types.Layer3Topology,
				Role:     types.NetworkRoleSecondary,
			},
			enableMultiNetPolicies: true,
			expectedReturn:         true,
		},
		{
			name: "should not watch namespaces for primary network when network segmentation is disabled",
			netCfg: &ovncnitypes.NetConf{
				NetConf:  cnitypes.NetConf{Name: "primary"},
				Topology: types.Layer3Topology,
				Role:     types.NetworkRolePrimary,
			},
			expectedReturn: false,
		},
		{
			name: "should not watch namespaces for secondary network when multi NetworkPolicies is disabled",
			netCfg: &ovncnitypes.NetConf{
				NetConf:  cnitypes.NetConf{Name: "secondary"},
				Topology: types.Layer3Topology,
				Role:     types.NetworkRoleSecondary,
			},
			expectedReturn: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			util.PrepareTestConfig()
			config.OVNKubernetesFeature.EnableMultiNetwork = tt.enableNetSeg || tt.enableMultiNetPolicies
			config.OVNKubernetesFeature.EnableNetworkSegmentation = tt.enableNetSeg
			config.OVNKubernetesFeature.EnableMultiNetworkPolicy = tt.enableMultiNetPolicies
			netInfo, err := util.NewNetInfo(tt.netCfg)
			require.NoError(t, err, "failed to create network info")
			bnc := &BaseNetworkController{
				ReconcilableNetInfo: util.NewReconcilableNetInfo(netInfo),
			}
			if tt.expectedReturn != bnc.shouldWatchNamespaces() {
				t.Fail()
			}
			assert.Equal(t, tt.expectedReturn, bnc.shouldWatchNamespaces())
		})
	}
}
