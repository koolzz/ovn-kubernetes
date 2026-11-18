// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package routeimport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	controllerutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	multinetworkmocks "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/mocks/multinetwork"
)

type subscriptionNetlink struct {
	util.NetLinkOps
	routes     chan chan<- netlink.RouteUpdate
	links      chan chan<- netlink.LinkUpdate
	failRoutes atomic.Bool
	failLinks  atomic.Bool
}

func (n *subscriptionNetlink) RouteSubscribeWithOptions(ch chan<- netlink.RouteUpdate, _ <-chan struct{}, _ netlink.RouteSubscribeOptions) error {
	if n.failRoutes.Swap(false) {
		return errors.New("route subscription unavailable")
	}
	n.routes <- ch
	return nil
}

func (n *subscriptionNetlink) LinkSubscribeWithOptions(ch chan<- netlink.LinkUpdate, _ <-chan struct{}, _ netlink.LinkSubscribeOptions) error {
	if n.failLinks.Swap(false) {
		return errors.New("link subscription unavailable")
	}
	n.links <- ch
	return nil
}

func TestSubscriptionReconnectReconcilesNetworks(t *testing.T) {
	for _, stream := range []string{"route", "link"} {
		for _, retry := range []bool{false, true} {
			name := stream + "/immediate"
			if retry {
				name = stream + "/retry"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					nl := &subscriptionNetlink{
						routes: make(chan chan<- netlink.RouteUpdate, 2),
						links:  make(chan chan<- netlink.LinkUpdate, 2),
					}
					defaultNetwork := &util.DefaultNetInfo{}
					udn := &multinetworkmocks.NetInfo{}
					udn.On("GetNetworkName").Return("udn")
					var reconciled []string
					c := &controller{
						ctx:        util.NewCancelableContext(),
						log:        logr.Discard(),
						netlink:    nl,
						networkIDs: map[int]string{0: "default", 1: "udn"},
						networks:   map[string]util.NetInfo{"default": defaultNetwork, "udn": udn},
						tables:     map[int]int{unix.RT_TABLE_MAIN: 0, 1001: 1},
						reconciler: controllerutil.NewReconciler("subscription-test", &controllerutil.ReconcilerConfig{
							Threadiness: 1,
							Reconcile: func(_ context.Context, key string) error {
								reconciled = append(reconciled, key)
								return nil
							},
						}),
					}
					require.NoError(t, c.Start())
					defer c.Stop()
					synctest.Wait()
					routes := <-nl.routes
					links := <-nl.links
					require.Empty(t, reconciled)

					if stream == "route" {
						nl.failRoutes.Store(retry)
						close(routes)
					} else {
						nl.failLinks.Store(retry)
						close(links)
					}
					synctest.Wait()
					if retry {
						// Failed subscription attempts must not start repeated full dumps.
						require.Empty(t, reconciled)
						time.Sleep(subscribePeriod)
						synctest.Wait()
					}
					if stream == "route" {
						routes = <-nl.routes
					} else {
						<-nl.links
					}
					time.Sleep(reconcileDelay)
					synctest.Wait()
					// Neither existing routes nor VRF link events need to replay.
					// A full reconcile also discovers withdrawals missed during the gap.
					require.ElementsMatch(t, []string{"default", "udn"}, reconciled)
					require.Same(t, defaultNetwork, c.getNetworkForTable(unix.RT_TABLE_MAIN))
					require.Same(t, udn, c.getNetworkForTable(1001))

					for table, network := range map[int]string{unix.RT_TABLE_MAIN: "default", 1001: "udn"} {
						reconciled = nil
						routes <- netlink.RouteUpdate{Route: netlink.Route{Protocol: unix.RTPROT_BGP, Table: table}}
						synctest.Wait()
						time.Sleep(reconcileDelay)
						synctest.Wait()
						require.Equal(t, []string{network}, reconciled)
					}
				})
			})
		}
	}
}
