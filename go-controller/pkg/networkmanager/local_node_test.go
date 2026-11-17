// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package networkmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cnitypes "github.com/containernetworking/cni/pkg/types"
	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	informerfactory "k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	ovncnitypes "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

type localNodeWatchFactory struct {
	watchFactory
	informer coreinformers.NodeInformer
}

func (f localNodeWatchFactory) NodeCoreInformer() coreinformers.NodeInformer { return f.informer }

type localNodeTestController struct {
	FakeNetworkController
	notifications atomic.Int32
}

func (c *localNodeTestController) ReconcileLocalNode() { c.notifications.Add(1) }

func TestLocalNodeSubnetNotifications(t *testing.T) {
	if err := config.PrepareTestConfig(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = config.PrepareTestConfig() })
	factory := informerfactory.NewSharedInformerFactory(fake.NewSimpleClientset(), 0)
	nm := newNetworkController("test", "node-a", &FakeControllerManager{}, localNodeWatchFactory{informer: factory.Core().V1().Nodes()})
	if nm.nodeController == nil {
		t.Fatal("local Node listener must exist with route advertisements disabled")
	}
	newObserved := func(name string) *localNodeTestController {
		t.Helper()
		info, err := util.NewNetInfo(&ovncnitypes.NetConf{NetConf: cnitypes.NetConf{Name: name, Type: "ovn-k8s-cni-overlay"}, Topology: types.Layer3Topology, Role: types.NetworkRolePrimary, Subnets: "10.128.0.0/14"})
		if err != nil {
			t.Fatal(err)
		}
		nm.setNetwork(name, util.NewMutableNetInfo(info))
		return &localNodeTestController{FakeNetworkController: FakeNetworkController{NetInfo: info}}
	}
	active, stopped, failed := newObserved("active"), newObserved("stopped"), newObserved("failed")
	nm.networkControllers["active"] = &networkControllerState{controller: active}
	nm.networkControllers["stopped"] = &networkControllerState{controller: stopped, stoppedAndDeleting: true}
	nm.networkControllers["failed"] = &networkControllerState{controller: failed, startFailed: true}
	other := newObserved("other")
	nm.networkControllers["other"] = &networkControllerState{controller: &other.FakeNetworkController}
	t.Cleanup(nm.Stop)
	if err := nm.syncNode("node-b"); err != nil {
		t.Fatal(err)
	}
	if active.notifications.Load() != 0 {
		t.Fatal("remote Node notified local controller")
	}
	if err := nm.syncNode("node-a"); err != nil {
		t.Fatal(err)
	}
	if active.notifications.Load() != 1 || stopped.notifications.Load() != 0 || failed.notifications.Load() != 0 {
		t.Fatalf("wrong lifecycle recipients: %d/%d/%d", active.notifications.Load(), stopped.notifications.Load(), failed.notifications.Load())
	}

	old := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", ResourceVersion: "1", Annotations: map[string]string{types.NodeSubnetsAnnotation: `{"red":["10.0.0.0/24"]}`}}}
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Node)
		want   bool
	}{
		{"new allocation", func(n *corev1.Node) { n.Annotations[types.NodeSubnetsAnnotation] = `{"red":["10.0.1.0/24"]}` }, true},
		{"allocation absent", func(n *corev1.Node) { delete(n.Annotations, types.NodeSubnetsAnnotation) }, true},
		{"unrelated annotation", func(n *corev1.Node) { n.Annotations["unrelated"] = "changed" }, false},
		{"labels without route advertisements", func(n *corev1.Node) { n.Labels = map[string]string{"x": "y"} }, false},
		{"status", func(n *corev1.Node) { n.Status.Phase = corev1.NodeRunning }, false},
		{"unchanged", func(*corev1.Node) {}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := old.DeepCopy()
			n.ResourceVersion = "2"
			tc.mutate(n)
			if got := nm.nodeNeedsUpdate(old, n); got != tc.want {
				t.Fatalf("needs update %v, want %v", got, tc.want)
			}
		})
	}
	remote := old.DeepCopy()
	remote.Name = "node-b"
	if nm.nodeNeedsUpdate(nil, remote) || !nm.nodeNeedsUpdate(nil, old) {
		t.Fatal("incorrect initial local/remote filter")
	}
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableRouteAdvertisements = true
	changed := old.DeepCopy()
	changed.ResourceVersion = "2"
	changed.Name = "node-b"
	changed.Labels = map[string]string{"x": "y"}
	if !nm.nodeNeedsUpdate(remote, changed) {
		t.Fatal("existing route-advertisement label trigger was lost")
	}
}

func TestLocalNodeInformerDispatch(t *testing.T) {
	if err := config.PrepareTestConfig(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = config.PrepareTestConfig() })
	g := gomega.NewWithT(t)
	client := fake.NewSimpleClientset()
	factory := informerfactory.NewSharedInformerFactory(client, 0)
	informer := factory.Core().V1().Nodes()
	nm := newNetworkController("test", "node-a", &FakeControllerManager{}, localNodeWatchFactory{informer: informer})
	info, err := util.NewNetInfo(&ovncnitypes.NetConf{NetConf: cnitypes.NetConf{Name: "red", Type: "ovn-k8s-cni-overlay"}, Topology: types.Layer3Topology, Role: types.NetworkRolePrimary, Subnets: "10.128.0.0/14"})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	observer := &localNodeTestController{FakeNetworkController: FakeNetworkController{NetInfo: info}}
	nm.setNetwork("red", util.NewMutableNetInfo(info))
	nm.setNetworkState("red", &networkControllerState{controller: observer})
	stop := make(chan struct{})
	t.Cleanup(func() { nm.Stop(); close(stop); factory.Shutdown() })
	factory.Start(stop)
	g.Expect(cache.WaitForCacheSync(stop, informer.Informer().HasSynced)).To(gomega.BeTrue())
	g.Expect(controller.Start(nm.nodeController)).To(gomega.Succeed())
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "same-node", ResourceVersion: "1", Annotations: map[string]string{types.NodeSubnetsAnnotation: `{"red":["10.128.0.0/24"]}`}}}
	_, err = client.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(observer.notifications.Load, 3*time.Second).Should(gomega.Equal(int32(1)))
	updated := node.DeepCopy()
	updated.ResourceVersion = "2"
	updated.Annotations[types.NodeSubnetsAnnotation] = `{"red":["10.128.1.0/24"]}`
	_, err = client.CoreV1().Nodes().Update(context.Background(), updated, metav1.UpdateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(observer.notifications.Load, 3*time.Second).Should(gomega.Equal(int32(2)))
	irrelevant := updated.DeepCopy()
	irrelevant.ResourceVersion = "3"
	irrelevant.Annotations["unrelated"] = "value"
	_, err = client.CoreV1().Nodes().Update(context.Background(), irrelevant, metav1.UpdateOptions{})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Eventually(func() string {
		n, e := informer.Lister().Get("node-a")
		if e != nil {
			return ""
		}
		return n.ResourceVersion
	}, 3*time.Second).Should(gomega.Equal("3"))
	g.Consistently(observer.notifications.Load, 100*time.Millisecond).Should(gomega.Equal(int32(2)))
}
