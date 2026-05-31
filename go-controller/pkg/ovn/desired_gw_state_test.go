// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

func TestDesiredGWState_AddGWORCollision(t *testing.T) {
	d := newDesiredGWState()
	d.addGW("10.0.0.1", false)
	d.addGW("10.0.0.1", true)
	if !d.gws["10.0.0.1"] {
		t.Fatalf("addGW should OR BFD on collision: false|true should be true, got %v", d.gws["10.0.0.1"])
	}
	d.addGW("10.0.0.1", false)
	if !d.gws["10.0.0.1"] {
		t.Fatalf("addGW should not downgrade BFD once true; got %v", d.gws["10.0.0.1"])
	}
	d.addGW("10.0.0.2", false)
	if d.gws["10.0.0.2"] {
		t.Fatalf("addGW should preserve BFD=false when no source asks for it; got %v", d.gws["10.0.0.2"])
	}
}

func TestDesiredGWState_Equal(t *testing.T) {
	a := newDesiredGWState()
	b := newDesiredGWState()
	if !a.equal(b) {
		t.Fatal("two empty states should be equal")
	}
	a.addGW("10.0.0.1", true)
	if a.equal(b) {
		t.Fatal("non-empty vs empty should not be equal")
	}
	b.addGW("10.0.0.1", true)
	if !a.equal(b) {
		t.Fatal("same single (IP, BFD) tuple should be equal")
	}
	c := newDesiredGWState()
	c.addGW("10.0.0.1", false)
	if a.equal(c) {
		t.Fatal("same IP with different BFD should not be equal")
	}
	d := newDesiredGWState()
	d.addGW("10.0.0.2", true)
	if a.equal(d) {
		t.Fatal("different IP should not be equal")
	}
}

func TestDesiredGWState_AddAnnotationGWs(t *testing.T) {
	d := newDesiredGWState()
	d.addAnnotationGWs(gatewayInfo{
		gws:        sets.New("10.0.0.1", "10.0.0.2"),
		bfdEnabled: true,
	})
	if got, want := len(d.gws), 2; got != want {
		t.Fatalf("expected %d IPs, got %d", want, got)
	}
	if !d.gws["10.0.0.1"] || !d.gws["10.0.0.2"] {
		t.Fatalf("annotation gws should carry BFD=true uniformly: %v", d.gws)
	}
}

func TestDesiredGWState_AddAnnotationGWs_NilSet(t *testing.T) {
	d := newDesiredGWState()
	d.addAnnotationGWs(gatewayInfo{}) // nil set
	if d.size() != 0 {
		t.Fatalf("nil annotation gws should be a no-op; got %d entries", d.size())
	}
}

func TestDesiredGWState_AddPodGWs(t *testing.T) {
	d := newDesiredGWState()
	d.addPodGWs(map[string]bool{
		"10.0.0.1": true,
		"10.0.0.2": false,
	})
	if !d.gws["10.0.0.1"] {
		t.Fatalf("pod gw with BFD=true should land BFD=true; got %v", d.gws["10.0.0.1"])
	}
	if d.gws["10.0.0.2"] {
		t.Fatalf("pod gw with BFD=false should land BFD=false; got %v", d.gws["10.0.0.2"])
	}
}

func TestDesiredGWState_MergeAnnotationAndPod_ORCollision(t *testing.T) {
	annotation := gatewayInfo{
		gws:        sets.New("10.0.0.1"),
		bfdEnabled: false,
	}
	podGWs := map[string]bool{
		"10.0.0.1": true,  // same IP, conflicting BFD
		"10.0.0.9": false, // pod-only
	}
	got := computeDesiredGWState(annotation, podGWs)
	if !got.gws["10.0.0.1"] {
		t.Fatalf("collision IP should OR to BFD=true; got %v", got.gws["10.0.0.1"])
	}
	if got.gws["10.0.0.9"] {
		t.Fatalf("pod-only BFD=false should remain false; got %v", got.gws["10.0.0.9"])
	}
	if got.size() != 2 {
		t.Fatalf("expected 2 IPs, got %d", got.size())
	}
}

func TestDesiredGWState_MergeIsOrderIndependent(t *testing.T) {
	annotation := gatewayInfo{
		gws:        sets.New("10.0.0.1"),
		bfdEnabled: true,
	}
	podGWs := map[string]bool{
		"10.0.0.1": false,
	}
	a := computeDesiredGWState(annotation, podGWs)

	// Construct the same merge in the opposite order to verify
	// commutativity. (The OR-on-collision merge is associative and
	// commutative by construction; this guards against a future
	// refactor accidentally introducing a precedence rule.)
	b := newDesiredGWState()
	b.addPodGWs(podGWs)
	b.addAnnotationGWs(annotation)

	if !a.equal(b) {
		t.Fatalf("merge should be order-independent; a=%v b=%v", a.gws, b.gws)
	}
}

func TestDesiredGWState_IPSet(t *testing.T) {
	d := computeDesiredGWState(
		gatewayInfo{gws: sets.New("10.0.0.2"), bfdEnabled: false},
		map[string]bool{"10.0.0.1": true, "10.0.0.3": false},
	)
	got := d.ipSet()
	want := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ipSet: want %v got %v", want, got)
	}
}

func TestDesiredGWState_IPSet_Empty(t *testing.T) {
	if got := newDesiredGWState().ipSet(); got != nil {
		t.Fatalf("empty ipSet should be nil; got %v", got)
	}
	var d *desiredGWState
	if got := d.ipSet(); got != nil {
		t.Fatalf("nil receiver ipSet should be nil; got %v", got)
	}
}

func TestDesiredGWState_NilSizeAndEqual(t *testing.T) {
	var a *desiredGWState
	b := newDesiredGWState()
	if a.size() != 0 {
		t.Fatalf("nil receiver size should be 0; got %d", a.size())
	}
	if !b.equal(b) {
		t.Fatalf("self-equal must hold for empty state")
	}
}
