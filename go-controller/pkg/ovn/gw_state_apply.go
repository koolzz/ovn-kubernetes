// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"sort"
	"sync"
)

// gwIPBFD pairs a gateway IP with its BFD flag. Used for the add/replace
// legs of a gateway-state delta where per-IP BFD matters.
type gwIPBFD struct {
	ip  string
	bfd bool
}

// gwStateDelta describes the changes required to bring the per-namespace
// applied gateway state to the desired state. Three legs:
//
// - add: IPs present in desired but not in applied. Pure additions.
// - remove: IPs present in applied but not in desired. Pure deletions.
// - replace: IPs present in both, but BFD flag differs. Delete the old
//   route and add the new one — must be one libovsdb transaction so
//   traffic doesn't fall into a window with no route for the IP.
//
// An empty delta (all three slices empty) means applied == desired and
// no programming work is required.
type gwStateDelta struct {
	add     []gwIPBFD
	remove  []string
	replace []gwIPBFD
}

// empty reports whether the delta has any work to do.
func (d gwStateDelta) empty() bool {
	return len(d.add) == 0 && len(d.remove) == 0 && len(d.replace) == 0
}

// computeGWStateDelta returns the diff that brings applied to desired.
// A nil applied is equivalent to "nothing programmed yet" — every
// desired IP becomes an add. A nil desired means "remove everything" —
// every applied IP becomes a remove. Both nil yields an empty delta.
//
// Output slices are sorted by IP for deterministic ordering (helps both
// tests and operator-visible logs).
func computeGWStateDelta(applied, desired *desiredGWState) gwStateDelta {
	var delta gwStateDelta
	for ip, bfd := range desired.entries() {
		appliedBFD, ok := applied.entries()[ip]
		switch {
		case !ok:
			delta.add = append(delta.add, gwIPBFD{ip: ip, bfd: bfd})
		case appliedBFD != bfd:
			delta.replace = append(delta.replace, gwIPBFD{ip: ip, bfd: bfd})
		}
	}
	for ip := range applied.entries() {
		if _, ok := desired.entries()[ip]; !ok {
			delta.remove = append(delta.remove, ip)
		}
	}
	sortGWIPBFD(delta.add)
	sortGWIPBFD(delta.replace)
	sort.Strings(delta.remove)
	return delta
}

// entries returns the underlying map. Nil-receiver-safe.
func (d *desiredGWState) entries() map[string]bool {
	if d == nil {
		return nil
	}
	return d.gws
}

func sortGWIPBFD(s []gwIPBFD) {
	sort.Slice(s, func(i, j int) bool { return s[i].ip < s[j].ip })
}

// nsAppliedGWState tracks the last successfully-applied gateway state
// per namespace, scoped to a single controller (the default network is
// the only consumer today; UDNs don't have gateway pods, so they have
// no applied-state to track). Reads/writes are safe for concurrent use.
//
// Bootstrap rule: callers must seed this from NBDB before the first
// per-namespace reconcile fires after a controller restart, otherwise a
// namespace with empty desired state and stale OVN routes would compare
// applied.equal(desired) trivially and skip the delete that should fire.
type nsAppliedGWState struct {
	mu sync.RWMutex
	// byNamespace is keyed by namespace name. A nil value means "no
	// known applied state for this namespace" — distinct from "applied
	// state is empty" (an explicit empty desiredGWState). The
	// distinction matters during bootstrap: if NBDB seeding has not
	// yet recorded a namespace, the snapshot must not be treated as
	// "empty" (which would mask stale-route deletes).
	byNamespace map[string]*desiredGWState
}

func newNSAppliedGWState() *nsAppliedGWState {
	return &nsAppliedGWState{byNamespace: map[string]*desiredGWState{}}
}

// Get returns the last-applied state for the namespace, or nil if no
// state has been recorded. The returned pointer is the live cache
// entry; callers must treat it as read-only.
func (s *nsAppliedGWState) Get(ns string) *desiredGWState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byNamespace[ns]
}

// Set records the applied state for the namespace. The caller hands
// ownership of state to the cache; callers should not mutate it after
// Set. A nil state clears the entry (equivalent to Delete).
func (s *nsAppliedGWState) Set(ns string, state *desiredGWState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state == nil {
		delete(s.byNamespace, ns)
		return
	}
	s.byNamespace[ns] = state
}

// Delete removes the applied state for the namespace.
func (s *nsAppliedGWState) Delete(ns string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byNamespace, ns)
}

// Has reports whether the cache has recorded any state for the
// namespace. Returns true even when the recorded state is empty.
func (s *nsAppliedGWState) Has(ns string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.byNamespace[ns]
	return ok
}

// Namespaces returns a sorted list of namespaces with recorded applied
// state. Useful for bootstrap reconciliation that needs to walk every
// known namespace (e.g., to detect annotations that have been cleared
// while the controller was down).
func (s *nsAppliedGWState) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.byNamespace) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.byNamespace))
	for ns := range s.byNamespace {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out
}
