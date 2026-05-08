// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"k8s.io/apimachinery/pkg/util/sets"

	libovsdbutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/libovsdb/util"
)

// NamespaceAnnotationState holds parsed annotation data for a single
// namespace, mirroring NodeAnnotationState in pkg/controllers/node.
//
// Fields are unexported; handlers in pkg/ovn read state through accessor
// methods (so the cache can change shape without rippling through every
// caller). Package-level *Changed helpers compare two snapshots and answer
// "did this annotation change?" without requiring callers to reimplement
// the comparison.
type NamespaceAnnotationState struct {
	namespaceName string

	aclLogging    libovsdbutil.ACLLoggingLevels
	aclLoggingErr error

	multicastEnabled bool

	routingGWs    sets.Set[string]
	routingGWsErr error

	bfdEnabled bool
}

func newNamespaceAnnotationState(
	namespaceName string,
	aclLogging libovsdbutil.ACLLoggingLevels,
	aclLoggingErr error,
	multicastEnabled bool,
	routingGWs sets.Set[string],
	routingGWsErr error,
	bfdEnabled bool,
) *NamespaceAnnotationState {
	return &NamespaceAnnotationState{
		namespaceName:    namespaceName,
		aclLogging:       aclLogging,
		aclLoggingErr:    aclLoggingErr,
		multicastEnabled: multicastEnabled,
		routingGWs:       routingGWs,
		routingGWsErr:    routingGWsErr,
		bfdEnabled:       bfdEnabled,
	}
}

// NamespaceName returns the name of the namespace this state describes.
func (s *NamespaceAnnotationState) NamespaceName() string {
	if s == nil {
		return ""
	}
	return s.namespaceName
}

// ACLLogging returns a value copy of the parsed ACL-logging levels.
// Returns the zero value if state is nil or the annotation could not be
// parsed.
func (s *NamespaceAnnotationState) ACLLogging() libovsdbutil.ACLLoggingLevels {
	if s == nil {
		return libovsdbutil.ACLLoggingLevels{}
	}
	return s.aclLogging
}

// MulticastEnabled returns whether multicast is enabled by annotation on
// the namespace. Returns false if state is nil.
func (s *NamespaceAnnotationState) MulticastEnabled() bool {
	if s == nil {
		return false
	}
	return s.multicastEnabled
}

// ExternalGWs returns a copy of the parsed routing-external-gws set. The
// boolean error return is non-nil only when the annotation was present but
// failed to parse; an absent annotation returns an empty set with no
// error.
func (s *NamespaceAnnotationState) ExternalGWs() (sets.Set[string], error) {
	if s == nil {
		return sets.New[string](), nil
	}
	if s.routingGWsErr != nil {
		return nil, s.routingGWsErr
	}
	return sets.New(s.routingGWs.UnsortedList()...), nil
}

// BFDEnabled returns whether the namespace's BFD annotation is set.
// Returns false if state is nil.
func (s *NamespaceAnnotationState) BFDEnabled() bool {
	if s == nil {
		return false
	}
	return s.bfdEnabled
}

// ACLLoggingChanged returns true if the parsed ACL-logging value differs
// between two annotation snapshots. Two nil snapshots compare equal.
func ACLLoggingChanged(oldState, newState *NamespaceAnnotationState) bool {
	if oldState == nil && newState == nil {
		return false
	}
	if oldState == nil || newState == nil {
		return true
	}
	return oldState.aclLogging != newState.aclLogging
}

// MulticastChanged returns true if the parsed multicast-enabled flag
// differs between two annotation snapshots.
func MulticastChanged(oldState, newState *NamespaceAnnotationState) bool {
	if oldState == nil && newState == nil {
		return false
	}
	if oldState == nil || newState == nil {
		return true
	}
	return oldState.multicastEnabled != newState.multicastEnabled
}

// ExternalGWAnnotationChanged returns true if the parsed
// routing-external-gws annotation (or its BFD flag) differs between two
// snapshots. Two nil snapshots compare equal.
func ExternalGWAnnotationChanged(oldState, newState *NamespaceAnnotationState) bool {
	if oldState == nil && newState == nil {
		return false
	}
	if oldState == nil || newState == nil {
		return true
	}
	if oldState.bfdEnabled != newState.bfdEnabled {
		return true
	}
	// Either side may have a parse error; treat any error change as a change.
	if (oldState.routingGWsErr == nil) != (newState.routingGWsErr == nil) {
		return true
	}
	if oldState.routingGWsErr != nil {
		// Both have errors; compare the error strings to detect transitions
		// between distinct parse-failure modes.
		return oldState.routingGWsErr.Error() != newState.routingGWsErr.Error()
	}
	return !oldState.routingGWs.Equal(newState.routingGWs)
}
