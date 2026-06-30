// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package ovn

import (
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	controllerutil "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controller"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/kubevirt"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	utilerrors "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util/errors"
)

func (bnc *BaseNetworkController) ensurePodStateCaches() {
	if bnc.appliedPods == nil {
		bnc.appliedPods = syncmap.NewSyncMap[*corev1.Pod]()
	}
	if bnc.deletedPods == nil {
		bnc.deletedPods = syncmap.NewSyncMap[string]()
	}
}

func podNeedsUpdate(_, _ *corev1.Pod) bool {
	return true
}

func (bnc *BaseNetworkController) podAppliedStateChanged(appliedPod, pod *corev1.Pod) bool {
	if appliedPod == nil || pod == nil {
		return false
	}
	if appliedPod.UID != "" && pod.UID != "" && appliedPod.UID != pod.UID {
		return true
	}
	return bnc.podNetworkAnnotationChanged(appliedPod, pod)
}

func (bnc *BaseNetworkController) podNetworkAnnotationChanged(oldPod, newPod *corev1.Pod) bool {
	nadKeys := map[string]struct{}{}
	for _, nadKey := range bnc.getPodNADKeys(oldPod) {
		nadKeys[nadKey] = struct{}{}
	}
	for _, nadKey := range bnc.getPodNADKeys(newPod) {
		nadKeys[nadKey] = struct{}{}
	}

	for nadKey := range nadKeys {
		oldAnnotation, oldErr := util.UnmarshalPodAnnotation(oldPod.Annotations, nadKey)
		newAnnotation, newErr := util.UnmarshalPodAnnotation(newPod.Annotations, nadKey)
		oldMissing := util.IsAnnotationNotSetError(oldErr)
		newMissing := util.IsAnnotationNotSetError(newErr)
		if oldMissing && newMissing {
			continue
		}
		if newMissing {
			continue
		}
		if oldMissing {
			if bnc.podLogicalPortCacheStale(newPod, nadKey, newAnnotation) {
				return true
			}
			continue
		}
		if newErr != nil {
			continue
		}
		if oldErr != nil {
			return true
		}
		if !reflect.DeepEqual(oldAnnotation, newAnnotation) {
			return true
		}
	}
	return false
}

func (bnc *BaseNetworkController) podLogicalPortCacheStale(pod *corev1.Pod, nadKey string, podAnnotation *util.PodAnnotation) bool {
	portInfo, err := bnc.logicalPortCache.get(pod, nadKey)
	if err != nil {
		return false
	}
	return !portInfo.expires.IsZero() || !util.IsIPNetsEqual(portInfo.ips, podAnnotation.IPs)
}

func podForAbsentReconcile(pod *corev1.Pod) *corev1.Pod {
	if pod == nil || !kubevirt.IsPodLiveMigratable(pod) || util.PodCompleted(pod) {
		return pod
	}
	pod = pod.DeepCopy()
	pod.Status.Phase = corev1.PodSucceeded
	return pod
}

func (bnc *BaseNetworkController) initPodController(name string, reconcile func(string) error, initialSync func([]interface{}) error) {
	bnc.ensurePodStateCaches()
	podInformer := bnc.watchFactory.PodCoreInformer()
	bnc.podController = controllerutil.NewController[corev1.Pod](
		name,
		&controllerutil.ControllerConfig[corev1.Pod]{
			RateLimiter:    workqueue.DefaultTypedControllerRateLimiter[string](),
			Reconcile:      reconcile,
			ObjNeedsUpdate: podNeedsUpdate,
			MaxAttempts:    controllerutil.InfiniteAttempts,
			// Keep pod reconcile serialized initially to match the old watcher path;
			// this is the scale tuning knob once keyed reconcile has soak time.
			Threadiness: 1,
			Informer:    podInformer.Informer(),
			Lister:      podInformer.Lister().List,
		},
	)
	bnc.podControllerInitialSync = initialSync
}

func (bnc *BaseNetworkController) runPodControllerInitialSync() error {
	if bnc.podControllerInitialSync == nil {
		return nil
	}

	pods, err := bnc.watchFactory.PodCoreInformer().Lister().List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list pods for initial sync on network %s: %w", bnc.GetNetworkName(), err)
	}
	objs := make([]interface{}, 0, len(pods))
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	if err := bnc.podControllerInitialSync(objs); err != nil {
		return err
	}
	return nil
}

func (bnc *BaseNetworkController) startPodController() error {
	if bnc.podController == nil {
		return fmt.Errorf("pod controller for network %s is not initialized", bnc.GetNetworkName())
	}
	if bnc.podControllerStarted {
		return nil
	}
	if err := controllerutil.StartWithInitialSync(bnc.runPodControllerInitialSync, bnc.podController); err != nil {
		return err
	}
	bnc.podControllerStarted = true
	return nil
}

func (bnc *BaseNetworkController) stopPodController() {
	if bnc.podController == nil || !bnc.podControllerStarted {
		return
	}
	controllerutil.Stop(bnc.podController)
	bnc.podControllerStarted = false
}

func (bnc *BaseNetworkController) recordAppliedPod(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		klog.Errorf("Failed to get pod key for applied pod cache on network %s: %v", bnc.GetNetworkName(), err)
		return
	}

	bnc.ensurePodStateCaches()
	bnc.appliedPods.LockKey(key)
	bnc.appliedPods.Store(key, pod.DeepCopy())
	bnc.appliedPods.UnlockKey(key)
	bnc.clearDeletedPod(key)
}

func (bnc *BaseNetworkController) getAppliedPod(key string) (*corev1.Pod, bool) {
	bnc.ensurePodStateCaches()
	bnc.appliedPods.LockKey(key)
	defer bnc.appliedPods.UnlockKey(key)
	pod, ok := bnc.appliedPods.Load(key)
	if !ok || pod == nil {
		return nil, false
	}
	return pod.DeepCopy(), true
}

func (bnc *BaseNetworkController) forgetAppliedPod(key string) {
	bnc.ensurePodStateCaches()
	bnc.appliedPods.LockKey(key)
	bnc.appliedPods.Delete(key)
	bnc.appliedPods.UnlockKey(key)
}

func (bnc *BaseNetworkController) markDeletedPod(key string, pod *corev1.Pod) {
	if pod == nil || pod.UID == "" {
		return
	}
	bnc.ensurePodStateCaches()
	bnc.deletedPods.LockKey(key)
	bnc.deletedPods.Store(key, string(pod.UID))
	bnc.deletedPods.UnlockKey(key)
}

func (bnc *BaseNetworkController) clearDeletedPod(key string) {
	bnc.ensurePodStateCaches()
	bnc.deletedPods.LockKey(key)
	bnc.deletedPods.Delete(key)
	bnc.deletedPods.UnlockKey(key)
}

func (bnc *BaseNetworkController) wasDeletedPodProcessed(key string, pod *corev1.Pod) bool {
	if pod == nil || pod.UID == "" {
		return false
	}
	bnc.ensurePodStateCaches()
	bnc.deletedPods.LockKey(key)
	defer bnc.deletedPods.UnlockKey(key)
	uid, ok := bnc.deletedPods.Load(key)
	return ok && uid == string(pod.UID)
}

func (bnc *BaseNetworkController) enqueuePod(pod *corev1.Pod, reason string) error {
	key, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return err
	}
	klog.V(5).Infof("Queueing pod %s for network %s reconciliation: %s", key, bnc.GetNetworkName(), reason)
	bnc.podController.Reconcile(key)
	return nil
}

func (bnc *BaseNetworkController) requeuePendingPods() error {
	if bnc.podController == nil {
		return fmt.Errorf("pod controller for network %s is not initialized", bnc.GetNetworkName())
	}

	var errs []error
	allPods, err := bnc.watchFactory.GetAllPods()
	if err != nil {
		return fmt.Errorf("failed to get all pods: %w", err)
	}

	for _, pod := range allPods {
		if !util.PodScheduled(pod) || pod.Status.Phase != corev1.PodPending {
			continue
		}
		if err := bnc.enqueuePod(pod, "pending pod requeue"); err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.Join(errs...)
}

func (bnc *BaseNetworkController) getPodForReconcile(key string) (*corev1.Pod, bool, error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, false, err
	}
	pod, err := bnc.watchFactory.PodCoreInformer().Lister().Pods(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return pod, true, nil
}
