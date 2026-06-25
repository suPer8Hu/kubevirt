/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package lifecycle

import (
	"context"
	"fmt"
	"strings"

	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	"kubevirt.io/kubevirt/pkg/controller"
)

const (
	patchingVMFmt                            = "Patching VM: %s"
	patchingVMStatusFmt                      = "Patching VM status: %s"
	jsonpatchTestErr                         = "jsonpatch test operation does not apply"
	volumeMigrationManualRecoveryRequiredErr = "VM recovery required: Volume migration failed, leaving some volumes pointing to non-consistent targets, manual intervention is needed to reassign them to their original volumes."
)

// Handler executes lifecycle operations against the cluster
type Handler struct {
	virtCli kubecli.KubevirtClient
}

func NewHandler(virtCli kubecli.KubevirtClient) *Handler {
	return &Handler{virtCli: virtCli}
}

func (h *Handler) StartVM(ctx context.Context, namespace, name string, startOptions *v1.StartOptions) *errors.StatusError {
	vm, err := h.virtCli.VirtualMachine(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return errors.NewNotFound(v1.Resource("virtualmachine"), name)
		}
		return errors.NewInternalError(fmt.Errorf("unable to retrieve vm [%s]: %v", name, err))
	}

	vmi, err := h.virtCli.VirtualMachineInstance(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return errors.NewInternalError(err)
	}

	if vmi != nil && !vmi.IsFinal() && vmi.Status.Phase != v1.Unknown && vmi.Status.Phase != v1.VmPhaseUnset {
		return errors.NewConflict(v1.Resource("virtualmachine"), name, fmt.Errorf("VM is already running"))
	}
	if controller.NewVirtualMachineConditionManager().HasConditionWithStatus(vm, v1.VirtualMachineManualRecoveryRequired, k8sv1.ConditionTrue) {
		return errors.NewConflict(v1.Resource("virtualmachine"), name, fmt.Errorf(volumeMigrationManualRecoveryRequiredErr))
	}

	startPaused := startOptions.Paused
	startChangeRequestData := make(map[string]string)
	if startPaused {
		startChangeRequestData[v1.StartRequestDataPausedKey] = v1.StartRequestDataPausedTrue
	}

	var patchErr error

	runStrategy, err := vm.RunStrategy()
	if err != nil {
		return errors.NewInternalError(err)
	}
	// RunStrategyHalted         -> spec.running = true / send start request for paused start
	// RunStrategyManual         -> send start request
	// RunStrategyAlways         -> doesn't make sense
	// RunStrategyRerunOnFailure -> doesn't make sense
	// RunStrategyOnce           -> doesn't make sense
	switch runStrategy {
	case v1.RunStrategyHalted:
		pausedStartStrategy := v1.StartStrategyPaused
		if startPaused && (vm.Spec.Template == nil || vm.Spec.Template.Spec.StartStrategy != &pausedStartStrategy) {
			patchBytes, err := getChangeRequestJson(vm, v1.VirtualMachineStateChangeRequest{
				Action: v1.StartRequest,
				Data:   startChangeRequestData,
			})
			if err != nil {
				return errors.NewInternalError(err)
			}
			log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
			_, patchErr = h.virtCli.VirtualMachine(vm.Namespace).PatchStatus(ctx, vm.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{DryRun: startOptions.DryRun})
		} else {
			patchBytes, err := getRunningPatch(vm, true)
			if err != nil {
				return errors.NewInternalError(err)
			}
			log.Log.Object(vm).V(4).Infof(patchingVMFmt, string(patchBytes))
			_, patchErr = h.virtCli.VirtualMachine(namespace).Patch(ctx, vm.GetName(), types.JSONPatchType, patchBytes, metav1.PatchOptions{DryRun: startOptions.DryRun})
		}

	case v1.RunStrategyRerunOnFailure, v1.RunStrategyManual:
		needsRestart := false
		if (runStrategy == v1.RunStrategyRerunOnFailure && vmi != nil && vmi.Status.Phase == v1.Succeeded) ||
			(runStrategy == v1.RunStrategyManual && vmi != nil && vmi.IsFinal()) {
			needsRestart = true
		} else if runStrategy == v1.RunStrategyRerunOnFailure && vmi != nil && vmi.Status.Phase == v1.Failed {
			return errors.NewConflict(v1.Resource("virtualmachine"), name, fmt.Errorf("%v does not support starting VM from failed state", v1.RunStrategyRerunOnFailure))
		}

		var patchBytes []byte
		if needsRestart {
			patchBytes, err = getChangeRequestJson(vm,
				v1.VirtualMachineStateChangeRequest{Action: v1.StopRequest, UID: &vmi.UID},
				v1.VirtualMachineStateChangeRequest{Action: v1.StartRequest, Data: startChangeRequestData})
		} else {
			patchBytes, err = getChangeRequestJson(vm,
				v1.VirtualMachineStateChangeRequest{Action: v1.StartRequest, Data: startChangeRequestData})
		}
		if err != nil {
			return errors.NewInternalError(err)
		}
		log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
		_, patchErr = h.virtCli.VirtualMachine(vm.Namespace).PatchStatus(ctx, vm.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{DryRun: startOptions.DryRun})
	case v1.RunStrategyAlways, v1.RunStrategyOnce:
		return errors.NewConflict(v1.Resource("virtualmachine"), name, fmt.Errorf("%v does not support manual start requests", runStrategy))
	}

	if patchErr != nil {
		if strings.Contains(patchErr.Error(), jsonpatchTestErr) {
			return errors.NewConflict(v1.Resource("virtualmachine"), name, patchErr)
		}
		return errors.NewInternalError(patchErr)
	}

	return nil
}

func getChangeRequestJson(vm *v1.VirtualMachine, changes ...v1.VirtualMachineStateChangeRequest) ([]byte, error) {
	patchSet := patch.New()
	// if there's no status field, add one
	newStatus := v1.VirtualMachineStatus{}
	if equality.Semantic.DeepEqual(vm.Status, newStatus) {
		newStatus.StateChangeRequests = append(newStatus.StateChangeRequests, changes...)
		patchSet.AddOption(patch.WithAdd("/status", newStatus))
	} else {
		patchSet.AddOption(patch.WithTest("/status/stateChangeRequests", vm.Status.StateChangeRequests))
		switch {
		case len(vm.Status.StateChangeRequests) == 0:
			patchSet.AddOption(patch.WithAdd("/status/stateChangeRequests", changes))
		case len(changes) == 1 && changes[0].Action == v1.StopRequest:
			// If this is a stopRequest, replace all existing StateChangeRequests.
			patchSet.AddOption(patch.WithReplace("/status/stateChangeRequests", changes))
		default:
			return nil, fmt.Errorf("unable to complete request: stop/start already underway")
		}
	}

	if vm.Status.StartFailure != nil {
		patchSet.AddOption(patch.WithRemove("/status/startFailure"))
	}

	return patchSet.GeneratePayload()
}

func getRunningPatch(vm *v1.VirtualMachine, running bool) ([]byte, error) {
	runStrategy := v1.RunStrategyHalted
	if running {
		runStrategy = v1.RunStrategyAlways
	}

	if vm.Spec.RunStrategy != nil {
		return patch.New(
			patch.WithTest("/spec/runStrategy", vm.Spec.RunStrategy),
			patch.WithReplace("/spec/runStrategy", runStrategy),
		).GeneratePayload()
	}

	return patch.New(
		patch.WithTest("/spec/running", vm.Spec.Running),
		patch.WithReplace("/spec/running", running),
	).GeneratePayload()
}
