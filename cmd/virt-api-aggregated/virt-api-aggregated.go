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

// entry point for the virt-api migration.
//
// It hosts the same subresources.kubevirt.io API group as the legacy virt-api
// binary (cmd/virt-api), but wires it up via k8s.io/apiserver's
// GenericAPIServer rather than the hand-rolled go-restful + net/http stack.
//
// At the bootstrap stage no storages are registered yet; the binary's only job
// is to prove that the Options -> Config -> New() -> Run() path works against
// KubeVirt's GroupVersions. Storages will be added one by one in subsequent
// iterations.
package main

import (
	"os"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"kubevirt.io/kubevirt/pkg/virt-api/apiserver"
)

func main() {
	s := apiserver.New()

	s.AddFlags(pflag.CommandLine)
	pflag.Parse()

	scheme := apiserver.NewScheme()

	// At the bootstrap stage we deliberately install zero API groups: the goal
	// of this binary is just to prove the GenericAPIServer wiring works. The
	// first storages (DummyREST + a single rest.Connecter such as freeze) will
	// be added in a follow-up commit, mirroring virt-template's approach.
	apiGroups := apiserver.APIGroups{}

	if err := s.Run(
		"virt-api-aggregated",
		scheme,
		apiserver.NewOpenAPIConfig(scheme),
		apiserver.NewOpenAPIV3Config(scheme),
		apiGroups,
	); err != nil {
		klog.Errorf("virt-api-aggregated exited with error: %v", err)
		os.Exit(1)
	}
}
