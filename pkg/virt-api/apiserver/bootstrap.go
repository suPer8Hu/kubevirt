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

package apiserver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/client-go/rest"
)

func TryBootstrap() error {
	s := New()
	s.secureServingOpts.BindPort = 0
	s.secureServingOpts.BindAddress = nil

	scheme := NewScheme()

	factory := serializer.NewCodecFactory(scheme)
	config := genericapiserver.NewRecommendedConfig(factory)
	config.EffectiveVersion = compatibility.DefaultBuildEffectiveVersion()
	config.EnableDiscovery = false

	config.ExternalAddress = "127.0.0.1:443"
	config.LoopbackClientConfig = &rest.Config{Host: "https://127.0.0.1"}

	server, err := config.Complete().New("virt-api-bootstrap", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return fmt.Errorf("virt-api generic apiserver bootstrap: %w", err)
	}
	if server == nil {
		return fmt.Errorf("virt-api generic apiserver bootstrap: nil server returned")
	}

	return nil
}
