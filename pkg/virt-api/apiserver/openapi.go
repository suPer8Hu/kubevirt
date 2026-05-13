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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var info = &spec.Info{
	InfoProps: spec.InfoProps{
		Title:       "KubeVirt virt-api (aggregated)",
		Description: "KubeVirt's subresources.kubevirt.io API group, served via k8s.io/apiserver.",
		Contact: &spec.ContactInfo{
			Name:  "kubevirt-dev",
			Email: "kubevirt-dev@googlegroups.com",
			URL:   "https://github.com/kubevirt/kubevirt",
		},
		License: &spec.License{
			Name: "Apache 2.0",
			URL:  "https://www.apache.org/licenses/LICENSE-2.0",
		},
	},
}

func NewOpenAPIConfig(scheme *runtime.Scheme) *common.Config {
	_ = scheme
	return &common.Config{
		ProtocolList: []string{"https"},
		Info:         info,
		DefaultResponse: &spec.Response{
			ResponseProps: spec.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetDefinitions: emptyDefinitions,
	}
}

func NewOpenAPIV3Config(scheme *runtime.Scheme) *common.OpenAPIV3Config {
	_ = scheme
	return &common.OpenAPIV3Config{
		Info: info,
		DefaultResponse: &spec3.Response{
			ResponseProps: spec3.ResponseProps{
				Description: "Default Response.",
			},
		},
		GetDefinitions: emptyDefinitions,
	}
}

func emptyDefinitions(_ common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	return map[string]common.OpenAPIDefinition{}
}
