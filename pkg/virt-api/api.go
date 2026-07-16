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

package virt_api

import (
	"context"
	"crypto/tls"
	goflag "flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	kvtls "kubevirt.io/kubevirt/pkg/util/tls"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/tools/cache"
	certificate2 "k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/flowcontrol"
	aggregatorclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"

	"kubevirt.io/kubevirt/pkg/util/ratelimiter"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
	clientutil "kubevirt.io/client-go/util"

	"kubevirt.io/kubevirt/pkg/certificates/bootstrap"
	"kubevirt.io/kubevirt/pkg/controller"
	clientmetrics "kubevirt.io/kubevirt/pkg/monitoring/metrics/common/client"
	metrics "kubevirt.io/kubevirt/pkg/monitoring/metrics/virt-api"
	netadmitter "kubevirt.io/kubevirt/pkg/network/admitter"
	"kubevirt.io/kubevirt/pkg/service"

	apiserver "kubevirt.io/kubevirt/pkg/virt-api/apiserver"
	"kubevirt.io/kubevirt/pkg/virt-api/apiserver/storage/virtualmachine"
	"kubevirt.io/kubevirt/pkg/virt-api/apiserver/storage/virtualmachineinstance"
	"kubevirt.io/kubevirt/pkg/virt-api/expand"
	"kubevirt.io/kubevirt/pkg/virt-api/webhooks"
	mutating_webhook "kubevirt.io/kubevirt/pkg/virt-api/webhooks/mutating-webhook"
	validating_webhook "kubevirt.io/kubevirt/pkg/virt-api/webhooks/validating-webhook"
	virtconfig "kubevirt.io/kubevirt/pkg/virt-config"
	"kubevirt.io/kubevirt/pkg/virt-operator/resource/generate/components"
)

const (
	// Default port that virt-api listens on.
	defaultPort = 443

	DefaultConsoleServerPort = 8186

	defaultCAConfigMapName     = "kubevirt-ca"
	defaultTlsCertFilePath     = "/etc/virt-api/certificates/tls.crt"
	defaultTlsKeyFilePath      = "/etc/virt-api/certificates/tls.key"
	defaultHandlerCertFilePath = "/etc/virt-handler/clientcertificates/tls.crt"
	defaultHandlerKeyFilePath  = "/etc/virt-handler/clientcertificates/tls.key"
)

type VirtApi interface {
	Run()
	AddFlags()
	Execute()
}

type virtAPIApp struct {
	apiServer *apiserver.APIServer

	// LEGACY(virt-api-migration): still passed by virt-operator but no longer read
	SubresourcesOnly bool
	virtCli          kubecli.KubevirtClient
	aggregatorClient *aggregatorclient.Clientset
	clusterConfig    *virtconfig.ClusterConfig

	namespace               string
	host                    string
	consoleServerPort       int
	handlerTLSConfiguration *tls.Config
	handlerCertManager      certificate2.Manager
	handlerCertFilePath     string
	handlerKeyFilePath      string

	// Serving certificate handed to the GenericAPIServer.
	caConfigMapName   string
	externallyManaged bool

	reloadableRateLimiter        *ratelimiter.ReloadableRateLimiter
	reloadableWebhookRateLimiter *ratelimiter.ReloadableRateLimiter

	// indicates if controllers were started with or without CDI/DataSource support
	hasCDIDataSource bool
	// the channel used to trigger re-initialization.
	reInitChan chan string

	kubeVirtServiceAccounts map[string]struct{}
}

var _ service.Service = &virtAPIApp{}

func NewVirtApi() VirtApi {

	app := &virtAPIApp{}
	app.apiServer = apiserver.New().
		WithSecureServingPort(defaultPort).
		WithSecureServingCert(defaultTlsCertFilePath, defaultTlsKeyFilePath)

	return app
}

func (app *virtAPIApp) Execute() {
	if err := metrics.SetupMetrics(); err != nil {
		panic(err)
	}

	app.reloadableRateLimiter = ratelimiter.NewReloadableRateLimiter(flowcontrol.NewTokenBucketRateLimiter(virtconfig.DefaultVirtAPIQPS, virtconfig.DefaultVirtAPIBurst))
	app.reloadableWebhookRateLimiter = ratelimiter.NewReloadableRateLimiter(flowcontrol.NewTokenBucketRateLimiter(virtconfig.DefaultVirtWebhookClientQPS, virtconfig.DefaultVirtWebhookClientBurst))

	clientmetrics.RegisterRestConfigHooks()
	clientConfig, err := kubecli.GetKubevirtClientConfig()
	if err != nil {
		panic(err)
	}
	clientConfig.RateLimiter = app.reloadableRateLimiter
	app.virtCli, err = kubecli.GetKubevirtClientFromRESTConfig(clientConfig)
	if err != nil {
		panic(err)
	}

	app.aggregatorClient = aggregatorclient.NewForConfigOrDie(clientConfig)

	app.namespace, err = clientutil.GetNamespace()
	if err != nil {
		panic(err)
	}

	app.kubeVirtServiceAccounts = webhooks.KubeVirtServiceAccounts(app.namespace)

	app.reInitChan = make(chan string, 10)

	app.Run()
}

func (app *virtAPIApp) prepareCertManager() {
	app.handlerCertManager = bootstrap.NewFileCertificateManager(app.handlerCertFilePath, app.handlerKeyFilePath)
}

// LEGACY(virt-api-migration): registered on http.DefaultServeMux and served through
// the bridge. Migrate to apiserver.MuxHandler (see VMValidatePath) and remove
func (app *virtAPIApp) registerValidatingWebhooks(informers *webhooks.Informers) {
	http.HandleFunc(components.VMICreateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMICreate(w, r, app.clusterConfig, app.kubeVirtServiceAccounts,
			func(field *field.Path, vmiSpec *v1.VirtualMachineInstanceSpec, clusterCfg *virtconfig.ClusterConfig) []metav1.StatusCause {
				return netadmitter.Validate(field, vmiSpec, clusterCfg)
			},
		)
	})
	http.HandleFunc(components.VMIUpdateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMIUpdate(w, r, app.clusterConfig, app.kubeVirtServiceAccounts)
	})
	http.HandleFunc(components.VMIRSValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMIRS(w, r, app.clusterConfig)
	})
	http.HandleFunc(components.VMPoolValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMPool(w, r, app.clusterConfig, app.kubeVirtServiceAccounts)
	})
	http.HandleFunc(components.VMIPresetValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMIPreset(w, r)
	})
	http.HandleFunc(components.MigrationCreateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeMigrationCreate(w, r, app.clusterConfig, app.virtCli, app.kubeVirtServiceAccounts)
	})
	http.HandleFunc(components.MigrationUpdateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeMigrationUpdate(w, r)
	})
	http.HandleFunc(components.VMSnapshotValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMSnapshots(w, r, app.clusterConfig, app.virtCli)
	})
	http.HandleFunc(components.VMRestoreValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMRestores(w, r, app.clusterConfig, app.virtCli, informers)
	})
	http.HandleFunc(components.VMBackupValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMBackups(w, r, app.clusterConfig, app.virtCli, informers)
	})
	http.HandleFunc(components.VMBackupTrackerValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMBackupTrackers(w, r, app.clusterConfig)
	})
	http.HandleFunc(components.VMExportValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVMExports(w, r, app.clusterConfig)
	})
	http.HandleFunc(components.VMInstancetypeValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVmInstancetypes(w, r)
	})
	http.HandleFunc(components.VMClusterInstancetypeValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVmClusterInstancetypes(w, r)
	})
	http.HandleFunc(components.VMPreferenceValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVmPreferences(w, r)
	})
	http.HandleFunc(components.VMClusterPreferenceValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVmClusterPreferences(w, r)
	})
	http.HandleFunc(components.StatusValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeStatusValidation(w, r, app.clusterConfig, app.virtCli, informers, app.kubeVirtServiceAccounts)
	})
	http.HandleFunc(components.PodEvictionValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServePodEvictionInterceptor(w, r, app.clusterConfig, app.virtCli)
	})
	http.HandleFunc(components.MigrationPolicyCreateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeMigrationPolicies(w, r)
	})
	http.HandleFunc(components.VMCloneCreateValidatePath, func(w http.ResponseWriter, r *http.Request) {
		validating_webhook.ServeVirtualMachineClones(w, r, app.clusterConfig, app.virtCli)
	})
}

// LEGACY(virt-api-migration): registered on http.DefaultServeMux and served through
// the bridge. Migrate to apiserver.MuxHandler and remove
func (app *virtAPIApp) registerMutatingWebhook(informers *webhooks.Informers) {

	http.HandleFunc(components.VMMutatePath, func(w http.ResponseWriter, r *http.Request) {
		mutating_webhook.ServeVMs(w, r, app.clusterConfig, app.virtCli)
	})
	http.HandleFunc(components.VMIMutatePath, func(w http.ResponseWriter, r *http.Request) {
		mutating_webhook.ServeVMIs(w, r, app.clusterConfig, informers, app.kubeVirtServiceAccounts)
	})
	http.HandleFunc(components.MigrationMutatePath, func(w http.ResponseWriter, r *http.Request) {
		mutating_webhook.ServeMigrationCreate(w, r)
	})
	http.HandleFunc(components.VMCloneCreateMutatePath, func(w http.ResponseWriter, r *http.Request) {
		mutating_webhook.ServeClones(w, r)
	})
	http.HandleFunc(components.VirtLauncherPodMutatePath, func(w http.ResponseWriter, r *http.Request) {
		mutating_webhook.ServeVirtLauncherPods(w, r, app.clusterConfig, app.virtCli)
	})
}

func (app *virtAPIApp) Run() {
	host, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("unable to get hostname: %v", err))
	}
	app.host = host

	// Get/Set selfsigned cert
	app.prepareCertManager()

	// Run informers for webhooks usage
	kubeInformerFactory := controller.NewKubeInformerFactory(app.virtCli.RestClient(), app.virtCli, app.aggregatorClient, app.namespace)

	kubeVirtInformer := kubeInformerFactory.KubeVirt()

	kubeInformerFactory.KubeVirtCAConfigMap()
	crdInformer := kubeInformerFactory.CRD()
	vmiPresetInformer := kubeInformerFactory.VirtualMachinePreset()
	vmRestoreInformer := kubeInformerFactory.VirtualMachineRestore()
	vmBackupInformer := kubeInformerFactory.VirtualMachineBackup()
	namespaceInformer := kubeInformerFactory.Namespace()

	stopChan := make(chan struct{}, 1)
	defer close(stopChan)
	kubeInformerFactory.Start(stopChan)
	kubeInformerFactory.WaitForCacheSync(stopChan)

	app.clusterConfig, err = virtconfig.NewClusterConfig(crdInformer, kubeVirtInformer, app.namespace)
	if err != nil {
		panic(err)
	}
	app.hasCDIDataSource = app.clusterConfig.HasDataSourceAPI()
	app.clusterConfig.SetConfigModifiedCallback(app.configModificationCallback)
	app.clusterConfig.SetConfigModifiedCallback(app.shouldChangeLogVerbosity)
	app.clusterConfig.SetConfigModifiedCallback(app.shouldChangeRateLimiter)

	var dataSourceInformer cache.SharedIndexInformer
	if app.hasCDIDataSource {
		dataSourceInformer = kubeInformerFactory.DataSource()
		log.Log.Infof("CDI detected, DataSource integration enabled")
	} else {
		// Add a dummy DataSource informer in the event datasource support
		// is disabled. This lets the controller continue to work without
		// requiring a separate branching code path.
		dataSourceInformer = kubeInformerFactory.DummyDataSource()
		log.Log.Infof("CDI not detected, DataSource integration disabled")
	}

	// It is safe to call kubeInformerFactory.Start multiple times.
	// The function is idempotent and will only start the informers that
	// have not been started yet
	kubeInformerFactory.Start(stopChan)
	kubeInformerFactory.WaitForCacheSync(stopChan)

	webhookInformers := &webhooks.Informers{
		VMIPresetInformer:  vmiPresetInformer,
		VMRestoreInformer:  vmRestoreInformer,
		VMBackupInformer:   vmBackupInformer,
		DataSourceInformer: dataSourceInformer,
		NamespaceInformer:  namespaceInformer,
	}

	go app.handlerCertManager.Start()

	app.setupHandlerTLS(kubeInformerFactory)
	app.registerLegacyWebhookMux(webhookInformers)
	metrics.SetVirtAPIReady()

	ctx := app.signalAwareContext()
	if err := app.startAggregatedAPIServer(ctx, webhookInformers); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

// This is where the dial virt-handler is done for console, vnc, etc
func (app *virtAPIApp) setupHandlerTLS(informerFactory controller.KubeInformerFactory) {
	kubevirtCAConfigInformer := informerFactory.KubeVirtCAConfigMap()
	kubevirtCAManager := kvtls.NewCAManager(kubevirtCAConfigInformer.GetStore(), app.namespace, app.caConfigMapName)
	app.handlerTLSConfiguration = kvtls.SetupTLSForVirtHandlerClients(kubevirtCAManager, app.handlerCertManager, app.externallyManaged)
}

// LEGACY(virt-api-migration): Remove this once all of these handlers are registered directly on the GenericAPIServer
// by WithMuxHandlers.
func (app *virtAPIApp) registerLegacyWebhookMux(webhookInformers *webhooks.Informers) {
	app.registerMutatingWebhook(webhookInformers)
	app.registerValidatingWebhooks(webhookInformers)
	http.Handle("/metrics", promhttp.Handler())
}

func (app *virtAPIApp) signalAwareContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	go func() {
		select {
		case s := <-c:
			log.Log.Infof("Received signal %s, initiating graceful shutdown", s.String())
		case msg := <-app.reInitChan:
			log.Log.Infof("Received signal to reInitialize virt-api [%s], initiating graceful shutdown", msg)
		}
		metrics.SetVirtAPINotReady()
		cancel()
	}()

	return ctx
}

func (app *virtAPIApp) startAggregatedAPIServer(ctx context.Context, webhookInformers *webhooks.Informers) error {
	s := app.apiServer.
		// LEGACY(virt-api-migration): this is the legacy bridge, I'll remove in final cleanup commit
		WithFallbackHandler(http.DefaultServeMux).
		WithBridgePaths(legacyBridgePaths()...).
		WithLongRunningSubresources("console", "vnc", "usbredir", "vsock", "portforward").
		// expand-vm-spec is a PUT to the collection path without a name which
		// cannot be expressed as a rest.Storage. So serve it as a plain mux handler
		// (like the webhooks) for every subresource version
		WithAPIHandlers(apiserver.ConditionalAPIHandler{
			Matches: func(info *request.RequestInfo) bool {
				return info.IsResourceRequest &&
					info.APIGroup == v1.SubresourceGroupName &&
					info.Resource == "expand-vm-spec"
			},
			Handler: expand.NewHandler(app.clusterConfig, app.virtCli),
		}).
		WithMuxHandlers(apiserver.MuxHandler{
			Path: components.VMValidatePath,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				validating_webhook.ServeVMs(w, r, app.clusterConfig, app.virtCli, webhookInformers, app.kubeVirtServiceAccounts)
			}),
		})

	scheme := apiserver.NewScheme()

	// Register each version gets its own storage instances.
	apiGroups := apiserver.APIGroups{}
	for _, gv := range v1.SubresourceGroupVersions {
		storage := virtualmachine.NewStorageMap(app.virtCli, app.clusterConfig)
		for resource, store := range virtualmachineinstance.NewStorageMap(app.virtCli, app.consoleServerPort, app.handlerTLSConfiguration, app.clusterConfig) {
			storage[resource] = store
		}
		apiGroups[gv] = storage
	}

	log.Log.Info("starting aggregated API server (GenericAPIServer) on port %d as the single virt-api listener")

	return s.Run(
		ctx,
		"virt-api-aggregated",
		scheme,
		apiserver.NewOpenAPIConfig(scheme),
		apiserver.NewOpenAPIV3Config(scheme),
		apiGroups,
	)
}

// LEGACY(virt-api-migration): paths still served from http.DefaultServeMux through
// the bridge. delete with the bridge
func legacyBridgePaths() []string {
	return []string{
		"/metrics",

		// Mutating webhooks.
		components.VMMutatePath,
		components.VMIMutatePath,
		components.MigrationMutatePath,
		components.VMCloneCreateMutatePath,
		components.VirtLauncherPodMutatePath,

		// Validating webhooks.
		components.VMICreateValidatePath,
		components.VMIUpdateValidatePath,
		components.VMIRSValidatePath,
		components.VMPoolValidatePath,
		components.VMIPresetValidatePath,
		components.MigrationCreateValidatePath,
		components.MigrationUpdateValidatePath,
		components.VMSnapshotValidatePath,
		components.VMRestoreValidatePath,
		components.VMBackupValidatePath,
		components.VMBackupTrackerValidatePath,
		components.VMExportValidatePath,
		components.VMInstancetypeValidatePath,
		components.VMClusterInstancetypeValidatePath,
		components.VMPreferenceValidatePath,
		components.VMClusterPreferenceValidatePath,
		components.StatusValidatePath,
		components.PodEvictionValidatePath,
		components.MigrationPolicyCreateValidatePath,
		components.VMCloneCreateValidatePath,

		components.KubeVirtUpdateValidatePath,
		components.KubeVirtCreateValidatePath,
	}
}

// Detects if a config has been applied that requires
// re-initializing virt-api.
func (app *virtAPIApp) configModificationCallback() {
	newHasCDI := app.clusterConfig.HasDataSourceAPI()
	if newHasCDI != app.hasCDIDataSource {
		if newHasCDI {
			log.Log.Infof("Reinitialize virt-api, cdi DataSource api has been introduced")
		} else {
			log.Log.Infof("Reinitialize virt-api, cdi DataSource api has been removed")
		}
		app.reInitChan <- "reinit due to CDI api change"
	}
}

// Update virt-api log verbosity on relevant config changes
func (app *virtAPIApp) shouldChangeLogVerbosity() {
	verbosity := app.clusterConfig.GetVirtAPIVerbosity(app.host)
	log.Log.SetVerbosityLevel(int(verbosity))
	log.Log.V(2).Infof("set log verbosity to %d", verbosity)
}

// Update virt-handler rate limiter
func (app *virtAPIApp) shouldChangeRateLimiter() {
	config := app.clusterConfig.GetConfig()
	qps := config.APIConfiguration.RestClient.RateLimiter.TokenBucketRateLimiter.QPS
	burst := config.APIConfiguration.RestClient.RateLimiter.TokenBucketRateLimiter.Burst
	app.reloadableRateLimiter.Set(flowcontrol.NewTokenBucketRateLimiter(qps, burst))
	log.Log.V(2).Infof("setting rate limiter for the API to %v QPS and %v Burst", qps, burst)
	qps = config.WebhookConfiguration.RestClient.RateLimiter.TokenBucketRateLimiter.QPS
	burst = config.WebhookConfiguration.RestClient.RateLimiter.TokenBucketRateLimiter.Burst
	app.reloadableWebhookRateLimiter.Set(flowcontrol.NewTokenBucketRateLimiter(qps, burst))
	log.Log.V(2).Infof("setting rate limiter for webhooks to %v QPS and %v Burst", qps, burst)
}

func (app *virtAPIApp) AddFlags() {
	flag.CommandLine.AddGoFlag(goflag.CommandLine.Lookup("v"))
	flag.CommandLine.AddGoFlag(goflag.CommandLine.Lookup("kubeconfig"))
	flag.CommandLine.AddGoFlag(goflag.CommandLine.Lookup("master"))

	app.apiServer.AddFlags(flag.CommandLine)

	flag.BoolVar(&app.SubresourcesOnly, "subresources-only", false,
		"Only serve subresource endpoints")
	flag.IntVar(&app.consoleServerPort, "console-server-port", DefaultConsoleServerPort,
		"The port virt-handler listens on for console requests")
	flag.StringVar(&app.caConfigMapName, "ca-configmap-name", defaultCAConfigMapName,
		"The name of configmap containing CA certificates to authenticate requests presenting client certificates with matching CommonName")
	flag.StringVar(&app.handlerCertFilePath, "handler-cert-file", defaultHandlerCertFilePath,
		"Client certificate used to prove the identity of the virt-api when it must call virt-handler during a request")
	flag.StringVar(&app.handlerKeyFilePath, "handler-key-file", defaultHandlerKeyFilePath,
		"Private key for the client certificate used to prove the identity of the virt-api when it must call virt-handler during a request")
	flag.BoolVar(&app.externallyManaged, "externally-managed", false,
		"Allow intermediate certificates to be used in building up the chain of trust when certificates are externally managed")
}
