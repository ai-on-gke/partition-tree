/*
Copyright 2026 Google LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	partitionv1beta1 "github.com/ai-on-gke/partition-tree/api/v1beta1"
	topocache "github.com/ai-on-gke/partition-tree/internal/cache"
	"github.com/ai-on-gke/partition-tree/internal/config"
	"github.com/ai-on-gke/partition-tree/internal/controller"
	"github.com/ai-on-gke/partition-tree/internal/gce"
	"github.com/ai-on-gke/partition-tree/internal/metadata"
	"github.com/ai-on-gke/partition-tree/internal/metrics"
	"github.com/ai-on-gke/partition-tree/internal/version"
	// +kubebuilder:scaffold:imports
)

const (
	gceProdEndpoint               = "https://www.googleapis.com/compute/beta/"
	gceStagingEndpoint            = "https://www.googleapis.com/compute/staging_beta/"
	defaultMetricCardinalityLimit = 100000
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(partitionv1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var configNamespace string
	var configName string
	var gceMock bool
	var gceMockFixtures string
	var useStagingEndpoint bool
	var gceEndpoint string
	var gceDisableAuth bool
	var otlpEndpoint string
	var enableMetrics, enableGoogleInternalMetrics, metricsDirectPush bool
	var enableTaints controller.TaintConfig
	var decoupledLifecycle bool
	var enableTPUAllocatableReadiness bool
	var metricsTombstoneTTL time.Duration
	var metricCardinalityLimit int
	var kubeAPIQPS float64
	var kubeAPIBurst int
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", 100, "QPS limit for the controller-manager Kubernetes API client.")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", 200, "Burst limit for the controller-manager Kubernetes API client.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&configNamespace, "config-namespace", "",
		"Namespace of the hot-reloadable config ConfigMap. Defaults to $POD_NAMESPACE, then \"default\".")
	flag.StringVar(&configName, "config-name", "partition-config",
		"Name of the hot-reloadable config ConfigMap.")
	flag.BoolVar(&gceMock, "gce-mock", false,
		"If set, use an in-memory GCE mock client and mock metadata instead of calling GCE. "+
			"Intended for e2e tests; never enable in production.")
	flag.BoolVar(&useStagingEndpoint, "use-staging-endpoint", false, "Use staging API endpoint.")
	flag.StringVar(&gceEndpoint, "gce-endpoint", "", "Override GCE API endpoint URL.")
	flag.BoolVar(&gceDisableAuth, "gce-disable-auth", false,
		"Disable authentication (ComputeTokenSource) for the GCE client. "+
			"Intended for local testing against simulated GCE; never enable in production.")
	flag.StringVar(&otlpEndpoint, "otlp-endpoint",
		"",
		"The OTLP endpoint to send metrics to. If empty, OpenTelemetry is disabled. "+
			"GKE deployments set this via config/manager/manager.yaml.")
	flag.BoolVar(&enableMetrics, "enable-metrics", true, "Enable OpenTelemetry/Prometheus metrics (customer-visible metrics)")
	flag.BoolVar(&metricsDirectPush, "metrics-direct-push", false,
		"If set, push customer metrics directly to Google Cloud Monitoring in-process "+
			"using the prometheus_target resource type. Cannot be used together with --otlp-endpoint.")
	flag.BoolVar(&enableGoogleInternalMetrics, "enable-google-internal-metrics", true, "Enable Google-internal metrics emitted with the k8s_entity resource type")
	flag.StringVar(&gceMockFixtures, "gce-mock-fixtures", "",
		"Path to a YAML fixture file consumed by the mock GCE client. "+
			"Only honored when --gce-mock is set. The file is re-read on every "+
			"call so e2e tests can swap fixtures via a mounted ConfigMap.")
	flag.Var(&enableTaints, "enable-taints", "If set, enable the taint feature. Comma-separated options: All, NodeTaints, NodesNotFound, PartitionState (default unset value: All).")
	flag.BoolVar(&decoupledLifecycle, "decoupled-lifecycle", false,
		"Enable decoupled PT CR lifecycle. When set, CRs are named after the "+
			"root partition ID and survive node-pool drain/re-provision cycles.")
	flag.BoolVar(&enableTPUAllocatableReadiness, "enable-tpu-allocatable-readiness", false,
		"If set, a leaf's NodeReady condition also requires every node in the leaf to have its "+
			"full TPU chip count allocatable (google.com/tpu); adds Reason TPUsNotAllocatable. "+
			"Read at startup only.")
	flag.DurationVar(&metricsTombstoneTTL, "metrics-tombstone-ttl", 30*time.Minute,
		"Duration before tombstoned metrics (0 values) are pruned from the cache.")
	flag.IntVar(&metricCardinalityLimit, "metric-cardinality-limit", defaultMetricCardinalityLimit,
		"Maximum number of distinct metric time series (cardinality limit) per metric instrument.")
	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if metricsDirectPush && otlpEndpoint != "" {
		setupLog.Error(nil, "--metrics-direct-push and --otlp-endpoint cannot both be specified; "+
			"choose either direct push to Cloud Monitoring or OTLP collector push")
		os.Exit(1)
	}
	if metricsDirectPush && !enableMetrics {
		setupLog.Error(nil, "--metrics-direct-push cannot be used with --enable-metrics=false")
		os.Exit(1)
	}

	if metricCardinalityLimit <= 0 {
		metricCardinalityLimit = defaultMetricCardinalityLimit
	}

	if configNamespace == "" {
		configNamespace = os.Getenv("POD_NAMESPACE")
	}
	if configNamespace == "" {
		// Refuse to silently fall through to "default": that almost
		// never matches the namespace the operator deployed the
		// ConfigMap into, and the resulting "config not found"
		// behavior is too easy to miss. The deployment manifest wires
		// POD_NAMESPACE via the downward API; local `make run` users
		// must pass --config-namespace explicitly.
		setupLog.Error(nil,
			"--config-namespace is empty and POD_NAMESPACE is unset; "+
				"set --config-namespace explicitly or run inside a pod with the downward API")
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	v := version.Get()
	setupLog.Info("Starting partition-tree controller",
		"version", v.GitVersion,
		"gitCommit", v.GitCommit,
		"buildDate", v.BuildDate,
		"goVersion", v.GoVersion,
		"compiler", v.Compiler,
		"platform", v.Platform,
	)

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	cacheOpts := controller.ManagerCacheOptions(configNamespace, configName, enableTPUAllocatableReadiness)

	restCfg := ctrl.GetConfigOrDie()
	if kubeAPIQPS > 0 {
		restCfg.QPS = float32(kubeAPIQPS)
	}
	if kubeAPIBurst > 0 {
		restCfg.Burst = kubeAPIBurst
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "446eed18.tpu.google.com",
		Cache:                  cacheOpts,
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Bootstrap: fetch the initial config with a cache-less client so
	// manager startup does not have to wait for the informer cache to
	// populate before the config store is usable.
	bootstrapClient, err := client.New(mgr.GetConfig(), client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		setupLog.Error(err, "Failed to construct bootstrap client")
		os.Exit(1)
	}
	initialCfg, err := config.LoadInitial(context.Background(), bootstrapClient, configNamespace, configName)
	if err != nil {
		setupLog.Error(err, "Failed to load initial config; aborting startup",
			"configmap", configNamespace+"/"+configName)
		os.Exit(1)
	}
	configStore := config.NewStore(initialCfg)
	setupLog.Info("initial config loaded",
		"polling", initialCfg.Polling.Interval.D().String(),
		"syncTimeout", initialCfg.SyncTimeout.D().String())

	var gceMeta metadata.GoogleMetadata
	if enableMetrics || enableGoogleInternalMetrics {
		meta, err := metadata.Get(context.Background(), gceMock)
		if err != nil {
			setupLog.Error(err, "Failed to load GCE metadata; aborting startup")
			os.Exit(1)
		}
		gceMeta = *meta
		setupLog.Info("GCE metadata loaded", "projectID", gceMeta.ProjectID, "clusterLocation", gceMeta.ClusterLocation, "clusterName", gceMeta.ClusterName)
	}

	setupLog.Info("Initializing OpenTelemetry",
		"tombstoneTTL", metricsTombstoneTTL,
		"cardinalityLimit", metricCardinalityLimit,
		"otlpEndpoint", otlpEndpoint,
		"metricsDirectPush", metricsDirectPush)
	otelShutdown, err := metrics.Init(
		context.Background(),
		otlpEndpoint,
		metricsDirectPush,
		gceMeta,
		metricsTombstoneTTL,
		metricCardinalityLimit,
		enableMetrics,
		enableGoogleInternalMetrics,
	)
	if err != nil {
		setupLog.Error(err, "Failed to initialize OpenTelemetry")
		os.Exit(1)
	}
	defer otelShutdown()

	topoCache := topocache.New()

	// Reload channel: one event per PartitionTree CR per successful
	// ConfigMap reload. Sized well above the target 1440+-CR fleet so
	// the loader can deliver the whole fan-out without back-pressure
	// on the main controller's work queue. The loader blocks on send
	// (see config.LoaderReconciler.Reconcile) so the buffer is a
	// tail-latency guard, not a drop gate.
	reloadCh := make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 4096)

	// PT update channel: the node informer pushes events here in
	// decoupled lifecycle mode when a MIG change or drain is detected.
	// Only used when --decoupled-lifecycle is set.
	var ptUpdateCh chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree]
	if decoupledLifecycle {
		ptUpdateCh = make(chan event.TypedGenericEvent[*partitionv1beta1.PartitionTree], 4096)
		setupLog.Info("decoupled lifecycle mode enabled")
	}
	if enableTPUAllocatableReadiness {
		setupLog.Info("TPU allocatable readiness enabled", "resource", controller.ResourceTPU)
	}

	if err := (&config.LoaderReconciler{
		Client:    mgr.GetClient(),
		Namespace: configNamespace,
		Name:      configName,
		Store:     configStore,
		ReloadCh:  reloadCh,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "ConfigLoader")
		os.Exit(1)
	}

	// gce.NewRealClient wraps compute/v0.beta with application-default
	// credentials. The --gce-mock flag swaps in the in-memory MockClient
	// for e2e — when --gce-mock-fixtures is also set, the mock re-reads
	// the YAML file on every call so tests can mutate fake GCE state by
	// editing a mounted ConfigMap.
	var gceClient gce.Client
	if gceMock {
		mock := gce.NewMockClient()
		if gceMockFixtures != "" {
			if err := mock.LoadFromFile(gceMockFixtures); err != nil {
				setupLog.Error(err, "Failed to load GCE mock fixtures",
					"fixtures", gceMockFixtures)
				os.Exit(1)
			}
			setupLog.Info("GCE mock client enabled", "fixtures", gceMockFixtures)
		} else {
			setupLog.Info("GCE mock client enabled with empty fixture set")
		}
		gceClient = mock
	} else {
		endpoint := gceProdEndpoint
		if useStagingEndpoint {
			endpoint = gceStagingEndpoint
		}
		if gceEndpoint != "" {
			endpoint = gceEndpoint
		}
		var clientOpts []option.ClientOption
		clientOpts = append(clientOpts, option.WithEndpoint(endpoint))
		if gceDisableAuth {
			setupLog.Info("Using unauthenticated GCE client (--gce-disable-auth)", "endpoint", endpoint)
			clientOpts = append(clientOpts, option.WithoutAuthentication())
		} else {
			setupLog.Info("Using ComputeTokenSource for GCE client")
			tokenSource := google.ComputeTokenSource("")
			clientOpts = append(clientOpts, option.WithTokenSource(tokenSource))
		}
		realClient, err := gce.NewRealClient(
			context.Background(),
			clientOpts...,
		)
		if err != nil {
			setupLog.Error(err, "Failed to build GCE client")
			os.Exit(1)
		}
		gceClient = gce.NewRateLimitedClient(realClient,
			initialCfg.RateLimit.QPS, initialCfg.RateLimit.Burst)
		setupLog.Info("GCE rate limiter enabled",
			"qps", initialCfg.RateLimit.QPS,
			"burst", initialCfg.RateLimit.Burst)
	}

	if err := (&controller.PartitionTreeReconciler{
		Client:                        mgr.GetClient(),
		Scheme:                        mgr.GetScheme(),
		GCE:                           gceClient,
		Cache:                         topoCache,
		ConfigStore:                   configStore,
		Clock:                         clock.RealClock{},
		ReloadCh:                      reloadCh,
		PTUpdateCh:                    ptUpdateCh,
		DecoupledLifecycle:            decoupledLifecycle,
		EnableTaints:                  enableTaints,
		EnableTPUAllocatableReadiness: enableTPUAllocatableReadiness,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "PartitionTree")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
