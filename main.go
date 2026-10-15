package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v8"
	kubeflow "github.com/StatCan/kubeflow-apis/clientset/versioned"
	"github.com/joho/godotenv"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	cleanerUtils "zone-volume-cleaner/utils"
)

func main() {
	slog.Info("Zone Volume Cleaner started")

	// Read a local .env file if present
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Setup configs
	var params CleanerParams
	flag.StringVar(&params.AzureResourceGroup, "AZURE_RESOURCE_GROUP", os.Getenv("AZURE_RESOURCE_GROUP"), "Azure resoure group for the disks")
	flag.StringVar(&params.AzureSubscriptionID, "AZURE_SUBSCRIPTION_ID", os.Getenv("AZURE_SUBSCRIPTION_ID"), "Azure subscription ID for the disks")
	flag.BoolVar(&params.DryRun, "DRY_RUN", cleanerUtils.GetEnv("DRY_RUN", "true") == "true", "Dry run without harmful operations")
	flag.IntVar(&params.GracePeriod, "GRACE_PERIOD", cleanerUtils.GetIntEnv("GRACE_PERIOD", 365), "Grace period for Disks (in days)")
	flag.StringVar(&params.NotificationTimesString, "NOTIF_TIMES", os.Getenv("NOTIF_TIMES"), "comma-seperated list of times to send notifications before volume deletion(in days)")
	flag.StringVar(&params.TestEmail, "TEST_EMAIL", os.Getenv("TEST_EMAIL"), "Email to use for testing notifications (to be used with TEST_NAMESPACE)")
	flag.StringVar(&params.TestNamespace, "TEST_NAMESPACE", os.Getenv("TEST_NAMESPACE"), "Limit the cleaner to only look at a specific namespace for testing")

	// Get configs for the email notifications (GC Notify in our case)
	var emailConfigs cleanerUtils.EmailConfigs
	flag.StringVar(&emailConfigs.BaseURL, "EMAIL_BASE_URL", os.Getenv("EMAIL_BASE_URL"), "Base URL for the email notification service")
	flag.StringVar(&emailConfigs.Endpoint, "EMAIL_ENDPOINT", os.Getenv("EMAIL_ENDPOINT"), "Endpoint for the email notification service")
	flag.StringVar(&emailConfigs.EmailTemplateID, "EMAIL_TEMPLATE_ID", os.Getenv("EMAIL_TEMPLATE_ID"), "ID for the email template in the email notification service")
	flag.StringVar(&emailConfigs.APIKey, "EMAIL_API_KEY", os.Getenv("EMAIL_API_KEY"), "Api key for the email notification service")

	// Define kube config to read local context if present
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	// Parse flags
	flag.Parse()

	// Validate parameters
	if params.GracePeriod < 1 {
		log.Fatal("Error while validating params. For safety reasons, grace period cannot be lower than one day.")
	}

	params.NotificationTimes = cleanerUtils.ParseNotificationTimes(params.NotificationTimesString)

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatalf("Error while building kube config: %s", err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error while building kube clientset: %s", err.Error())
	}

	// // creates the in-cluster config
	// config, err := rest.InClusterConfig()
	// if err != nil {
	// 	panic(err.Error())
	// }
	// // creates the clientset
	// clientset, err := kubernetes.NewForConfig(config)
	// if err != nil {
	// 	panic(err.Error())
	// }

	// Authenticating with Azure using the default environment variables
	// https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/azidentity#section-readme
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("Error while building Azure Credential: %s", err.Error())
	}

	disksClient, err := armcompute.NewDisksClient(params.AzureSubscriptionID, cred, nil)
	if err != nil {
		log.Fatalf("Error while building Azure Disk Client: %s", err.Error())
	}

	// Setup Kubeflow lister
	kubeflowClientset, err := kubeflow.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error while building kubeflow clientset: %s", err.Error())
	}

	// Disable email notifications if the configs are not properly all set
	isEmailDisabled := emailConfigs.BaseURL == "" ||
		emailConfigs.Endpoint == "" ||
		emailConfigs.EmailTemplateID == "" ||
		emailConfigs.APIKey == ""

	if isEmailDisabled {
		slog.Warn("Email notification configs are not fully setup. Skipping emailing volume owners.")
	}

	cfg := CleanerConfigs{
		params:               params,
		emailConfigs:         emailConfigs,
		clientset:            clientset,
		kubeflowClient:       kubeflowClientset,
		disksClient:          disksClient,
		disableNotifications: isEmailDisabled,
	}

	cfg.cleanVolumes()

	slog.Info("Zone Volume Cleaner ended")
}
