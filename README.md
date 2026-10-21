# zone-volume-cleaner
(This project was built from the [Statcan Volume Cleaner](https://github.com/StatCan/volume-cleaner))

A Golang script for the Zone to automatically identify and clean up stale Persistent Volume Claims(PVCs) in Kubernetes(K8s), which will cascade into deleting their bound Persistent Volumes(PVs) that are backed by Azure Disks for persistent storage.

When PVCs are associated with a Notebook resource, the script will only delete the notebook and its volumes if they are all stale. 

## Getting Started

This script is intended to run on a K8s cluster with a Cronjob. But it can still run locally on your machine for development and testing purposes, using your current local K8s context.

### Prerequisites

- Go
- K8s cluster context

### Configurations

These configurations can either bet set in your environment variables, as flags in your `go run` command or by making a `.env` file in your project directory.

| Config | Description |
| --- | ---|
| AZURE_RESOURCE_GROUP | Azure resource group name containing the Disks backing the PVs on the K8s cluster |
| AZURE_SUBSCRIPTION_ID | Azure subscription ID containing the Disks backing the PVs on the K8s cluster |
| AZURE_TENANT_ID | Azure tenant ID containing the Disks backing the PVs on the K8s cluster |
| DRY_RUN | If set to `true`, this will prevent resources from being impacted by any operations done by this script. This will also prevent email notifications from being sent. Defaults to `true` |
| GRACE_PERIOD | Amount of days for a PV being unused before it is considered stale and deleted. For example, `90` would mean that if a PVC has been unused for 90 days, it will get deleted. |
| NOTIF_TIMES | Comma-seperated list of days before a PVC deletion to send email notifications. For example, `14,7,1` would make the script send emails 14 days before the PVC deletion, 7 days before PVC deletion and 1 day before PVC deletion. |
| TEST_EMAIL | Email address to be used for testing the email notifications |
| TEST_NAMESPACE | Namespace to be used for testing, instead of running the script through all namespaces |
| EMAIL_BASE_URL | Base URL for the emailing service |
| EMAIL_ENDPOINT | API Endpoint for the emailing service |
| AZURE_CLIENT_ID | Azure Application(client) ID used for authentication |
| AZURE_CLIENT_SECRET | Azure client secret used for authentication |
| EMAIL_TEMPLATE_ID | ID for the email template to use (in GCNotify) |
| EMAIL_API_KEY | API key to use for the GCNotify email service |

### Running locally 

Assuming you setup a `.env` file, to run locally:
```go
go run .
```

### Building the image

Build the image by running:
```bash
docker build .
```

### Running on the cluster

Deploy the manifests under the `./manifests` on the cluster, filling the values as needed.