package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v8"
	kubeflow "github.com/StatCan/kubeflow-apis/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	cleanerUtils "zone-volume-cleaner/utils"
)

type cleanerJobReport struct {
	notebooksDeleted int
	volumesDeleted   int
	emailsSent       int
	errorsReported   int
}

type cleanerParams struct {
	azureResourceGroup      string
	azureSubscriptionID     string
	dryRun                  bool
	gracePeriod             int
	notificationTimes       []int
	notificationTimesString string
	testEmail               string
	testNamespace           string
}

type CleanerConfigs struct {
	params               cleanerParams
	emailConfigs         cleanerUtils.EmailConfigs
	clientset            *kubernetes.Clientset
	kubeflowClient       *kubeflow.Clientset
	disksClient          *armcompute.DisksClient
	httpClient           *http.Client
	disableNotifications bool
	report               *cleanerJobReport
}

const (
	NOTIFICATION_COUNT_LABEL = "zone-volume-cleaner/notification-count"
	NS_OWNER_ANNOTATION      = "owner"
)

func (cfg *CleanerConfigs) cleanVolumes() {
	// check if the test namespace was set
	if cfg.params.testNamespace != "" {
		slog.Info("Running only on testing namespace", "NAMESPACE", cfg.params.testNamespace)

		err := cfg.cleanNamespaceVolumes(cfg.params.testNamespace, cfg.params.testEmail)
		// Job report error count will be incremented inside the function
		if err != nil {
			slog.Error("Error occured while cleaning volumes for namespace", "NAMESPACE", cfg.params.testNamespace, "ERROR", err)
		}
		return
	}

	// TODO: Set a solution for users on extended leave, probably a label on namespaces/profiles
	profileNamespaces, err := cfg.clientset.CoreV1().Namespaces().List(context.TODO(), v1.ListOptions{
		LabelSelector: "app.kubernetes.io/part-of=kubeflow-profile",
	})
	if err != nil {
		cfg.report.errorsReported++
		slog.Error("Error while getting namespaces", "ERROR", err)
		return
	}

	for _, ns := range profileNamespaces.Items {
		ownerEmail := ns.Annotations[NS_OWNER_ANNOTATION]

		err := cfg.cleanNamespaceVolumes(ns.Name, ownerEmail)
		// Job report error count will be incremented inside the function
		if err != nil {
			slog.Error("Error occured while cleaning volumes for namespace", "NAMESPACE", cfg.params.testNamespace, "ERROR", err)
			// If an error occurs, move to the next namespace
			continue
		}
	}
}

func (cfg *CleanerConfigs) cleanNamespaceVolumes(ns string, ownerEmail string) error {
	logger := slog.Default().With("NAMESPACE", ns)

	// get PVCs for profile namespace
	pvcs, err := cfg.clientset.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		cfg.report.errorsReported++
		logger.Error("Error while getting PVCs", "ERROR", err)
		return err
	}

	// format the list of pvcs into a map for easy handling
	pvcsMap := make(map[string]corev1.PersistentVolumeClaim)
	for _, pvc := range pvcs.Items {
		pvcsMap[pvc.Name] = pvc
	}

	// Get the formatted data for the notebooks and their associated PVCs, as well as the list of all attached PVCs
	notebookPvcMap, attachedPvcMap, err := getNotebooksPvcsInfo(cfg.kubeflowClient, ns, pvcsMap)
	if err != nil {
		cfg.report.errorsReported++
		logger.Error("Error while setting up notebooks PVCs information", "ERROR", err)
		return err
	}

	attachedPvcList := slices.Collect(maps.Keys(attachedPvcMap))

	unattachedPvcs := make([]corev1.PersistentVolumeClaim, 0)
	// determine which pvcs are unattached
	for _, pvc := range pvcs.Items {
		if !slices.Contains(attachedPvcList, pvc.Name) {
			unattachedPvcs = append(unattachedPvcs, pvc)
		}
	}

	// Clean volumes not attached to notebooks
	err = cfg.cleanUnattachedNamespaceVolumes(unattachedPvcs, ownerEmail, logger)
	// Job report error count will be incremented inside the function
	if err != nil {
		logger.Error("Error while cleaning unattached PVCs", "ERROR", err)
		return err
	}

	// Clean volumes that are attached to notebooks
	err = cfg.cleanAttachedNamespaceVolumes(ns, notebookPvcMap, ownerEmail, logger)
	// Job report error count will be incremented inside the function
	if err != nil {
		logger.Error("Error while cleaning attached PVCs", "ERROR", err)
		return err
	}

	return nil
}

// Deletes stale volumes that are expected to not be attached to any Kubeflow Notebook resource.
func (cfg *CleanerConfigs) cleanUnattachedNamespaceVolumes(pvcs []corev1.PersistentVolumeClaim, ownerEmail string, logger *slog.Logger) error {
	logger.Info("Evaluating unattached PVCs")

	// If an error occurs in this loop, just log it and continue to process the next pvc
	for _, pvc := range pvcs {
		pvcLogger := logger.With("PVC", pvc.Name)

		timeUntilStale, err := cfg.getTimeUntilStale(pvc, cfg.params.gracePeriod)
		if err != nil {
			cfg.report.errorsReported++
			pvcLogger.Error("Error while getting time until stale for PVC", "PVC_STATUS", pvc.Status.Phase, "ERROR", err)
			continue
		}

		isStale := timeUntilStale < 0
		if isStale {
			// if PVC is stale, proceed to delete it
			pvcLogger.Info("PVC is stale", "TIME_UNTIL_STALE", timeUntilStale)

			err := cfg.deletePvc(pvc, pvcLogger)
			if err != nil {
				cfg.report.errorsReported++
				pvcLogger.Error("Error while deleting unattached PVC", "ERROR", err)
				continue
			}

			if !cfg.disableNotifications {
				//TODO: also notify user when their pvc does get deleted?
				err = cfg.sendDeleteEmail()
				if err != nil {
					cfg.report.errorsReported++
					pvcLogger.Error("Error while sending email to user about recently deleted volumes", "ERROR", err)
					continue
				}
			}
		} else {
			pvcLogger.Info("PVC is not stale.", "TIME_UNTIL_STALE", timeUntilStale)
			if !cfg.disableNotifications {
				// if the PVC is not yet stale, determine if users needs to be notified of impending stale disks.
				if ownerEmail == "" {
					pvcLogger.Warn("Namespace owner email could not be found. Skipping email notifications")
					continue
				}

				err := cfg.notifyUserStaleVolume(ownerEmail, timeUntilStale, pvc, pvcLogger)
				if err != nil {
					cfg.report.errorsReported++
					pvcLogger.Error("Error while sending email to user about upcoming stale volumes", "ERROR", err)
					continue
				}
			}
		}
	}

	return nil
}

func (cfg *CleanerConfigs) cleanAttachedNamespaceVolumes(ns string, notebookPvcMap map[string][]corev1.PersistentVolumeClaim, ownerEmail string, logger *slog.Logger) error {
	logger.Info("Evaluating attached PVCs")

	for notebook, vols := range notebookPvcMap {
		notebookLogger := logger.With("NOTEBOOK", notebook)

		staleVolumesCount := 0
		// map to keep track of which volumes are stale. Used for email notfications
		// key is the pvc name; value is the time until stale
		nonStaleVolumesMap := make(map[string]float64)

		for _, pvc := range vols {
			notebookPvcLogger := notebookLogger.With("PVC", pvc.Name)

			timeUntilStale, err := cfg.getTimeUntilStale(pvc, cfg.params.gracePeriod)
			if err != nil {
				cfg.report.errorsReported++
				notebookPvcLogger.Error("Error while getting time until stale for PVC", "PVC_STATUS", pvc.Status.Phase, "ERROR", err)
				continue
			}

			isStale := timeUntilStale < 0
			if isStale {
				notebookPvcLogger.Info("PVC is stale", "TIME_UNTIL_STALE", timeUntilStale)
				staleVolumesCount++
			} else {
				notebookPvcLogger.Info("PVC is not stale.", "TIME_UNTIL_STALE", timeUntilStale)
				nonStaleVolumesMap[pvc.Name] = timeUntilStale
			}
		}

		// only proceed to cleaning the notebook (and its volumes) if all the volumes are stale
		if staleVolumesCount == len(vols) {
			notebookLogger.Info("Notebook and its PVCs are stale")
			err := cfg.deleteNotebookAndPvcs(ns, notebook, vols, notebookLogger)
			if err != nil {
				cfg.report.errorsReported++
				notebookLogger.Error("Error while trying to delete notebook and its PVCs.", "ERROR", err)
				continue
			}

			if !cfg.disableNotifications {
				//TODO: also notify user when their notebook and pvcs does get deleted?
				for _, pvc := range vols {
					err = cfg.sendDeleteEmail()
					if err != nil {
						cfg.report.errorsReported++
						notebookLogger.Error("Error while sending email to user about recently deleted volume", "PVC", pvc.Name, "ERROR", err)
						continue
					}
				}
			}

			continue
		} else {
			// if the notebook and PVCs are not yet stale, determine if users needs to be notified of impending stale disks.
			notebookLogger.Info("Notebook and its PVCS are not fully stale")

			if !cfg.disableNotifications {
				// TODO: Make logic to notify a user when they have a notebook with one or more volumes that is stale
				// Could just loop over the notifyPvcOwner Fn for each pvc, but we might be able to just run this once for the whole notebook & its volumes
				// We would just have to most likely tweak the email format.

				if ownerEmail == "" {
					logger.Warn("Namespace owner email could not be found. Skipping email notifications")
					continue
				}
				for _, pvc := range vols {
					err := cfg.notifyUserStaleVolume(ownerEmail, nonStaleVolumesMap[pvc.Name], pvc, notebookLogger)
					if err != nil {
						cfg.report.errorsReported++
						notebookLogger.Error("Error while trying to notify user", "ERROR", err)
						continue
					}
				}
			}
		}
	}

	return nil
}

// Function to format the data of a namespace for the list of Kubeflow Notebooks and their attached volumes for easier manipulation
// Returns:
//   - Map of notebooks and their associated PVCs
//   - Map of PVCs attached to notebooks. Using a map object to make it a unique list. The bool value is irrelevant.
//   - Error
func getNotebooksPvcsInfo(kubeflowClient *kubeflow.Clientset, ns string, pvcsMap map[string]corev1.PersistentVolumeClaim) (map[string][]corev1.PersistentVolumeClaim, map[string]bool, error) {
	notebooks, err := kubeflowClient.KubeflowV1().Notebooks(ns).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		return nil, nil, err
	}

	notebooksPvcMap := make(map[string][]corev1.PersistentVolumeClaim)
	attachedPVCs := make(map[string]bool)

	for _, nb := range notebooks.Items {
		vols := make([]corev1.PersistentVolumeClaim, 0)
		for _, vol := range nb.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				vols = append(vols, pvcsMap[vol.PersistentVolumeClaim.ClaimName])
				attachedPVCs[vol.PersistentVolumeClaim.ClaimName] = false
			}
		}

		// we only care about notebooks that have volumes
		if len(vols) > 0 {
			notebooksPvcMap[nb.Name] = vols
		}
	}

	return notebooksPvcMap, attachedPVCs, nil
}

// Query Azure to get the last Ownership time for the given volume handle.
// The returned value represents the last time the volume was attached to a running pod.
// TODO: This is currently doing one call per PV. Maybe it should be changed to get all the disks in 1 call to azure, and then we filter through that list?
// TODO: This would limit the calls to azure from n calls to 1 call. Might help with networking speed. But the payload would be larger and all stored in memory.
func (cfg *CleanerConfigs) getPvcLastOwnershipTimeFromAzure(volumeHandle string) (*time.Time, error) {
	// Get the Azure Disk name from the PV resource
	diskHandleSplit := strings.Split(volumeHandle, "/")
	diskName := diskHandleSplit[len(diskHandleSplit)-1]

	// Get the disk information from Azure
	diskResponse, err := cfg.disksClient.Get(context.TODO(), cfg.params.azureResourceGroup, diskName, nil)
	if err != nil {
		return nil, fmt.Errorf("Error while getting Azure Disk %s: %s", diskName, err.Error())
	}

	lastOwnershipTime := diskResponse.Properties.LastOwnershipUpdateTime
	createdTime := diskResponse.Properties.TimeCreated

	if lastOwnershipTime == nil && createdTime == nil {
		return nil, fmt.Errorf("Cannot determine last time Disk %s was used. Last Ownership Update time and Time Created are both empty", diskName)
	} else if lastOwnershipTime == nil {
		// If the last ownership update time is null, return the Time Create instead
		return createdTime, nil
	}

	return lastOwnershipTime, nil
}

// Gets the time of when a PVC was last used
func (cfg *CleanerConfigs) getPvcLastOwnershipTime(pvc corev1.PersistentVolumeClaim) (*time.Time, error) {
	pv, err := cfg.clientset.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, v1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("Error while getting Persistent Volume: %s", err.Error())
	}

	lastOwnershipTime, err := cfg.getPvcLastOwnershipTimeFromAzure(pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return nil, fmt.Errorf("Error while getting Last Ownership Time from Azure: %s", err.Error())
	}

	return lastOwnershipTime, nil
}

// Returns the time until a PVC is stale
// A negative value means that the PVC is stale
func (cfg *CleanerConfigs) getTimeUntilStale(pvc corev1.PersistentVolumeClaim, gracePeriod int) (float64, error) {
	lastOwnershipTime, err := cfg.getPvcLastOwnershipTime(pvc)
	if err != nil {
		return 0, fmt.Errorf("Error while getting last ownership time for PVC: %s", err.Error())
	}

	// difference in days
	diff := time.Since(*lastOwnershipTime).Hours() / 24

	return float64(gracePeriod) - diff, nil
}

func (cfg *CleanerConfigs) deletePvc(pvc corev1.PersistentVolumeClaim, logger *slog.Logger) error {
	if cfg.params.dryRun {
		cfg.report.volumesDeleted++
		logger.Info("[DRY RUN] Deleted PVC")
		return nil
	}

	err := cfg.clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(context.TODO(), pvc.Name, v1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("Error while deleting PVC %s: %s", pvc.Name, err.Error())
	}

	cfg.report.volumesDeleted++
	logger.Info("Deleted PVC")
	return nil
}

func (cfg *CleanerConfigs) deleteNotebookAndPvcs(namespace string, notebook string, pvcs []corev1.PersistentVolumeClaim, logger *slog.Logger) error {
	if cfg.params.dryRun {
		cfg.report.notebooksDeleted++
		logger.Info("[DRY RUN] Deleted Notebook")

		for _, pvc := range pvcs {
			cfg.report.volumesDeleted++
			logger.Info("[DRY RUN] Deleted attached PVC", "PVC", pvc.Name)
		}

		return nil
	}

	err := cfg.kubeflowClient.KubeflowV1().Notebooks(namespace).Delete(context.TODO(), notebook, v1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("Error while deleting Notebook: %s", err.Error())
	}

	cfg.report.notebooksDeleted++
	logger.Info("Deleted Notebook")

	for _, pvc := range pvcs {
		err := cfg.deletePvc(pvc, logger)
		if err != nil {
			return fmt.Errorf("Error while deleting attached PVC %s: %s", pvc.Name, err.Error())
		}

		logger.Info("Deleted PVC", "PVC", pvc.Name)
	}

	return nil
}

func (cfg *CleanerConfigs) getNotificationCountLabelValue(timeUntilStale float64, pvc corev1.PersistentVolumeClaim, logger *slog.Logger) (int, error) {
	notifCountString, ok := pvc.Labels[NOTIFICATION_COUNT_LABEL]
	if !ok {
		// if notif count label is not present, we can assume that this is the first time the pvc is evaluated
		logger.Warn("Notification Count label not found. Defaulting to nearest value (without going over) in notification times parameter")

		notifCount := 0
		// Loops through the defined notification times to find the nearest value without going over.
		for i := 0; i < len(cfg.params.notificationTimes); i++ {
			if timeUntilStale > float64(cfg.params.notificationTimes[i]) {
				// In theory this should not break on the first run of the loop since
				// we already check for that condition prior to calling this function
				break
			}

			notifCount = i
		}

		return notifCount, nil
	}

	notifCount, err := strconv.Atoi(notifCountString)
	if err != nil {
		return 0, fmt.Errorf("Failed to parse notification count label: %s", err.Error())
	}

	return notifCount, nil
}

// checks email times and determines if this pvc's owner should be emailed
func (cfg *CleanerConfigs) shouldSendStaleEmail(timeUntilStale float64, notifCount int, logger *slog.Logger) bool {
	if notifCount < len(cfg.params.notificationTimes) {
		currentNotifTime := float64(cfg.params.notificationTimes[notifCount])

		if currentNotifTime >= timeUntilStale {
			logger.Info("Email will be sent.", "CLOSEST_EMAIL_INTERVAL_DAYS", currentNotifTime)
			return true
		}

		logger.Info("Email will not be sent", "DAYS_UNTIL_NEXT_EMAIL", timeUntilStale-currentNotifTime)
	}

	return false
}

func (cfg *CleanerConfigs) notifyUserStaleVolume(ownerEmail string, timeUntilStale float64, pvc corev1.PersistentVolumeClaim, logger *slog.Logger) error {
	logger.Info("Notifying user of volume approching deletion")

	if len(cfg.params.notificationTimes) == 0 {
		logger.Warn("Notification Times parameter is empty, skipping notifying user")
		return nil
	}

	// If volume was used more recently than our largest notification time,
	// reset the notification count label and don't notify the user
	if timeUntilStale > float64(cfg.params.notificationTimes[0]) {
		if cfg.params.dryRun {
			logger.Info("[DRY RUN] Volume was used recently. Reset the notification count label and avoided sending notification", "TIME_UNTIL_STALE", timeUntilStale)
			return nil
		}

		err := cleanerUtils.RemovePvcLabel(cfg.clientset, NOTIFICATION_COUNT_LABEL, pvc.Namespace, pvc.Name)
		if err != nil {
			// logger.Error("Error while resetting notification count label", "ERROR", err)
			return fmt.Errorf("Error while resetting notification count label: %s", err.Error())
		}

		logger.Info("Volume was used recently. Reset the notification count label and avoided sending notification", "TIME_UNTIL_STALE", timeUntilStale)
		return nil
	}

	notifCount, err := cfg.getNotificationCountLabelValue(timeUntilStale, pvc, logger)
	if err != nil {
		return fmt.Errorf("Failed to get Notification Count label value: %s", err.Error())
	}

	// Exit from the function if it should not send an email
	if !cfg.shouldSendStaleEmail(timeUntilStale, notifCount, logger) {
		return nil
	}

	if cfg.params.dryRun {
		cfg.report.emailsSent++
		logger.Info("[DRY RUN] Email notification sent", "EMAIL", ownerEmail, "TIME_UNTIL_STALE", timeUntilStale)
		return nil
	}

	personalisation := cleanerUtils.GetEmailPersonalisation(pvc.Namespace, pvc.Name, timeUntilStale)

	// TODO: Instead of sending 1 email per pvc, we should instead send one email per namespace with a report of what is getting stale.
	err = cleanerUtils.SendEmail(cfg.httpClient, cfg.emailConfigs, ownerEmail, personalisation)
	if err != nil {
		return fmt.Errorf("Error while sending email to %s with details %+v: %s", ownerEmail, personalisation, err.Error())
	}

	cfg.report.emailsSent++
	logger.Info("Email notification sent", "EMAIL", ownerEmail, "TIME_UNTIL_STALE", timeUntilStale)

	// Increment notification count by 1
	newNotifCount := strconv.Itoa(notifCount + 1)
	err = cleanerUtils.SetPvcLabel(cfg.clientset, NOTIFICATION_COUNT_LABEL, newNotifCount, pvc.Namespace, pvc.Name)
	if err != nil {
		return fmt.Errorf("Error while updating notification count label: %s", err.Error())
	}

	return nil
}

func (cfg *CleanerConfigs) sendDeleteEmail() error {
	if !cfg.shouldSendDeleteEmail() {
		return nil
	}

	return nil
}

func (cfg *CleanerConfigs) shouldSendDeleteEmail() bool {
	return false
}
