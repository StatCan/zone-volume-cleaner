package main

import (
	"context"
	"errors"
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
	disableNotifications bool
	report               *cleanerJobReport
}

const (
	NOTIFICATION_COUNT_LABEL = "zone-volume-cleaner/notification-count"
	NS_OWNER_ANNOTATION      = "owner"
)

func (cfg *CleanerConfigs) cleanVolumes() {
	// One http client is created for emailing users
	httpClient := &http.Client{Timeout: 10 * time.Second}

	// check if the test namespace was set
	if cfg.params.testNamespace != "" {
		slog.Info("Running only on testing namespace", "NAMESPACE", cfg.params.testNamespace)

		err := cfg.cleanNamespaceVolumes(cfg.params.testNamespace, cfg.params.testEmail, httpClient)
		if err != nil {
			slog.Error("Error occured while cleaning volumes for namespace", "NAMESPACE", cfg.params.testNamespace, "ERROR", err)
		}
		return
	}

	profileNamespaces, err := cfg.clientset.CoreV1().Namespaces().List(context.TODO(), v1.ListOptions{
		LabelSelector: "app.kubernetes.io/part-of=kubeflow-profile",
	})
	if err != nil {
		slog.Error("Error while getting namespaces", "ERROR", err)
	}

	for _, ns := range profileNamespaces.Items {
		ownerEmail := ns.Annotations[NS_OWNER_ANNOTATION]

		err := cfg.cleanNamespaceVolumes(ns.Name, ownerEmail, httpClient)
		if err != nil {
			// If an error occurs, move to the next namespace
			slog.Error("Error occured while cleaning volumes for namespace", "NAMESPACE", cfg.params.testNamespace, "ERROR", err)
			continue
		}
	}
}

func (cfg *CleanerConfigs) cleanNamespaceVolumes(ns string, ownerEmail string, httpClient *http.Client) error {
	logger := slog.Default().With("NAMESPACE", ns)

	// get PVCs for profile namespace
	pvcs, err := cfg.clientset.CoreV1().PersistentVolumeClaims(ns).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		logger.Error("Error while getting PVCs", "ERROR", err)
		return err
	}

	// format the list of pvcs into a map for easy handling
	pvcsMap := make(map[string]corev1.PersistentVolumeClaim)
	for _, pvc := range pvcs.Items {
		pvcsMap[pvc.Name] = pvc
	}

	// Get a map of notebooks and their volumes for easier manipulation
	// using a map object to get the list of attached PVC names to make it a unique list. The bool value is irrelevant
	attachedPvcMap := make(map[string]bool)
	notebookPvcMap, err := setupNotebooksPvcsInfo(cfg.kubeflowClient, ns, pvcsMap, attachedPvcMap, logger)
	if err != nil {
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
	err = cfg.cleanUnattachedNamespaceVolumes(unattachedPvcs, ownerEmail, httpClient, logger)
	if err != nil {
		logger.Error("Error while cleaning unattached PVCs", "ERROR", err)
		return err
	}

	// Clean volumes that are attached to notebooks
	err = cfg.cleanAttachedNamespaceVolumes(ns, notebookPvcMap, ownerEmail, httpClient, logger)
	if err != nil {
		logger.Error("Error while cleaning attached PVCs", "ERROR", err)
		return err
	}

	return nil
}

func (cfg *CleanerConfigs) cleanUnattachedNamespaceVolumes(pvcs []corev1.PersistentVolumeClaim, ownerEmail string, httpClient *http.Client, logger *slog.Logger) error {
	logger.Info("Evaluating unattached PVCs")

	for _, pvc := range pvcs {
		pvcLogger := logger.With("PVC", pvc.Name)

		pvcLogger.Info("Evaluating unattached PVC")

		timeUntilStale, err := cfg.getTimeUntilStale(pvc, cfg.params.gracePeriod, pvcLogger)
		if err != nil {
			pvcLogger.Error("Error while getting time until stale for PVC", "ERROR", err)
			continue
		}

		isStale := 0 > timeUntilStale
		if !isStale {
			pvcLogger.Info("Grace period not passed. PVC not stale.", "TIME_UNTIL_STALE", timeUntilStale)
			if !cfg.disableNotifications {
				// if the PVC is not yet stale, determine if users needs to be notified of impending stale disks.
				if ownerEmail == "" {
					logger.Warn("Namespace owner email could not be found. Skipping email notifications")
					continue
				}

				err := cfg.notifyPvcOwner(ownerEmail, timeUntilStale, pvc, httpClient, pvcLogger)
				if err != nil {
					pvcLogger.Error("Error while trying to notify user", "ERROR", err)
					continue
				}
			}
		} else {
			// if PVC is stale, proceed to delete it
			pvcLogger.Info("PVC is stale")
			//TODO: also notify user when their pvc does get deleted?
			err := cfg.deletePvc(pvc, pvcLogger)
			// if an error occurs while trying to delete the PVC,
			// just log it and continue to the next one
			if err != nil {
				pvcLogger.Error("Error while trying to delete unattached PVC.", "ERROR", err)
			}

			continue
		}
	}

	return nil
}

func (cfg *CleanerConfigs) cleanAttachedNamespaceVolumes(ns string, notebookPvcMap map[string][]corev1.PersistentVolumeClaim, ownerEmail string, httpClient *http.Client, logger *slog.Logger) error {
	logger.Info("Evaluating attached PVCs")

	for k, v := range notebookPvcMap {
		notebookLogger := logger.With("NOTEBOOK", k)

		staleVolumesCount := 0
		// map to keep track of which volumes are stale. Used for email notfications
		// key is the pvc name; value is the time until stale
		nonStaleVolumesMap := make(map[string]float64)

		for _, pvc := range v {
			notebookPvcLogger := notebookLogger.With("PVC", pvc.Name)

			timeUntilStale, err := cfg.getTimeUntilStale(pvc, cfg.params.gracePeriod, notebookPvcLogger)
			if err != nil {
				notebookPvcLogger.Error("Error while getting time until stale for PVC", "ERROR", err)
				continue
			}

			isStale := 0 > timeUntilStale
			if !isStale {
				notebookPvcLogger.Info("PVC not stale.", "TIME_UNTIL_STALE", timeUntilStale)
				nonStaleVolumesMap[pvc.Name] = timeUntilStale
				// slog.Info("test time", "PVC", pvc.Name, "TIME_UNTIL_STALE", timeUntilStale, "MAP", nonStaleVolumesMap)
			} else {
				notebookPvcLogger.Info("PVC is stale")
				staleVolumesCount++
			}
		}

		// only proceed to cleaning the notebook (and its volumes) if all the volumes are stale
		if staleVolumesCount == len(v) {
			//TODO: also notify user when their notebook and pvcs get deleted?
			err := cfg.deleteNotebookAndPvcs(ns, k, v, notebookLogger)
			if err != nil {
				notebookLogger.Error("Error while trying to delete notebook and its PVCs.", "ERROR", err)
				return err
			}
		} else if !cfg.disableNotifications {
			// if the notebook and PVCs are not yet stale, determine if users needs to be notified of impending stale disks.
			notebookLogger.Info("Notebook is not fully stale. Notifying user of upcoming stale volumes for the noteboook")
			// TODO: Make logic to notify a user when they have a notebook with one or more volumes that is stale
			// Could just loop over the notifyPvcOwner Fn for each pvc, but we might be able to just run this once for the whole notebook & its volumes
			// We would just have to most likely tweak the email format.

			if ownerEmail == "" {
				logger.Warn("Namespace owner email could not be found. Skipping email notifications")
				continue
			}
			for _, vol := range v {
				err := cfg.notifyPvcOwner(ownerEmail, nonStaleVolumesMap[vol.Name], vol, httpClient, notebookLogger)
				if err != nil {
					notebookLogger.Error("Error while trying to notify user", "ERROR", err)
					continue
				}
			}
		}
	}

	return nil
}

// Helper function to return a map of notebooks and their attached volumes.
// Will also update the given set to get all attached volumes
func setupNotebooksPvcsInfo(kubeflowClient *kubeflow.Clientset, ns string, pvcsMap map[string]corev1.PersistentVolumeClaim, attachedPVCs map[string]bool, logger *slog.Logger) (map[string][]corev1.PersistentVolumeClaim, error) {
	// get notebooks for profile namespace
	notebooks, err := kubeflowClient.KubeflowV1().Notebooks(ns).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		logger.Error("Error while getting notebooks", "ERROR", err)
		return nil, err
	}

	notebooksPvcMap := make(map[string][]corev1.PersistentVolumeClaim)

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

	return notebooksPvcMap, nil
}

// Gets the time of when a PVC was last used
func (cfg *CleanerConfigs) getPvcLastOwnershipTime(pvc corev1.PersistentVolumeClaim, logger *slog.Logger) (*time.Time, error) {
	pv, err := cfg.clientset.CoreV1().PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, v1.GetOptions{})
	if err != nil {
		logger.Error("Error while getting Persistent Volume for PVC", "PVC_STATUS", pvc.Status.Phase, "PV", pvc.Spec.VolumeName, "ERROR", err)
		return nil, err
	}

	lastOwnershipTime, err := getPvcLastOwnershipTimeFromAzure(cfg.disksClient, pvc, *pv, cfg.params.azureResourceGroup, logger)
	if lastOwnershipTime == nil || err != nil {
		logger.Error("Error while getting last ownership time for PVC")
		return nil, err
	}

	if lastOwnershipTime == nil {
		return lastOwnershipTime, errors.New("Cannot determine when PVC was last used. Last Ownership Time and Time Created on Azure Disk are null")
	}

	return lastOwnershipTime, nil
}

// Query Azure to get the last Ownership time for a PVC.
// This value represents the last time the PVC was attached to a running pod.
func getPvcLastOwnershipTimeFromAzure(disksClient *armcompute.DisksClient, pvc corev1.PersistentVolumeClaim, pv corev1.PersistentVolume, resourceGroup string, logger *slog.Logger) (*time.Time, error) {
	// Get the Azure Disk name from the PV resource
	diskHandle := pv.Spec.CSI.VolumeHandle
	diskHandleSplit := strings.Split(diskHandle, "/")
	diskName := diskHandleSplit[len(diskHandleSplit)-1]

	// Get the disk information from Azure
	diskResponse, err := disksClient.Get(context.TODO(), resourceGroup, diskName, nil)
	if err != nil {
		logger.Error("Error while getting Azure Disk", "DISK_NAME", diskName, "ERROR", err)
		return nil, err
	}

	// Convert the lastOwnershipUpdateTime into Time format
	lastOwnershipTime := diskResponse.Properties.LastOwnershipUpdateTime
	if lastOwnershipTime == nil {
		logger.Warn("Last Ownership Update Time is null. Using Time Created instead", "DISK_NAME", diskName)
		return diskResponse.Properties.TimeCreated, nil
	}

	return lastOwnershipTime, nil
}

// Returns the time until a PVC is stale
// A negative value means that the PVC is stale
func (cfg *CleanerConfigs) getTimeUntilStale(pvc corev1.PersistentVolumeClaim, gracePeriod int, logger *slog.Logger) (float64, error) {
	lastOwnershipTime, err := cfg.getPvcLastOwnershipTime(pvc, logger)
	if err != nil {
		logger.Error("Error while getting last ownership time for PVC", "ERROR", err)
		return 0, err
	}

	logger.Info("Retrieved last ownership time for PVC", "TIME", lastOwnershipTime)

	// difference in days
	diff := time.Since(*lastOwnershipTime).Hours() / 24

	logger.Info("Time passed since PVC was last used (in days)", "TIME", diff)

	return float64(gracePeriod) - diff, nil
}

func (cfg *CleanerConfigs) deletePvc(pvc corev1.PersistentVolumeClaim, logger *slog.Logger) error {
	if cfg.params.dryRun {
		logger.Info("[DRY RUN] Deleted PVC")
		return nil
	}

	err := cfg.clientset.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(context.TODO(), pvc.Name, v1.DeleteOptions{})
	if err != nil {
		logger.Error("Error while deleting PVC", "ERROR", err)
		return err
	}

	logger.Info("Deleted PVC")
	return nil
}

func (cfg *CleanerConfigs) deleteNotebookAndPvcs(namespace string, notebook string, pvcs []corev1.PersistentVolumeClaim, logger *slog.Logger) error {
	if cfg.params.dryRun {
		logger.Info("[DRY RUN] Deleted Notebook")

		for _, pvc := range pvcs {
			logger.Info("[DRY RUN] Deleted attached PVC", "PVC", pvc.Name)
		}

		return nil
	}

	err := cfg.kubeflowClient.KubeflowV1().Notebooks(namespace).Delete(context.TODO(), notebook, v1.DeleteOptions{})
	if err != nil {
		logger.Error("Error while deleting Notebook", "ERROR", err)
		return err
	}

	logger.Info("Deleted Notebook")

	for _, pvc := range pvcs {
		err := cfg.deletePvc(pvc, logger)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cfg *CleanerConfigs) notifyPvcOwner(ownerEmail string, timeUntilStale float64, pvc corev1.PersistentVolumeClaim, httpClient *http.Client, logger *slog.Logger) error {
	logger.Info("Notifying user of volume approching deletion")

	if len(cfg.params.notificationTimes) == 0 {
		logger.Warn("Notification Times parameter is empty, skipping notifying user")
		return nil
	}

	// If volume was used more recently than our farthest notification time,
	// Reset the notification count label and don't notify the user
	if float64(cfg.params.notificationTimes[0]) < timeUntilStale {
		if cfg.params.dryRun {
			// log.Print("[DRY RUN] Email owner.")
			// emailCount++
			// continue

			logger.Info("[DRY RUN] Volume was used recently. Resetting the notification count label and avoid sending notification", "TIME_UNTIL_STALE", timeUntilStale)
			return nil
		}

		logger.Info("Volume was used recently. Resetting the notification count label and avoid sending notification", "TIME_UNTIL_STALE", timeUntilStale)
		err := cleanerUtils.RemovePvcLabel(cfg.clientset, NOTIFICATION_COUNT_LABEL, pvc.Namespace, pvc.Name)
		if err != nil {
			logger.Error("Error while resetting notification count label", "ERROR", err)
			return err
		}
	}

	notifCountString, ok := pvc.Labels[NOTIFICATION_COUNT_LABEL]
	if !ok {
		// log.Printf("[INFO] Label %s not found. Skipping.", cfg.NotifLabel)
		// errCount++
		// continue

		// if notif count label is not present, assume this is the first time the pvc is evaluated
		logger.Info("Notification Count label not found. Defaulting to nearest value")
		notifCountString = "0"
		tempVal := 0
		// Loops through the defined notification times to find the nearest value without going over.
		for i := 0; i < len(cfg.params.notificationTimes); i++ {
			// slog.Info("test1", "v1", cfg.params.NotificationTimes[i], "v2", timeUntilStale)
			if float64(cfg.params.notificationTimes[i]) < timeUntilStale {
				break
			}

			tempVal = i
		}

		notifCountString = strconv.Itoa(tempVal)
	}

	notifCount, err := strconv.Atoi(notifCountString)
	if err != nil {
		// log.Printf("[ERROR] Failed to parse notification count: %v", countErr)
		// errCount++
		// continue

		logger.Error("Failed to parse notification count label", "ERROR", err)
		return err
	}

	shouldSendMail := cfg.ShouldSendMail(timeUntilStale, notifCount, logger)
	// if err != nil {
	// 	// log.Printf("[ERROR] Failed to parse timestamp: %s", mailError)
	// 	// errCount++
	// 	// continue
	// 	logger.Error("Error while trying to determine if email should be sent", "ERROR", err)
	// }

	if shouldSendMail {
		if cfg.params.dryRun {
			// log.Print("[DRY RUN] Email owner.")
			// emailCount++
			// continue

			logger.Info("[DRY RUN] Email owner.", "TIME_UNTIL_STALE", timeUntilStale)
			return nil
		}

		// personal consists of details passed into the email template as variables while email is
		// the email address that is consistent regardless of the template

		// email, personal := utilsInternal.EmailDetails(kube, pvc, daysLeft)
		personalisation := cleanerUtils.GetEmailPersonalisation(pvc.Namespace, pvc.Name, timeUntilStale)

		// TODO: Instead of sending 1 email per pvc, we should instead send one email per namespace with a report of what is getting stale.
		err := cleanerUtils.SendEmail(httpClient, cfg.emailConfigs, ownerEmail, personalisation)
		if err != nil {
			// log.Printf("[Error] Unable to send an email to %s at %s: %s", personal.Name, email, err)
			// errCount++
			// continue
			logger.Error("Error while sending email", "EMAIL", ownerEmail, "EMAIL_DETAILS", personalisation, "ERROR", err)
			return err
		}

		logger.Info("Email notification sent", "EMAIL", ownerEmail, "NAME", personalisation.Name)

		// Update Email Count
		// emailCount++

		// Increment notification count by 1
		newNotifCount := strconv.Itoa(notifCount + 1)
		err = cleanerUtils.SetPvcLabel(cfg.clientset, NOTIFICATION_COUNT_LABEL, newNotifCount, pvc.Namespace, pvc.Name)
		if err != nil {
			logger.Error("Error while updating notification count label", "ERROR", err)
			return err
		}
	}

	return nil
}

// checks email times and determines if this pvc's owner should be emailed
func (cfg *CleanerConfigs) ShouldSendMail(timeUntilStale float64, notifCount int, logger *slog.Logger) bool {
	// timeObj, err := time.Parse(cfg.TimeFormat, timestamp)
	// if err != nil {
	// 	return false, 0.0, err
	// }
	// daysLeft := float64(cfg.GracePeriod) - time.Since(timeObj).Hours()/24

	// this logic ensures that emails are eventually sent even if the
	// scheduler is down and misses a few days

	// log.Printf("[INFO] Emails already sent: %d", currNotif)

	if notifCount < len(cfg.params.notificationTimes) {
		currentNotifTime := float64(cfg.params.notificationTimes[notifCount])

		if currentNotifTime >= timeUntilStale {
			logger.Info("Email to be sent.", "CLOSEST_EMAIL_INTERVAL_DAYS", currentNotifTime)
			return true
		}

		logger.Info("Email will not be sent", "DAYS_UNTIL_NEXT_EMAIL", timeUntilStale-currentNotifTime)
	}

	return false
}
