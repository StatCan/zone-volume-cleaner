package utils

import (
	// standard packages
	"context"
	"fmt"

	// external packages
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Patches the label from the PVC resource
func patchPvcLabel(clientset *kubernetes.Clientset, label string, value string, ns string, pvc string) error {
	patch := []byte(fmt.Sprintf(`{"metadata":{"labels":{"%s":%s}}}`, label, value))
	_, err := clientset.CoreV1().PersistentVolumeClaims(ns).Patch(
		context.TODO(),
		pvc,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)

	return err
}

// Updates the label value from the PVC resource, or adds the label if it doesnt exist
func SetPvcLabel(clientset *kubernetes.Clientset, label string, value string, ns string, pvc string) error {
	return patchPvcLabel(clientset, label, fmt.Sprintf(`"%s"`, value), ns, pvc)
}

// Removes the label from the PVC resource
func RemovePvcLabel(clientset *kubernetes.Clientset, label string, ns string, pvc string) error {
	// setting label to null (not "null") will remove it
	return patchPvcLabel(clientset, label, "null", ns, pvc)
}
