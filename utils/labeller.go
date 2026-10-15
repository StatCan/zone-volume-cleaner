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

// modifies pvc labels
// requires sufficient rbac permissions
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

// setting label will add it if doesn't exist
func SetPvcLabel(clientset *kubernetes.Clientset, label string, value string, ns string, pvc string) error {
	err := patchPvcLabel(clientset, label, fmt.Sprintf(`"%s"`, value), ns, pvc)

	return err
}

// setting label to null (not "null") will remove it
func RemovePvcLabel(clientset *kubernetes.Clientset, label string, ns string, pvc string) error {
	err := patchPvcLabel(clientset, label, "null", ns, pvc)

	return err
}
