package hot

import (
	"context"
	"errors"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// checkDrainAnnotation queries the K8s API for the current pod's annotation.
// Returns true if the annotation value is "true".
func checkDrainAnnotation(ctx context.Context, annotationKey string) (bool, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return false, fmt.Errorf("in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return false, fmt.Errorf("creating clientset: %w", err)
	}

	podName := os.Getenv("POD_NAME")
	namespace := os.Getenv("POD_NAMESPACE")
	if podName == "" || namespace == "" {
		return false, errors.New("POD_NAME or POD_NAMESPACE not set")
	}

	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("getting pod: %w", err)
	}

	return pod.Annotations[annotationKey] == "true", nil
}
