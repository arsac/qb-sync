package hot

import (
	"context"
	"errors"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/arsac/qb-sync/internal/metrics"
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

// Drain triggers a one-shot evacuation: hands off ALL synced torrents to cold,
// ignoring MinSpaceGB and MinSeedingTime. Blocks until the cycle completes.
// Returns ErrDrainInProgress if a drain is already running.
func (t *QBTask) Drain(ctx context.Context) error {
	if !t.draining.CompareAndSwap(false, true) {
		return ErrDrainInProgress
	}
	defer func() {
		t.draining.Store(false)
		metrics.Draining.Set(0)
	}()
	metrics.Draining.Set(1)
	t.logger.InfoContext(ctx, "drain started")
	err := t.maybeMoveToCold(ctx)
	if err != nil {
		t.logger.ErrorContext(ctx, "drain failed", "error", err)
	} else {
		t.logger.InfoContext(ctx, "drain complete")
	}
	return err
}

// Draining reports whether a drain is in progress.
func (t *QBTask) Draining() bool {
	return t.draining.Load()
}

// MaybeMoveToCold is the exported version of maybeMoveToCold for testing.
func (t *QBTask) MaybeMoveToCold(ctx context.Context) error {
	return t.maybeMoveToCold(ctx)
}
