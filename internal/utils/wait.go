package utils

import (
	"context"
	"errors"
	"time"
)

var ErrTimeout = errors.New("timeout waiting for condition")

type ConditionFunc func(ctx context.Context) (bool, error)

func Until(ctx context.Context, condition ConditionFunc, interval, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		met, err := condition(ctx)
		if err != nil {
			return err
		}
		if met {
			return nil
		}

		if time.Now().After(deadline) {
			return ErrTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
