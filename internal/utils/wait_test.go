package utils

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUntil(t *testing.T) {
	t.Parallel()

	t.Run("returns immediately when condition is met", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		start := time.Now()
		err := Until(ctx, func(_ context.Context) (bool, error) {
			return true, nil
		}, 100*time.Millisecond, 1*time.Second)

		require.NoError(t, err)
		assert.Less(t, time.Since(start), 50*time.Millisecond)
	})

	t.Run("waits until condition is met", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		calls := 0
		err := Until(ctx, func(_ context.Context) (bool, error) {
			calls++
			return calls >= 3, nil
		}, 10*time.Millisecond, 1*time.Second)

		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("returns timeout error when condition not met", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		err := Until(ctx, func(_ context.Context) (bool, error) {
			return false, nil
		}, 10*time.Millisecond, 50*time.Millisecond)

		assert.ErrorIs(t, err, ErrTimeout)
	})

	t.Run("returns error from condition func", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		expectedErr := errors.New("test error")
		err := Until(ctx, func(_ context.Context) (bool, error) {
			return false, expectedErr
		}, 10*time.Millisecond, 1*time.Second)

		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()

		err := Until(ctx, func(_ context.Context) (bool, error) {
			return false, nil
		}, 10*time.Millisecond, 1*time.Second)

		assert.ErrorIs(t, err, context.Canceled)
	})
}
