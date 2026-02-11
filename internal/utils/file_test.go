package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAreHardlinked(t *testing.T) {
	t.Parallel()

	t.Run("hardlinked files return true", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		file1 := filepath.Join(dir, "file1.txt")
		file2 := filepath.Join(dir, "file2.txt")

		err := os.WriteFile(file1, []byte("test content"), 0644)
		require.NoError(t, err)

		err = os.Link(file1, file2)
		require.NoError(t, err)

		linked, err := AreHardlinked(file1, file2)
		require.NoError(t, err)
		assert.True(t, linked)
	})

	t.Run("different files return false", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		file1 := filepath.Join(dir, "file1.txt")
		file2 := filepath.Join(dir, "file2.txt")

		err := os.WriteFile(file1, []byte("content1"), 0644)
		require.NoError(t, err)

		err = os.WriteFile(file2, []byte("content2"), 0644)
		require.NoError(t, err)

		linked, err := AreHardlinked(file1, file2)
		require.NoError(t, err)
		assert.False(t, linked)
	})

	t.Run("nonexistent file returns error", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		_, err := AreHardlinked(filepath.Join(dir, "nonexistent1"), filepath.Join(dir, "nonexistent2"))
		assert.Error(t, err)
	})
}
