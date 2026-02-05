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

	tmpDir := t.TempDir()

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
		_, err := AreHardlinked(filepath.Join(tmpDir, "nonexistent1"), filepath.Join(tmpDir, "nonexistent2"))
		assert.Error(t, err)
	})
}

func TestFileExistsWithSize(t *testing.T) {
	t.Parallel()

	t.Run("file exists with correct size", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "test.txt")

		content := []byte("hello world")
		err := os.WriteFile(path, content, 0644)
		require.NoError(t, err)

		exists, err := FileExistsWithSize(path, int64(len(content)))
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("file exists with wrong size", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "test.txt")

		err := os.WriteFile(path, []byte("hello"), 0644)
		require.NoError(t, err)

		exists, err := FileExistsWithSize(path, 100)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("file does not exist", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()

		exists, err := FileExistsWithSize(filepath.Join(dir, "nonexistent"), 100)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}
