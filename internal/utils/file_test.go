package utils

import (
	"context"
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

func TestFdCache_ReusesAcrossReads(t *testing.T) {
	t.Parallel()

	// Two regions in one file. Two cached reads should produce one open
	// (the cache hit on the second). We verify by counting fds against
	// /proc on Linux is platform-specific; instead verify the behavioral
	// contract: same returned *os.File pointer on repeated Open of the
	// same path, and Close() makes the cache empty.
	dir := t.TempDir()
	path := filepath.Join(dir, "f.dat")
	require.NoError(t, os.WriteFile(path, []byte("hello world!!!! more bytes"), 0o644))

	cache := NewFdCache()
	defer cache.Close()

	fd1, err := cache.Open(path)
	require.NoError(t, err)
	fd2, err := cache.Open(path)
	require.NoError(t, err)
	assert.Same(t, fd1, fd2, "second Open of same path must return the cached *os.File pointer (no re-open)")

	cache.Close()
	assert.Empty(t, cache.fds, "Close must clear the cache")
}

func TestReadPieceFromFilesCached_HonorsContextCancel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.dat")
	require.NoError(t, os.WriteFile(path, make([]byte, 1024), 0o644))

	cache := NewFdCache()
	defer cache.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancelled

	regions := []FileRegion{{Path: path, Offset: 0, Size: 1024}}
	_, err := ReadPieceFromFilesCached(ctx, cache, regions, 0, 100)
	require.Error(t, err, "cancelled context must surface from the read loop, not silently complete")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReadPieceFromFilesCached_MatchesUncachedResult(t *testing.T) {
	t.Parallel()

	// Two-file torrent layout: file A is bytes [0..512), file B is bytes [512..1024).
	// Read a piece that spans the boundary; cached and uncached must produce identical bytes.
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.dat")
	pathB := filepath.Join(dir, "b.dat")
	bytesA := make([]byte, 512)
	bytesB := make([]byte, 512)
	for i := range bytesA {
		bytesA[i] = byte(i)
	}
	for i := range bytesB {
		bytesB[i] = byte(255 - i)
	}
	require.NoError(t, os.WriteFile(pathA, bytesA, 0o644))
	require.NoError(t, os.WriteFile(pathB, bytesB, 0o644))

	regions := []FileRegion{
		{Path: pathA, Offset: 0, Size: 512},
		{Path: pathB, Offset: 512, Size: 512},
	}

	uncached, err := ReadPieceFromFiles(regions, 256, 512) // straddles the boundary
	require.NoError(t, err)

	cache := NewFdCache()
	defer cache.Close()
	cached, err := ReadPieceFromFilesCached(context.Background(), cache, regions, 256, 512)
	require.NoError(t, err)

	assert.Equal(t, uncached, cached, "cached read must match uncached for boundary-spanning piece")
}

func TestFdCache_OpenAfterCloseReinitializes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))

	cache := NewFdCache()
	_, err := cache.Open(path)
	require.NoError(t, err)
	cache.Close()

	// A late Open after Close must not panic on the nil'd map; it should
	// hand back a fresh fd (deferred Close patterns can race a final read).
	f, err := cache.Open(path)
	require.NoError(t, err)
	require.NotNil(t, f)
	cache.Close()
}
