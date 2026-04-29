package destination

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// TestFlushDirtyStates_ReleasesLockDuringIO verifies that flushDirtyStates
// releases state.mu while performing file I/O. Before the snapshot fix,
// state.mu was held during the entire saveState call. A slow or hung
// filesystem (e.g. NFS) would block all WritePiece and FinalizeTorrent
// calls for the same torrent, creating a liveness hazard.
//
// The test injects a saveState that blocks on a channel, simulating slow I/O.
// A concurrent goroutine tries to acquire state.mu during that window.
// With the old code this would deadlock; with the snapshot fix it succeeds.
func TestFlushDirtyStates_ReleasesLockDuringIO(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	written := bitset.New(100)
	written.Set(0)
	state := &serverTorrentState{
		written:          written,
		dirty:            true,
		statePath:        tmpDir + "/.state",
		piecesSinceFlush: 1,
	}

	store := newTorrentStore(tmpDir, logger)
	store.entries["deadlock-test"] = state
	s := &Server{
		config: ServerConfig{BasePath: tmpDir},
		logger: logger,
		store:  store,
	}

	// saveStateFunc blocks until unblockIO is closed, simulating slow disk I/O.
	unblockIO := make(chan struct{})
	ioStarted := make(chan struct{})
	s.saveStateFunc = func(_ string, _ *bitset.BitSet) error {
		close(ioStarted) // signal that we're in the I/O phase
		<-unblockIO      // block until test unblocks us
		return nil
	}

	// Start flush in background.
	flushDone := make(chan struct{})
	go func() {
		s.flushDirtyStates(context.Background())
		close(flushDone)
	}()

	// Wait for flush to enter the I/O phase (lock must be released by now).
	select {
	case <-ioStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for flush to start I/O")
	}

	// Try to acquire state.mu. With the snapshot fix, this succeeds immediately
	// because the lock was released before I/O. With the old code, the lock
	// would still be held and this would block until unblockIO is closed.
	lockAcquired := make(chan struct{})
	go func() {
		state.mu.Lock()
		close(lockAcquired)
		state.mu.Unlock()
	}()

	select {
	case <-lockAcquired:
		// Success: state.mu was free during I/O.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("state.mu was blocked during flush I/O — lock held during file write (old behavior)")
	}

	// Unblock the I/O and let flush complete.
	close(unblockIO)
	<-flushDone

	// Verify state was marked clean.
	state.mu.Lock()
	if state.dirty {
		t.Error("expected dirty=false after successful flush")
	}
	if state.piecesSinceFlush != 0 {
		t.Errorf("expected piecesSinceFlush=0, got %d", state.piecesSinceFlush)
	}
	state.mu.Unlock()
}

// TestFlushDirtyStates_ConcurrentWritesDuringIO verifies that pieces written
// during a flush are not lost. The snapshot captures state at a point in time;
// writes that arrive during I/O must keep the dirty flag set.
func TestFlushDirtyStates_ConcurrentWritesDuringIO(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := testLogger(t)

	written := bitset.New(100)
	for i := range uint(10) {
		written.Set(i)
	}
	state := &serverTorrentState{
		written:          written,
		dirty:            true,
		statePath:        tmpDir + "/.state",
		piecesSinceFlush: 5,
	}

	store := newTorrentStore(tmpDir, logger)
	store.entries["concurrent-test"] = state
	s := &Server{
		config: ServerConfig{BasePath: tmpDir},
		logger: logger,
		store:  store,
	}

	// During the I/O phase, simulate new pieces arriving.
	unblockIO := make(chan struct{})
	ioStarted := make(chan struct{})
	s.saveStateFunc = func(_ string, _ *bitset.BitSet) error {
		close(ioStarted)
		<-unblockIO
		return nil
	}

	var wg sync.WaitGroup

	// Start flush.
	wg.Go(func() {
		s.flushDirtyStates(context.Background())
	})

	// Wait for I/O phase, then simulate concurrent writes.
	<-ioStarted
	state.mu.Lock()
	state.written.Set(50)
	state.dirty = true
	state.piecesSinceFlush += 3 // 3 new pieces arrived during I/O
	state.mu.Unlock()

	// Unblock I/O and let flush complete.
	close(unblockIO)
	wg.Wait()

	// Verify: dirty must remain true because new writes arrived during I/O.
	state.mu.Lock()
	defer state.mu.Unlock()

	if !state.dirty {
		t.Error("dirty should remain true — pieces were written during flush I/O")
	}
	if state.piecesSinceFlush != 3 {
		t.Errorf("piecesSinceFlush should be 3 (new writes only), got %d", state.piecesSinceFlush)
	}
}
