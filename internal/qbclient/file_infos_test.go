package qbclient

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/utils"
)

// qbFilesForTest builds a TorrentFiles list in reverse index order, so anything
// relying on the incoming order rather than sortedByIndex shows up. Every file
// gets a distinct name and size.
func qbFilesForTest(n int) qbittorrent.TorrentFiles {
	files := make(qbittorrent.TorrentFiles, n)
	for i := range files {
		idx := n - 1 - i
		files[i].Index = idx
		files[i].Name = "f" + strconv.Itoa(idx) + ".dat"
		files[i].Size = int64(1000 + idx)
		files[i].Priority = 1
	}
	return files
}

// TestBuildFileInfos_ProbesEveryFileConcurrently pins that the identity probes
// actually overlap and stay within fileIDProbeConcurrency. Each probe parks
// until the limit is simultaneously in flight; a watchdog releases everyone if
// that never happens, so a serial regression fails on the recorded peak instead
// of hanging.
func TestBuildFileInfos_ProbesEveryFileConcurrently(t *testing.T) {
	const files = 64

	var (
		mu       sync.Mutex
		inFlight int
		peak     int
		calls    int
	)
	release := make(chan struct{})
	var once sync.Once

	probe := func(string) (uint64, uint64, error) {
		mu.Lock()
		calls++
		inFlight++
		peak = max(peak, inFlight)
		reached := inFlight >= fileIDProbeConcurrency
		mu.Unlock()

		if reached {
			once.Do(func() { close(release) })
		} else {
			// Serial regression: nobody else will ever arrive, so bail out
			// rather than deadlock and let the peak assertion do the talking.
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-release:
			case <-timer.C:
				once.Do(func() { close(release) })
			}
		}

		mu.Lock()
		inFlight--
		mu.Unlock()
		return 7, 9, nil
	}

	out := buildFileInfos(sortedByIndex(qbFilesForTest(files)), "/content", probe)

	if len(out) != files {
		t.Fatalf("built %d file infos, want %d", len(out), files)
	}
	if calls != files {
		t.Errorf("probed %d files, want %d", calls, files)
	}
	if peak != fileIDProbeConcurrency {
		t.Errorf("peak concurrent probes = %d, want exactly %d", peak, fileIDProbeConcurrency)
	}
}

// TestBuildFileInfos_StampsEachFileWithItsOwnIdentity pins that the fan-out
// writes every result into its own slot: each file carries the inode of the
// path derived from its own name, its own running-sum offset, and a file that
// is absent from disk keeps a zero identity rather than borrowing a neighbour's.
func TestBuildFileInfos_StampsEachFileWithItsOwnIdentity(t *testing.T) {
	const (
		files       = 16
		missingIdx  = 5
		unselectIdx = 9
	)

	contentDir := t.TempDir()
	qbFiles := qbFilesForTest(files)
	for i := range qbFiles {
		if qbFiles[i].Index == unselectIdx {
			qbFiles[i].Priority = 0
		}
		if qbFiles[i].Index == missingIdx {
			continue
		}
		path := filepath.Join(contentDir, qbFiles[i].Name)
		if err := os.WriteFile(path, []byte(qbFiles[i].Name), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
	}

	sorted := sortedByIndex(qbFiles)
	out := buildFileInfos(sorted, contentDir, utils.GetFileID)

	var wantOffset int64
	for i, fi := range out {
		if fi.GetPath() != sorted[i].Name {
			t.Fatalf("file %d: path = %q, want %q", i, fi.GetPath(), sorted[i].Name)
		}
		if fi.GetOffset() != wantOffset {
			t.Errorf("file %d: offset = %d, want %d", i, fi.GetOffset(), wantOffset)
		}
		if fi.GetSize() != sorted[i].Size {
			t.Errorf("file %d: size = %d, want %d", i, fi.GetSize(), sorted[i].Size)
		}
		if want := sorted[i].Priority > 0; fi.GetSelected() != want {
			t.Errorf("file %d: selected = %v, want %v", i, fi.GetSelected(), want)
		}
		wantOffset += sorted[i].Size

		if i == missingIdx {
			if fi.GetInode() != 0 || fi.GetDevice() != 0 {
				t.Errorf("absent file %d: identity = (%d,%d), want (0,0)",
					i, fi.GetDevice(), fi.GetInode())
			}
			continue
		}

		wantDev, wantIno, err := utils.GetFileID(filepath.Join(contentDir, sorted[i].Name))
		if err != nil {
			t.Fatalf("file %d: probing on-disk identity: %v", i, err)
		}
		if fi.GetInode() != wantIno || fi.GetDevice() != wantDev {
			t.Errorf("file %d: identity = (%d,%d), want (%d,%d) from its own path",
				i, fi.GetDevice(), fi.GetInode(), wantDev, wantIno)
		}
	}
}
