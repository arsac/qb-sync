package source

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/config"
	"github.com/arsac/qb-sync/internal/qbclient"
)

// hardlinkFixture lays out a content directory where torrents 2k and 2k+1 share
// one hardlinked file, plus two loners: one whose file exists and one whose file
// does not. It returns the torrents and the hashes expected in each group.
func newHardlinkFixture(t *testing.T, pairs int) (*QBTask, *mockQBClient, []qbittorrent.Torrent, [][]string) {
	t.Helper()

	contentDir := t.TempDir()
	filesByHash := make(map[string]qbittorrent.TorrentFiles)

	writeFile := func(name string) string {
		path := filepath.Join(contentDir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}

	// Each torrent carries a unique file so a cross-wired probe attributes an
	// inode to the wrong torrent and shows up as a wrong group.
	addTorrent := func(hash string, names ...string) {
		files := make(qbittorrent.TorrentFiles, 0, len(names)+1)
		unique := fmt.Sprintf("%s/unique.bin", hash)
		writeFile(unique)
		files = append(files, qbittorrent.TorrentFile{Index: 0, Name: unique, Priority: 1})
		for i, name := range names {
			files = append(files, qbittorrent.TorrentFile{Index: i + 1, Name: name, Priority: 1})
		}
		filesByHash[hash] = files
	}

	var torrents []qbittorrent.Torrent
	var wantGroups [][]string
	add := func(hash string) {
		torrents = append(torrents, qbittorrent.Torrent{Hash: hash, SavePath: contentDir})
	}

	for k := range pairs {
		shared := fmt.Sprintf("shared%02d.bin", k)
		sharedPath := writeFile(shared)
		link := fmt.Sprintf("link%02d.bin", k)
		if err := os.Link(sharedPath, filepath.Join(contentDir, link)); err != nil {
			t.Fatalf("hardlink %s: %v", link, err)
		}

		left, right := fmt.Sprintf("pair%02da", k), fmt.Sprintf("pair%02db", k)
		addTorrent(left, shared)
		addTorrent(right, link)
		add(left)
		add(right)
		wantGroups = append(wantGroups, []string{left, right})
	}

	// A torrent sharing nothing stays alone.
	addTorrent("loner")
	add("loner")
	wantGroups = append(wantGroups, []string{"loner"})

	// A torrent whose files are absent from disk contributes no inodes at all,
	// which must still leave it in a group of its own rather than dropping it.
	filesByHash["ghost"] = qbittorrent.TorrentFiles{
		{Index: 0, Name: "ghost/missing.bin", Priority: 1},
	}
	add("ghost")
	wantGroups = append(wantGroups, []string{"ghost"})

	client := &mockQBClient{filesByHash: filesByHash}
	task := &QBTask{
		cfg:       &config.SourceConfig{},
		logger:    testLogger(t),
		srcClient: client,
		source:    qbclient.NewSource(nil, contentDir),
	}
	return task, client, torrents, wantGroups
}

// groupHashes renders the grouping as sorted hash lists so it can be compared
// regardless of the order union-find happened to emit roots in.
func groupHashes(groups []torrentGroup) [][]string {
	out := make([][]string, 0, len(groups))
	for _, g := range groups {
		hashes := make([]string, 0, len(g.torrents))
		for _, tor := range g.torrents {
			hashes = append(hashes, tor.Hash)
		}
		slices.Sort(hashes)
		out = append(out, hashes)
	}
	slices.SortFunc(out, slices.Compare)
	return out
}

func TestGroupHardlinkedTorrents_FanOutGroupsSharedInodes(t *testing.T) {
	t.Parallel()

	task, client, torrents, wantGroups := newHardlinkFixture(t, 12)

	got := groupHashes(task.groupHardlinkedTorrents(t.Context(), torrents))

	slices.SortFunc(wantGroups, slices.Compare)
	if len(got) != len(wantGroups) {
		t.Fatalf("group count = %d, want %d (got %v)", len(got), len(wantGroups), got)
	}
	for i := range got {
		if !slices.Equal(got[i], wantGroups[i]) {
			t.Errorf("group %d = %v, want %v", i, got[i], wantGroups[i])
		}
	}

	if calls := client.getFilesCalls.Load(); calls != int64(len(torrents)) {
		t.Errorf("file-information calls = %d, want one per torrent (%d)", calls, len(torrents))
	}
}

func TestCollectFileOwners_ProbesTorrentsConcurrently(t *testing.T) {
	t.Parallel()

	// Park every probe until hardlinkScanConcurrency are simultaneously in
	// flight. A serial implementation never clears the barrier and is released
	// by the watchdog instead, recording a peak of 1.
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
		released bool
	)
	barrier := make(chan struct{})
	release := func() {
		mu.Lock()
		defer mu.Unlock()
		if !released {
			released = true
			close(barrier)
		}
	}

	task, client, torrents, _ := newHardlinkFixture(t, 12)
	client.filesHook = func(string) {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		reached := inFlight >= hardlinkScanConcurrency
		mu.Unlock()

		if reached {
			release()
		} else {
			timer := time.AfterFunc(2*time.Second, release)
			<-barrier
			timer.Stop()
		}

		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	task.collectFileOwners(t.Context(), torrents)

	mu.Lock()
	defer mu.Unlock()
	if peak != hardlinkScanConcurrency {
		t.Errorf("peak concurrent probes = %d, want %d", peak, hardlinkScanConcurrency)
	}
}

func TestCollectFileOwners_HonoursItsConcurrencyLimit(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	inFlight, peak := 0, 0

	task, client, torrents, _ := newHardlinkFixture(t, 12)
	client.filesHook = func(string) {
		mu.Lock()
		inFlight++
		peak = max(peak, inFlight)
		mu.Unlock()

		time.Sleep(time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()
	}

	task.collectFileOwners(t.Context(), torrents)

	mu.Lock()
	defer mu.Unlock()
	if peak > hardlinkScanConcurrency {
		t.Errorf("peak concurrent probes = %d, want at most %d", peak, hardlinkScanConcurrency)
	}
}
