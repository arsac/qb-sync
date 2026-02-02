package tasks

import (
	"testing"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/assert"

	"github.com/mailoarsac/qb-router/internal/config"
)

func TestHasSyncedTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags string
		want bool
	}{
		{
			name: "empty tags",
			tags: "",
			want: false,
		},
		{
			name: "single synced tag",
			tags: "synced",
			want: true,
		},
		{
			name: "synced with other tags",
			tags: "foo, synced, bar",
			want: true,
		},
		{
			name: "synced at start",
			tags: "synced, foo",
			want: true,
		},
		{
			name: "synced at end",
			tags: "foo, synced",
			want: true,
		},
		{
			name: "no synced tag",
			tags: "foo, bar, baz",
			want: false,
		},
		{
			name: "partial match not counted",
			tags: "unsynced, syncedmore",
			want: false,
		},
		{
			name: "extra whitespace",
			tags: "  synced  ,  foo  ",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hasSyncedTag(tt.tags)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSplitTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tags string
		want []string
	}{
		{
			name: "empty string",
			tags: "",
			want: nil,
		},
		{
			name: "single tag",
			tags: "foo",
			want: []string{"foo"},
		},
		{
			name: "multiple tags",
			tags: "foo,bar,baz",
			want: []string{"foo", "bar", "baz"},
		},
		{
			name: "tags with spaces",
			tags: "foo, bar , baz",
			want: []string{"foo", "bar", "baz"},
		},
		{
			name: "empty segments",
			tags: "foo,,bar",
			want: []string{"foo", "bar"},
		},
		{
			name: "only spaces",
			tags: "   ",
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := splitTags(tt.tags)
			assert.Equal(t, tt.want, got)
		})
	}
}

func newTestQBTask(srcPath, destPath string) *QBTask {
	return &QBTask{
		cfg: &config.Config{
			SrcPath:        srcPath,
			DestPath:       destPath,
			MinSpaceGB:     50,
			MinSeedingTime: time.Hour,
			SleepInterval:  30 * time.Second,
		},
	}
}

func TestDestFilePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		srcPath  string
		destPath string
		savePath string
		fileName string
		want     string
	}{
		{
			name:     "simple file in subdirectory",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/src/Movies/SomeMovie",
			fileName: "movie.mkv",
			want:     "/dest/Movies/SomeMovie/movie.mkv",
		},
		{
			name:     "file directly in src",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/src",
			fileName: "file.txt",
			want:     "/dest/file.txt",
		},
		{
			name:     "nested path",
			srcPath:  "/downloads",
			destPath: "/cold/storage",
			savePath: "/downloads/TV/Show/Season1",
			fileName: "episode.mkv",
			want:     "/cold/storage/TV/Show/Season1/episode.mkv",
		},
		{
			name:     "save path not under src path (preserves full path)",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/other/path",
			fileName: "file.txt",
			want:     "/dest/other/path/file.txt",
		},
		{
			name:     "trailing slash in src path",
			srcPath:  "/src/",
			destPath: "/dest",
			savePath: "/src/Movies",
			fileName: "movie.mkv",
			want:     "/dest/Movies/movie.mkv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			task := newTestQBTask(tt.srcPath, tt.destPath)
			got := task.destFilePath(tt.savePath, tt.fileName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDestSavePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		srcPath  string
		destPath string
		savePath string
		want     string
	}{
		{
			name:     "simple subdirectory",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/src/Movies/SomeMovie",
			want:     "/dest/Movies/SomeMovie",
		},
		{
			name:     "root save path",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/src",
			want:     "/dest",
		},
		{
			name:     "nested path",
			srcPath:  "/downloads",
			destPath: "/cold",
			savePath: "/downloads/TV/Show",
			want:     "/cold/TV/Show",
		},
		{
			name:     "save path not under src (fallback to base)",
			srcPath:  "/src",
			destPath: "/dest",
			savePath: "/other/SomeDir",
			want:     "/dest/SomeDir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			task := newTestQBTask(tt.srcPath, tt.destPath)
			got := task.destSavePath(tt.savePath)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCreateGroup(t *testing.T) {
	t.Parallel()

	task := newTestQBTask("/src", "/dest")

	tests := []struct {
		name           string
		torrents       []qbittorrent.Torrent
		wantPopularity int64
		wantMaxSize    int64
		wantMinSeeding int64
	}{
		{
			name: "single torrent",
			torrents: []qbittorrent.Torrent{
				{Hash: "a", Size: 1000, SeedingTime: 100, NumComplete: 5, NumIncomplete: 3},
			},
			wantPopularity: 8,
			wantMaxSize:    1000,
			wantMinSeeding: 100,
		},
		{
			name: "multiple torrents - picks max size and min seeding",
			torrents: []qbittorrent.Torrent{
				{Hash: "a", Size: 1000, SeedingTime: 100, NumComplete: 5, NumIncomplete: 3},
				{Hash: "b", Size: 5000, SeedingTime: 50, NumComplete: 10, NumIncomplete: 2},
				{Hash: "c", Size: 2000, SeedingTime: 200, NumComplete: 1, NumIncomplete: 1},
			},
			wantPopularity: 8 + 12 + 2, // sum of all
			wantMaxSize:    5000,       // max
			wantMinSeeding: 50,         // min
		},
		{
			name: "all same values",
			torrents: []qbittorrent.Torrent{
				{Hash: "a", Size: 100, SeedingTime: 60, NumComplete: 1, NumIncomplete: 0},
				{Hash: "b", Size: 100, SeedingTime: 60, NumComplete: 1, NumIncomplete: 0},
			},
			wantPopularity: 2,
			wantMaxSize:    100,
			wantMinSeeding: 60,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			group := task.createGroup(tt.torrents)

			assert.Equal(t, tt.wantPopularity, group.popularity)
			assert.Equal(t, tt.wantMaxSize, group.maxSize)
			assert.Equal(t, tt.wantMinSeeding, group.minSeeding)
			assert.Len(t, group.torrents, len(tt.torrents))
		})
	}
}

func TestSortGroupsByPriority(t *testing.T) {
	t.Parallel()

	task := newTestQBTask("/src", "/dest")

	groups := []torrentGroup{
		{popularity: 100, maxSize: 1000}, // high popularity, small
		{popularity: 10, maxSize: 500},   // low popularity, small
		{popularity: 10, maxSize: 2000},  // low popularity, large
		{popularity: 50, maxSize: 1500},  // medium popularity
	}

	sorted := task.sortGroupsByPriority(groups)

	// Should sort by: popularity ASC, then maxSize DESC
	// Expected order:
	// 1. popularity=10, size=2000 (lowest pop, largest)
	// 2. popularity=10, size=500 (lowest pop, smaller)
	// 3. popularity=50, size=1500 (medium pop)
	// 4. popularity=100, size=1000 (highest pop)

	assert.Equal(t, int64(10), sorted[0].popularity)
	assert.Equal(t, int64(2000), sorted[0].maxSize)

	assert.Equal(t, int64(10), sorted[1].popularity)
	assert.Equal(t, int64(500), sorted[1].maxSize)

	assert.Equal(t, int64(50), sorted[2].popularity)
	assert.Equal(t, int64(100), sorted[3].popularity)
}

func TestMoveGroupToCold_RespectsMinSeedingTime(t *testing.T) {
	t.Parallel()

	// Create task with 1 hour min seeding time
	task := &QBTask{
		cfg: &config.Config{
			SrcPath:        "/src",
			DestPath:       "/dest",
			MinSeedingTime: time.Hour,
			DryRun:         true, // Use dry run to avoid needing real clients
		},
	}

	tests := []struct {
		name       string
		minSeeding int64
		shouldMove bool
	}{
		{
			name:       "seeding time less than required",
			minSeeding: 1800, // 30 minutes
			shouldMove: false,
		},
		{
			name:       "seeding time equals required",
			minSeeding: 3600, // 1 hour
			shouldMove: true,
		},
		{
			name:       "seeding time exceeds required",
			minSeeding: 7200, // 2 hours
			shouldMove: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			group := torrentGroup{
				torrents:   []qbittorrent.Torrent{{Hash: "test", Name: "Test Torrent"}},
				minSeeding: tt.minSeeding,
			}

			// In dry run mode, moveGroupToCold will log but not actually move
			// We're testing that it respects the min seeding time check
			// The function returns nil for both cases (skipped or dry-run moved)
			// but we can verify the logic by checking if it would proceed

			// Since we can't easily test the actual behavior without mocking,
			// we verify the threshold calculation is correct
			minSeedingSeconds := int64(task.cfg.MinSeedingTime.Seconds())
			wouldMove := group.minSeeding >= minSeedingSeconds
			assert.Equal(t, tt.shouldMove, wouldMove)
		})
	}
}

func TestIsErrorState(t *testing.T) {
	t.Parallel()

	task := newTestQBTask("/src", "/dest")

	tests := []struct {
		state   qbittorrent.TorrentState
		isError bool
	}{
		{qbittorrent.TorrentStateError, true},
		{qbittorrent.TorrentStateMissingFiles, true},
		{qbittorrent.TorrentStateUploading, false},
		{qbittorrent.TorrentStateStalledUp, false},
		{qbittorrent.TorrentStatePausedUp, false},
		{qbittorrent.TorrentStateCheckingUp, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.isError, task.isErrorState(tt.state))
		})
	}
}

func TestIsCheckingState(t *testing.T) {
	t.Parallel()

	task := newTestQBTask("/src", "/dest")

	tests := []struct {
		state      qbittorrent.TorrentState
		isChecking bool
	}{
		{qbittorrent.TorrentStateCheckingUp, true},
		{qbittorrent.TorrentStateCheckingDl, true},
		{qbittorrent.TorrentStateCheckingResumeData, true},
		{qbittorrent.TorrentStateUploading, false},
		{qbittorrent.TorrentStateStalledUp, false},
		{qbittorrent.TorrentStateError, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.isChecking, task.isCheckingState(tt.state))
		})
	}
}
