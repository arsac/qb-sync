package tasks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/errgroup"

	"github.com/mailoarsac/qb-router/internal/config"
	"github.com/mailoarsac/qb-router/internal/utils"
)

const (
	syncedTag           = "synced"
	pollInterval        = 2 * time.Second
	pollTimeout         = 5 * time.Minute
	bytesPerGB          = int64(1024 * 1024 * 1024)
	maxConcurrentChecks = 10
)

type torrentFile struct {
	Name string
	Size int64
}

type QBTask struct {
	cfg        *config.Config
	logger     *slog.Logger
	srcClient  *qbittorrent.Client
	destClient *qbittorrent.Client
}

func NewQBTask(cfg *config.Config, logger *slog.Logger) (*QBTask, error) {
	srcClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     cfg.SrcURL,
		Username: cfg.SrcUsername,
		Password: cfg.SrcPassword,
	})

	destClient := qbittorrent.NewClient(qbittorrent.Config{
		Host:     cfg.DestURL,
		Username: cfg.DestUsername,
		Password: cfg.DestPassword,
	})

	return &QBTask{
		cfg:        cfg,
		logger:     logger,
		srcClient:  srcClient,
		destClient: destClient,
	}, nil
}

func (t *QBTask) Run(ctx context.Context) error {
	if err := t.srcClient.LoginCtx(ctx); err != nil {
		return fmt.Errorf("logging into source: %w", err)
	}
	if err := t.destClient.LoginCtx(ctx); err != nil {
		return fmt.Errorf("logging into destination: %w", err)
	}

	ticker := time.NewTicker(t.cfg.SleepInterval)
	defer ticker.Stop()

	for {
		t.runOnce(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (t *QBTask) runOnce(ctx context.Context) {
	if err := t.tagSyncedTorrents(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to tag synced torrents", "error", err)
	}

	if err := t.maybeMoveToCold(ctx); err != nil {
		t.logger.ErrorContext(ctx, "failed to move torrents to cold", "error", err)
	}
}

func (t *QBTask) tagSyncedTorrents(ctx context.Context) error {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Filter: qbittorrent.TorrentFilterCompleted,
	})
	if err != nil {
		return fmt.Errorf("fetching completed torrents: %w", err)
	}

	for _, torrent := range torrents {
		if hasSyncedTag(torrent.Tags) {
			continue
		}

		synced, syncErr := t.areTorrentFilesSynced(ctx, torrent)
		if syncErr != nil {
			t.logger.WarnContext(ctx, "failed to check if synced", "hash", torrent.Hash, "error", syncErr)
			continue
		}

		if synced {
			t.logger.InfoContext(ctx, "tagging torrent as synced", "name", torrent.Name, "hash", torrent.Hash)
			if !t.cfg.DryRun {
				if tagErr := t.srcClient.AddTagsCtx(ctx, []string{torrent.Hash}, syncedTag); tagErr != nil {
					t.logger.ErrorContext(ctx, "failed to add tag", "hash", torrent.Hash, "error", tagErr)
				}
			}
		}
	}

	return nil
}

func (t *QBTask) areTorrentFilesSynced(ctx context.Context, torrent qbittorrent.Torrent) (bool, error) {
	filesPtr, err := t.srcClient.GetFilesInformationCtx(ctx, torrent.Hash)
	if err != nil {
		return false, fmt.Errorf("fetching files: %w", err)
	}

	if filesPtr == nil || len(*filesPtr) == 0 {
		return false, nil
	}

	files := *filesPtr

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentChecks)

	results := make([]bool, len(files))

	for i, file := range files {
		g.Go(func() error {
			destPath := t.destFilePath(torrent.SavePath, file.Name)

			exists, checkErr := utils.FileExistsWithSize(destPath, file.Size)
			if checkErr != nil {
				t.logger.WarnContext(ctx, "error checking file", "dest", destPath, "error", checkErr)
				return nil
			}
			results[i] = exists
			return nil
		})
	}

	if waitErr := g.Wait(); waitErr != nil {
		return false, waitErr
	}

	for _, synced := range results {
		if !synced {
			return false, nil
		}
	}

	return true, nil
}

// relativePath strips the source path prefix from an absolute path.
func (t *QBTask) relativePath(absPath string) string {
	if after, ok := strings.CutPrefix(absPath, t.cfg.SrcPath); ok {
		rel := after
		return strings.TrimPrefix(rel, string(filepath.Separator))
	}
	return absPath
}

// destFilePath calculates the destination path for a file.
// It strips the source path prefix and prepends the destination path.
// Example: srcPath=/src, savePath=/src/Movies/Film, fileName=movie.mkv
// Result: /dest/Movies/Film/movie.mkv.
func (t *QBTask) destFilePath(savePath, fileName string) string {
	contentPath := filepath.Join(savePath, fileName)
	return filepath.Join(t.cfg.DestPath, t.relativePath(contentPath))
}

func (t *QBTask) getFreeSpaceGB(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	// Compute free space in GB.
	// Use uint64 arithmetic and divide by bytesPerGB first to ensure the
	// result fits in int64 for any filesystem under 8 exabytes.
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	freeGB := freeBytes / uint64(bytesPerGB)
	// Safe conversion: freeGB is at most ~8 billion (8 EB / 1 GB) which fits in int64.
	if freeGB > uint64(math.MaxInt64) {
		return math.MaxInt64, nil
	}
	return int64(freeGB), nil
}

func (t *QBTask) maybeMoveToCold(ctx context.Context) error {
	if !t.cfg.Force {
		freeSpaceGB, err := t.getFreeSpaceGB(t.cfg.SrcPath)
		if err != nil {
			return fmt.Errorf("getting free space: %w", err)
		}

		t.logger.InfoContext(ctx, "checking free space", "freeGB", freeSpaceGB, "minGB", t.cfg.MinSpaceGB)

		if freeSpaceGB >= t.cfg.MinSpaceGB {
			return nil
		}
	}

	torrents, err := t.fetchSyncedTorrents(ctx)
	if err != nil {
		return fmt.Errorf("fetching synced torrents: %w", err)
	}

	groups := t.groupHardlinkedTorrents(ctx, torrents)
	sortedGroups := t.sortGroupsByPriority(groups)

	for _, group := range sortedGroups {
		if moveErr := t.moveGroupToCold(ctx, group); moveErr != nil {
			t.logger.ErrorContext(ctx, "failed to move group", "error", moveErr)
			continue
		}

		if !t.cfg.Force {
			currentSpaceGB, spaceErr := t.getFreeSpaceGB(t.cfg.SrcPath)
			if spaceErr != nil {
				return fmt.Errorf("getting free space: %w", spaceErr)
			}
			if currentSpaceGB >= t.cfg.MinSpaceGB {
				t.logger.InfoContext(ctx, "reached minimum free space, stopping migration")
				return nil
			}
		}
	}

	return nil
}

func (t *QBTask) fetchSyncedTorrents(ctx context.Context) ([]qbittorrent.Torrent, error) {
	torrents, err := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Filter: qbittorrent.TorrentFilterCompleted,
		Tag:    syncedTag,
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(torrents, func(i, j int) bool {
		return torrents[i].Size < torrents[j].Size
	})

	return torrents, nil
}

type torrentGroup struct {
	torrents   []qbittorrent.Torrent
	popularity int64
	maxSize    int64
	minSeeding int64
}

func (t *QBTask) groupHardlinkedTorrents(ctx context.Context, torrents []qbittorrent.Torrent) []torrentGroup {
	if len(torrents) == 0 {
		return nil
	}

	visited := make(map[string]bool)
	var groups []torrentGroup

	fileCache := make(map[string][]torrentFile)
	for _, torrent := range torrents {
		filesPtr, err := t.srcClient.GetFilesInformationCtx(ctx, torrent.Hash)
		if err != nil {
			t.logger.WarnContext(ctx, "failed to get files", "hash", torrent.Hash, "error", err)
			continue
		}
		if filesPtr != nil {
			files := make([]torrentFile, len(*filesPtr))
			for i, f := range *filesPtr {
				files[i] = torrentFile{Name: f.Name, Size: f.Size}
			}
			fileCache[torrent.Hash] = files
		}
	}

	for _, torrent := range torrents {
		if visited[torrent.Hash] {
			continue
		}

		group := []qbittorrent.Torrent{torrent}
		visited[torrent.Hash] = true

		for _, other := range torrents {
			if visited[other.Hash] {
				continue
			}

			if t.areHardlinkedTorrents(torrent, other, fileCache) {
				group = append(group, other)
				visited[other.Hash] = true
			}
		}

		groups = append(groups, t.createGroup(group))
	}

	return groups
}

func (t *QBTask) areHardlinkedTorrents(a, b qbittorrent.Torrent, cache map[string][]torrentFile) bool {
	filesA := cache[a.Hash]
	filesB := cache[b.Hash]

	for _, fileA := range filesA {
		pathA := filepath.Join(a.SavePath, fileA.Name)
		for _, fileB := range filesB {
			pathB := filepath.Join(b.SavePath, fileB.Name)
			hardlinked, err := utils.AreHardlinked(pathA, pathB)
			if err != nil {
				continue
			}
			if hardlinked {
				return true
			}
		}
	}

	return false
}

func (t *QBTask) createGroup(torrents []qbittorrent.Torrent) torrentGroup {
	group := torrentGroup{
		torrents:   torrents,
		minSeeding: torrents[0].SeedingTime,
	}

	for _, torrent := range torrents {
		group.popularity += torrent.NumComplete + torrent.NumIncomplete
		if torrent.Size > group.maxSize {
			group.maxSize = torrent.Size
		}
		if torrent.SeedingTime < group.minSeeding {
			group.minSeeding = torrent.SeedingTime
		}
	}

	return group
}

func (t *QBTask) sortGroupsByPriority(groups []torrentGroup) []torrentGroup {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].popularity != groups[j].popularity {
			return groups[i].popularity < groups[j].popularity
		}
		return groups[i].maxSize > groups[j].maxSize
	})
	return groups
}

func (t *QBTask) moveGroupToCold(ctx context.Context, group torrentGroup) error {
	minSeedingSeconds := int64(t.cfg.MinSeedingTime.Seconds())
	if group.minSeeding < minSeedingSeconds {
		t.logger.InfoContext(ctx, "group has not seeded long enough",
			"minSeeding", group.minSeeding,
			"required", minSeedingSeconds)
		return nil
	}

	for _, torrent := range group.torrents {
		if err := t.moveTorrentToCold(ctx, torrent); err != nil {
			return fmt.Errorf("moving torrent %s: %w", torrent.Hash, err)
		}
	}

	return nil
}

func (t *QBTask) moveTorrentToCold(ctx context.Context, torrent qbittorrent.Torrent) error {
	t.logger.InfoContext(ctx, "moving torrent to cold storage", "name", torrent.Name, "hash", torrent.Hash)

	if t.cfg.DryRun {
		t.logger.InfoContext(ctx, "dry run: would move torrent", "name", torrent.Name)
		return nil
	}

	if pauseErr := t.srcClient.PauseCtx(ctx, []string{torrent.Hash}); pauseErr != nil {
		return fmt.Errorf("pausing torrent: %w", pauseErr)
	}

	waitErr := utils.Until(ctx, func(_ context.Context) (bool, error) {
		srcTorrents, getErr := t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{torrent.Hash},
		})
		if getErr != nil {
			return false, getErr
		}
		if len(srcTorrents) == 0 {
			return false, errors.New("torrent not found")
		}
		return t.isPausedOrStalledState(srcTorrents[0].State), nil
	}, pollInterval, pollTimeout)
	if waitErr != nil {
		return fmt.Errorf("waiting for torrent to pause: %w", waitErr)
	}

	// Check if torrent already exists on destination.
	destTorrents, destErr := t.destClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{
		Hashes: []string{torrent.Hash},
	})
	if destErr != nil {
		return fmt.Errorf("checking destination: %w", destErr)
	}

	if len(destTorrents) > 0 {
		return t.handleExistingDestTorrent(ctx, torrent, destTorrents[0])
	}

	return t.addTorrentToDestination(ctx, torrent)
}

// handleExistingDestTorrent handles the case where the torrent already exists on the destination.
func (t *QBTask) handleExistingDestTorrent(
	ctx context.Context,
	srcTorrent qbittorrent.Torrent,
	destTorrent qbittorrent.Torrent,
) error {
	// Check for error states.
	if t.isErrorState(destTorrent.State) {
		t.logger.WarnContext(ctx, "destination torrent is in error state, resuming source",
			"name", srcTorrent.Name, "state", destTorrent.State)
		if resumeErr := t.srcClient.ResumeCtx(ctx, []string{srcTorrent.Hash}); resumeErr != nil {
			t.logger.ErrorContext(ctx, "failed to resume source torrent", "hash", srcTorrent.Hash, "error", resumeErr)
		}
		return fmt.Errorf("destination torrent in error state: %s", destTorrent.State)
	}

	// Wait for destination to complete hash check (Progress == 1.0).
	if readyErr := t.waitForDestinationReady(ctx, srcTorrent.Hash); readyErr != nil {
		t.logger.WarnContext(ctx, "destination torrent not ready, resuming source",
			"hash", srcTorrent.Hash, "error", readyErr)
		if resumeErr := t.srcClient.ResumeCtx(ctx, []string{srcTorrent.Hash}); resumeErr != nil {
			t.logger.ErrorContext(ctx, "failed to resume source torrent", "hash", srcTorrent.Hash, "error", resumeErr)
		}
		return fmt.Errorf("destination torrent not ready: %w", readyErr)
	}

	t.logger.InfoContext(
		ctx,
		"torrent verified on destination, deleting from source with files",
		"name",
		srcTorrent.Name,
	)
	if resumeErr := t.destClient.ResumeCtx(ctx, []string{srcTorrent.Hash}); resumeErr != nil {
		t.logger.WarnContext(ctx, "failed to resume destination torrent", "hash", srcTorrent.Hash, "error", resumeErr)
	}
	return t.srcClient.DeleteTorrentsCtx(ctx, []string{srcTorrent.Hash}, true)
}

// addTorrentToDestination exports the torrent and adds it to the destination client.
func (t *QBTask) addTorrentToDestination(ctx context.Context, torrent qbittorrent.Torrent) error {
	torrentData, exportErr := t.srcClient.ExportTorrentCtx(ctx, torrent.Hash)
	if exportErr != nil {
		return fmt.Errorf("exporting torrent: %w", exportErr)
	}

	// Calculate destination save path by replacing src prefix with dest prefix.
	savePath := t.destSavePath(torrent.SavePath)

	opts := map[string]string{
		"savepath":           savePath,
		"skip_checking":      "false",
		"paused":             "false",
		"root_folder":        "true",
		"contentLayout":      "Original",
		"autoTMM":            "false",
		"sequentialDownload": "false",
	}

	// Preserve category and tags.
	if torrent.Category != "" {
		opts["category"] = torrent.Category
	}
	if torrent.Tags != "" {
		opts["tags"] = torrent.Tags
	}

	addErr := t.destClient.AddTorrentFromMemoryCtx(ctx, torrentData, opts)
	if addErr != nil {
		if resumeErr := t.srcClient.ResumeCtx(ctx, []string{torrent.Hash}); resumeErr != nil {
			t.logger.ErrorContext(ctx, "failed to resume source torrent", "hash", torrent.Hash, "error", resumeErr)
		}
		return fmt.Errorf("adding torrent to destination: %w", addErr)
	}

	// Wait for destination torrent to be ready (hash check complete, no errors).
	if readyErr := t.waitForDestinationReady(ctx, torrent.Hash); readyErr != nil {
		t.logger.WarnContext(ctx, "destination torrent not ready, resuming source", "hash", torrent.Hash)
		if resumeErr := t.srcClient.ResumeCtx(ctx, []string{torrent.Hash}); resumeErr != nil {
			t.logger.ErrorContext(ctx, "failed to resume source torrent", "hash", torrent.Hash, "error", resumeErr)
		}
		return fmt.Errorf("waiting for destination torrent: %w", readyErr)
	}

	t.logger.InfoContext(ctx, "deleting torrent from source with files", "name", torrent.Name, "hash", torrent.Hash)
	return t.srcClient.DeleteTorrentsCtx(ctx, []string{torrent.Hash}, true)
}

// isErrorState returns true if the torrent state indicates an error.
func (t *QBTask) isErrorState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateError ||
		state == qbittorrent.TorrentStateMissingFiles
}

// isCheckingState returns true if the torrent is still checking files.
func (t *QBTask) isCheckingState(state qbittorrent.TorrentState) bool {
	return state == qbittorrent.TorrentStateCheckingUp ||
		state == qbittorrent.TorrentStateCheckingDl ||
		state == qbittorrent.TorrentStateCheckingResumeData
}

// isPausedOrStalledState returns true if the torrent is paused/stopped or stalled.
func (t *QBTask) isPausedOrStalledState(state qbittorrent.TorrentState) bool {
	switch state {
	case qbittorrent.TorrentStatePausedDl,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateStoppedUp,
		qbittorrent.TorrentStateStoppedDl:
		return true
	case qbittorrent.TorrentStateError,
		qbittorrent.TorrentStateMissingFiles,
		qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateCheckingUp,
		qbittorrent.TorrentStateForcedUp,
		qbittorrent.TorrentStateAllocating,
		qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateMetaDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateCheckingResumeData,
		qbittorrent.TorrentStateMoving,
		qbittorrent.TorrentStateUnknown:
		return false
	}
	return false
}

// isReadyState returns true if the torrent is in a seeding/upload state.
func (t *QBTask) isReadyState(state qbittorrent.TorrentState) bool {
	switch state {
	case qbittorrent.TorrentStateUploading,
		qbittorrent.TorrentStateStalledUp,
		qbittorrent.TorrentStateForcedUp,
		qbittorrent.TorrentStatePausedUp,
		qbittorrent.TorrentStateStoppedUp:
		return true
	case qbittorrent.TorrentStateError,
		qbittorrent.TorrentStateMissingFiles,
		qbittorrent.TorrentStateQueuedUp,
		qbittorrent.TorrentStateCheckingUp,
		qbittorrent.TorrentStateAllocating,
		qbittorrent.TorrentStateDownloading,
		qbittorrent.TorrentStateMetaDl,
		qbittorrent.TorrentStatePausedDl,
		qbittorrent.TorrentStateStoppedDl,
		qbittorrent.TorrentStateQueuedDl,
		qbittorrent.TorrentStateStalledDl,
		qbittorrent.TorrentStateCheckingDl,
		qbittorrent.TorrentStateForcedDl,
		qbittorrent.TorrentStateCheckingResumeData,
		qbittorrent.TorrentStateMoving,
		qbittorrent.TorrentStateUnknown:
		return false
	}
	return false
}

// waitForDestinationReady waits for the destination torrent to complete hash check
// and be in a healthy seeding state. Returns error if torrent enters error state
// or times out.
func (t *QBTask) waitForDestinationReady(ctx context.Context, hash string) error {
	return utils.Until(ctx, func(pollCtx context.Context) (bool, error) {
		torrents, err := t.destClient.GetTorrentsCtx(pollCtx, qbittorrent.TorrentFilterOptions{
			Hashes: []string{hash},
		})
		if err != nil {
			return false, err
		}
		if len(torrents) == 0 {
			return false, nil // Still waiting for torrent to appear.
		}

		destTorrent := torrents[0]

		// Check for error states - fail immediately.
		if t.isErrorState(destTorrent.State) {
			return false, fmt.Errorf("torrent entered error state: %s", destTorrent.State)
		}

		// Still checking - keep waiting.
		if t.isCheckingState(destTorrent.State) {
			return false, nil
		}

		// Verify hash check is complete: Progress must be 100%.
		// This is the key check - ensures all files are verified.
		if destTorrent.Progress < 1.0 {
			t.logger.DebugContext(pollCtx, "destination torrent still checking",
				"hash", hash, "progress", destTorrent.Progress, "state", destTorrent.State)
			return false, nil
		}

		// Torrent is ready - must be in a seeding/upload state with 100% progress.
		isReady := t.isReadyState(destTorrent.State)
		if isReady {
			t.logger.InfoContext(pollCtx, "destination torrent ready",
				"hash", hash, "progress", destTorrent.Progress, "state", destTorrent.State)
		}

		return isReady, nil
	}, pollInterval, pollTimeout)
}

// destSavePath calculates the destination save path by replacing the source prefix with dest prefix.
// Example: srcPath=/src, destPath=/dest, savePath=/src/Movies/Film
// Result: /dest/Movies/Film.
func (t *QBTask) destSavePath(savePath string) string {
	if strings.HasPrefix(savePath, t.cfg.SrcPath) {
		return filepath.Join(t.cfg.DestPath, t.relativePath(savePath))
	}
	// Fallback: just use the base name under dest path
	return filepath.Join(t.cfg.DestPath, filepath.Base(savePath))
}

func hasSyncedTag(tags string) bool {
	return slices.Contains(splitTags(tags), syncedTag)
}

func splitTags(tags string) []string {
	if tags == "" {
		return nil
	}
	parts := strings.Split(tags, ",")
	var result []string
	for _, part := range parts {
		tag := strings.TrimSpace(part)
		if tag != "" {
			result = append(result, tag)
		}
	}
	return result
}
