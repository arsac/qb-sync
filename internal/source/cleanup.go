package source

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/autobrr/go-qbittorrent"
	"golang.org/x/sync/errgroup"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// hardlinkScanConcurrency bounds how many torrents are probed at once when
// building the hardlink groups. Sized to overlap the per-file stats and the
// client-side cost of each file-information request without burying the
// PieceMonitor's piece-state polls - which run 8-wide against the same
// single-threaded qBittorrent WebUI - behind a deep request queue.
const hardlinkScanConcurrency = 8

// cleanupStats holds counters from a single source cleanup cycle for metrics and logging.
type cleanupStats struct {
	groupsEvaluated   int
	groupsDeleted     int
	groupsSkippedSeed int
	groupsFailed      int
	torrentsHandedOff int
	isDraining        bool
	freeSpaceBefore   int64
}

type torrentGroup struct {
	torrents   []qbittorrent.Torrent
	popularity int64
	maxSize    int64
	minSeeding int64
}

// unionFind implements a disjoint-set data structure for grouping torrents.
type unionFind struct {
	parent map[string]string
	rank   map[string]int
}

func newUnionFind() *unionFind {
	return &unionFind{
		parent: make(map[string]string),
		rank:   make(map[string]int),
	}
}

func (uf *unionFind) find(x string) string {
	if _, ok := uf.parent[x]; !ok {
		uf.parent[x] = x
	}
	if uf.parent[x] != x {
		uf.parent[x] = uf.find(uf.parent[x]) // path compression
	}
	return uf.parent[x]
}

func (uf *unionFind) union(x, y string) {
	rx, ry := uf.find(x), uf.find(y)
	if rx == ry {
		return
	}
	// union by rank
	switch {
	case uf.rank[rx] < uf.rank[ry]:
		uf.parent[rx] = ry
	case uf.rank[rx] > uf.rank[ry]:
		uf.parent[ry] = rx
	default:
		uf.parent[ry] = rx
		uf.rank[rx]++
	}
}

// maybeMoveToDest deletes torrents known to be complete on destination when space is low.
// During a drain (t.draining is true), bypasses space and seeding checks to
// evacuate all synced torrents.
func (t *QBTask) maybeMoveToDest(ctx context.Context) error {
	isDraining := t.draining.Load()

	if !isDraining {
		freeSpaceGB, err := t.getFreeSpaceGB(ctx)
		if err != nil {
			return fmt.Errorf("getting free space: %w", err)
		}
		t.logger.InfoContext(ctx, "checking free space",
			"freeGB", freeSpaceGB,
			"minGB", t.cfg.MinSpaceGB,
		)
		if freeSpaceGB >= t.cfg.MinSpaceGB {
			return nil
		}
	}

	torrents, err := t.fetchTorrentsCompletedOnDest(ctx)
	if err != nil {
		return fmt.Errorf("fetching torrents completed on destination: %w", err)
	}
	if len(torrents) == 0 {
		return nil
	}

	sortedGroups := sortGroupsByPriority(t.groupHardlinkedTorrents(ctx, torrents))

	var freeSpaceBefore int64
	if !isDraining {
		var spaceErr error
		freeSpaceBefore, spaceErr = t.getFreeSpaceGB(ctx)
		if spaceErr != nil {
			return fmt.Errorf("getting free space before cleanup: %w", spaceErr)
		}
	}

	stats := t.processGroups(ctx, sortedGroups, isDraining, freeSpaceBefore)
	t.recordCleanupMetrics(ctx, stats)
	return nil
}

// processGroups iterates the prioritized groups and performs the per-group
// handoff, returning a populated cleanupStats. It honors the seeding threshold
// (unless draining) and stops early once the estimated free space passes the
// configured minimum.
func (t *QBTask) processGroups(
	ctx context.Context,
	sortedGroups []torrentGroup,
	isDraining bool,
	freeSpaceBefore int64,
) cleanupStats {
	stats := cleanupStats{
		groupsEvaluated: len(sortedGroups),
		isDraining:      isDraining,
		freeSpaceBefore: freeSpaceBefore,
	}
	minSeedingSeconds := int64(t.cfg.MinSeedingTime.Seconds())

	// Track estimated freed bytes instead of re-querying after each deletion.
	// qBittorrent deletes files asynchronously, so getFreeSpaceGB would still
	// return the old value immediately after DeleteTorrentsCtx, causing the
	// loop to over-clean. maxSize is a reasonable estimate per group since
	// hardlinked files within a group are deduplicated by union-find.
	var estimatedFreedBytes int64

	for _, group := range sortedGroups {
		if !isDraining && group.minSeeding < minSeedingSeconds {
			t.logger.InfoContext(ctx, "group has not seeded long enough",
				"minSeeding", group.minSeeding,
				"required", minSeedingSeconds,
			)
			stats.groupsSkippedSeed++
			continue
		}

		handed, moveErr := t.deleteGroupFromHot(ctx, group)
		stats.torrentsHandedOff += handed
		if moveErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete group", "error", moveErr)
			stats.groupsFailed++
		} else {
			stats.groupsDeleted++
			estimatedFreedBytes += group.maxSize
		}

		estimatedFreeGB := freeSpaceBefore + estimatedFreedBytes/bytesPerGB
		if !isDraining && estimatedFreeGB >= t.cfg.MinSpaceGB {
			t.logger.InfoContext(ctx, "estimated free space reached threshold, stopping",
				"estimatedFreeGB", estimatedFreeGB,
				"minGB", t.cfg.MinSpaceGB,
			)
			break
		}
	}
	return stats
}

// recordCleanupMetrics records Prometheus counters and logs a summary for a cleanup cycle.
func (t *QBTask) recordCleanupMetrics(ctx context.Context, s cleanupStats) {
	metrics.SourceCleanupGroupsTotal.WithLabelValues(metrics.ResultSuccess).Add(float64(s.groupsDeleted))
	metrics.SourceCleanupGroupsTotal.WithLabelValues(metrics.ResultSkippedSeeding).Add(float64(s.groupsSkippedSeed))
	metrics.SourceCleanupGroupsTotal.WithLabelValues(metrics.ResultFailure).Add(float64(s.groupsFailed))
	metrics.SourceCleanupTorrentsHandedOffTotal.Add(float64(s.torrentsHandedOff))

	logAttrs := []any{
		"groupsEvaluated", s.groupsEvaluated,
		"groupsDeleted", s.groupsDeleted,
		"groupsSkippedSeeding", s.groupsSkippedSeed,
		"groupsFailed", s.groupsFailed,
		"torrentsHandedOff", s.torrentsHandedOff,
	}
	if !s.isDraining {
		freeSpaceAfter, spaceErr := t.getFreeSpaceGB(ctx)
		if spaceErr == nil {
			logAttrs = append(logAttrs, "spaceFreedGB", freeSpaceAfter-s.freeSpaceBefore)
		}
	}
	t.logger.InfoContext(ctx, "source cleanup cycle complete", logAttrs...)
}

// hasTag reports whether the comma-separated tag list contains the target tag.
func hasTag(tags, target string) bool {
	for tag := range strings.SplitSeq(tags, ",") {
		if strings.TrimSpace(tag) == target {
			return true
		}
	}
	return false
}

// fetchTorrentsCompletedOnDest returns source torrents that are known to be complete on destination.
func (t *QBTask) fetchTorrentsCompletedOnDest(ctx context.Context) ([]qbittorrent.Torrent, error) {
	allTorrents, err := t.cycleTorrentList(ctx)
	if err != nil {
		return nil, err
	}

	completedSnapshot := t.store.CompletedSnapshot()

	var result []qbittorrent.Torrent
	for _, torrent := range allTorrents {
		if _, ok := completedSnapshot[torrent.Hash]; !ok {
			continue
		}
		if !t.draining.Load() && t.cfg.ExcludeCleanupTag != "" &&
			hasTag(torrent.Tags, t.cfg.ExcludeCleanupTag) {
			continue
		}
		// Require SyncedTag if configured: protects re-downloaded torrents that
		// destination already has (they get the completed cache entry via
		// queryDestStatus but never went through markTorrentSynced).
		// applySyncedTag is called here so transient tag-application failures
		// self-heal on the next cleanup cycle rather than blocking indefinitely.
		if t.cfg.SyncedTag != "" && !hasTag(torrent.Tags, t.cfg.SyncedTag) {
			t.applySyncedTag(ctx, torrent.Hash)
			continue
		}
		result = append(result, torrent)
	}

	slices.SortFunc(result, func(a, b qbittorrent.Torrent) int {
		return cmp.Compare(a.Size, b.Size)
	})

	return result, nil
}

func (t *QBTask) groupHardlinkedTorrents(ctx context.Context, torrents []qbittorrent.Torrent) []torrentGroup {
	if len(torrents) == 0 {
		return nil
	}

	// Phase 1: stat each file, build (device,inode) -> []torrentHash map.
	fileKeyToHashes := t.collectFileOwners(ctx, torrents)

	// Phase 2: Union-find — for each file shared by multiple torrents, union their groups
	uf := newUnionFind()
	for _, hashes := range fileKeyToHashes {
		if len(hashes) < 2 { //nolint:mnd // minimum count for a shared file
			continue
		}
		for i := 1; i < len(hashes); i++ {
			uf.union(hashes[0], hashes[i])
		}
	}

	// Phase 3: Collect groups from union-find roots
	rootToTorrents := make(map[string][]qbittorrent.Torrent)
	for _, torrent := range torrents {
		root := uf.find(torrent.Hash)
		rootToTorrents[root] = append(rootToTorrents[root], torrent)
	}

	groups := make([]torrentGroup, 0, len(rootToTorrents))
	for _, group := range rootToTorrents {
		groups = append(groups, newTorrentGroup(group))
	}

	return groups
}

// fileKey identifies a file on disk. Keying on inode alone would falsely group
// files that share an inode number across different filesystems (e.g. separate
// volumes mounted under the same root).
type fileKey struct{ dev, ino uint64 }

// collectFileOwners maps each file's (device, inode) to the torrents holding it.
//
// Probing one torrent costs a file-information round-trip plus one stat per
// file, and every torrent known to be complete on destination is probed, so
// serially this put the whole synced library's worth of round-trip latency on
// the orchestrator goroutine - delaying the next cycle's finalization polls and
// new-torrent admission. The probes are independent, so they fan out; results
// are merged in torrent order afterwards so the grouping does not depend on
// which probe finished first.
func (t *QBTask) collectFileOwners(
	ctx context.Context,
	torrents []qbittorrent.Torrent,
) map[fileKey][]string {
	keysPerTorrent := make([][]fileKey, len(torrents))

	var g errgroup.Group
	g.SetLimit(hardlinkScanConcurrency)
	for i, torrent := range torrents {
		g.Go(func() error {
			keysPerTorrent[i] = t.torrentFileKeys(ctx, torrent)
			return nil
		})
	}
	_ = g.Wait() // torrentFileKeys reports its own failures and never returns one

	owners := make(map[fileKey][]string)
	for i, keys := range keysPerTorrent {
		for _, key := range keys {
			owners[key] = append(owners[key], torrents[i].Hash)
		}
	}
	return owners
}

// torrentFileKeys returns the (device, inode) of every file of one torrent that
// exists on disk. A torrent whose files cannot be listed contributes nothing,
// which leaves it in a group of its own.
func (t *QBTask) torrentFileKeys(ctx context.Context, torrent qbittorrent.Torrent) []fileKey {
	files, err := t.cycleFilesFor(ctx, torrent.Hash)
	if err != nil {
		t.logger.WarnContext(ctx, "failed to get files", "hash", torrent.Hash, "error", err)
		return nil
	}

	contentDir := t.source.ResolveContentDir(torrent.SavePath)
	keys := make([]fileKey, 0, len(files))
	for _, f := range files {
		dev, ino, statErr := utils.GetFileID(filepath.Join(contentDir, f.Name))
		if statErr != nil || ino == 0 {
			continue
		}
		keys = append(keys, fileKey{dev: dev, ino: ino})
	}
	return keys
}

func newTorrentGroup(torrents []qbittorrent.Torrent) torrentGroup {
	if len(torrents) == 0 {
		return torrentGroup{}
	}

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

func sortGroupsByPriority(groups []torrentGroup) []torrentGroup {
	slices.SortFunc(groups, func(a, b torrentGroup) int {
		if a.popularity != b.popularity {
			return cmp.Compare(a.popularity, b.popularity)
		}
		// Longest seeded first (already contributed most to the swarm)
		if a.minSeeding != b.minSeeding {
			return cmp.Compare(b.minSeeding, a.minSeeding)
		}
		// Largest first (reclaim more space)
		return cmp.Compare(b.maxSize, a.maxSize)
	})
	return groups
}

// deleteGroupFromHot deletes a group of torrents complete on destination from source storage.
// Returns the number of torrents successfully handed off.
// Uses a 3-step handoff to prevent dual seeding:
//  1. Stop on source → fails? skip torrent (source keeps seeding, destination stays stopped)
//  2. Start on destination → fails? resume on source (rollback, nobody left seeding otherwise)
//  3. Delete from source → fails? log it (destination is seeding, next cycle retries)
func (t *QBTask) deleteGroupFromHot(ctx context.Context, group torrentGroup) (int, error) {
	var handed, failed int
	for _, torrent := range group.torrents {
		t.logger.InfoContext(ctx, "handing off torrent from source to destination",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)

		if t.cfg.DryRun {
			continue
		}

		if stopErr := t.srcClient.StopCtx(ctx, []string{torrent.Hash}); stopErr != nil {
			metrics.TorrentStopErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
			t.logger.WarnContext(ctx, "failed to stop torrent on source, skipping handoff",
				"hash", torrent.Hash, "error", stopErr)
			failed++
			continue
		}

		if startErr := t.grpcDest.StartTorrent(ctx, torrent.Hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.ErrorContext(ctx, "failed to start torrent on destination, resuming on source",
				"hash", torrent.Hash, "error", startErr)
			if resumeErr := t.srcClient.ResumeCtx(ctx, []string{torrent.Hash}); resumeErr != nil {
				metrics.TorrentResumeErrorsTotal.WithLabelValues(metrics.ModeSource).Inc()
				t.logger.WarnContext(ctx, "failed to resume torrent on source after destination start failure",
					"hash", torrent.Hash, "error", resumeErr)
			}
			failed++
			continue
		}

		if deleteErr := t.srcClient.DeleteTorrentsCtx(ctx, []string{torrent.Hash}, true); deleteErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete torrent from source (destination is seeding, will retry)",
				"hash", torrent.Hash, "error", deleteErr)
		}

		handed++
	}

	if failed > 0 {
		return handed, fmt.Errorf("%d of %d torrents failed handoff", failed, len(group.torrents))
	}
	return handed, nil
}

func (t *QBTask) getFreeSpaceGB(ctx context.Context) (int64, error) {
	freeBytes, err := t.srcClient.GetFreeSpaceOnDiskCtx(ctx)
	if err != nil {
		return 0, err
	}
	return freeBytes / bytesPerGB, nil
}
