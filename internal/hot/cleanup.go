package hot

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/autobrr/go-qbittorrent"

	"github.com/arsac/qb-sync/internal/metrics"
	"github.com/arsac/qb-sync/internal/utils"
)

// cleanupStats holds counters from a single hot cleanup cycle for metrics and logging.
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

// maybeMoveToCold deletes torrents known to be complete on cold when space is low.
// During a drain (t.draining is true), bypasses space and seeding checks to
// evacuate all synced torrents.
//
//nolint:gocognit
func (t *QBTask) maybeMoveToCold(ctx context.Context) error {
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

	torrents, err := t.fetchTorrentsCompletedOnCold(ctx)
	if err != nil {
		return fmt.Errorf("fetching torrents completed on cold: %w", err)
	}

	if len(torrents) == 0 {
		return nil
	}

	groups := t.groupHardlinkedTorrents(ctx, torrents)
	sortedGroups := sortGroupsByPriority(groups)

	var freeSpaceBefore int64
	if !isDraining {
		var spaceErr error
		freeSpaceBefore, spaceErr = t.getFreeSpaceGB(ctx)
		if spaceErr != nil {
			return fmt.Errorf("getting free space before cleanup: %w", spaceErr)
		}
	}

	var groupsDeleted, groupsSkippedSeeding, groupsFailed, torrentsHandedOff int
	minSeedingSeconds := int64(t.cfg.MinSeedingTime.Seconds())

	for _, group := range sortedGroups {
		if !isDraining && group.minSeeding < minSeedingSeconds {
			t.logger.InfoContext(ctx, "group has not seeded long enough",
				"minSeeding", group.minSeeding,
				"required", minSeedingSeconds,
			)
			groupsSkippedSeeding++
			continue
		}

		handed, moveErr := t.deleteGroupFromHot(ctx, group)
		torrentsHandedOff += handed
		if moveErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete group", "error", moveErr)
			groupsFailed++
		} else {
			groupsDeleted++
		}

		if !isDraining {
			currentSpaceGB, spaceErr := t.getFreeSpaceGB(ctx)
			if spaceErr != nil {
				return fmt.Errorf("getting free space: %w", spaceErr)
			}
			if currentSpaceGB >= t.cfg.MinSpaceGB {
				t.logger.InfoContext(ctx, "reached minimum free space, stopping")
				break
			}
		}
	}

	t.recordCleanupMetrics(ctx, cleanupStats{
		groupsEvaluated:   len(sortedGroups),
		groupsDeleted:     groupsDeleted,
		groupsSkippedSeed: groupsSkippedSeeding,
		groupsFailed:      groupsFailed,
		torrentsHandedOff: torrentsHandedOff,
		isDraining:        isDraining,
		freeSpaceBefore:   freeSpaceBefore,
	})

	return nil
}

// recordCleanupMetrics records Prometheus counters and logs a summary for a cleanup cycle.
func (t *QBTask) recordCleanupMetrics(ctx context.Context, s cleanupStats) {
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultSuccess).Add(float64(s.groupsDeleted))
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultSkippedSeeding).Add(float64(s.groupsSkippedSeed))
	metrics.HotCleanupGroupsTotal.WithLabelValues(metrics.ResultFailure).Add(float64(s.groupsFailed))
	metrics.HotCleanupTorrentsHandedOffTotal.Add(float64(s.torrentsHandedOff))

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
	t.logger.InfoContext(ctx, "hot cleanup cycle complete", logAttrs...)
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

// fetchTorrentsCompletedOnCold returns hot torrents that are known to be complete on cold.
func (t *QBTask) fetchTorrentsCompletedOnCold(ctx context.Context) ([]qbittorrent.Torrent, error) {
	allTorrents := t.cycleTorrents
	if allTorrents != nil {
		metrics.CycleCacheHitsTotal.Inc()
	} else {
		var err error
		allTorrents, err = t.srcClient.GetTorrentsCtx(ctx, qbittorrent.TorrentFilterOptions{})
		if err != nil {
			return nil, err
		}
	}

	t.completedMu.RLock()
	defer t.completedMu.RUnlock()

	var result []qbittorrent.Torrent
	for _, torrent := range allTorrents {
		if !t.completedOnCold[torrent.Hash] {
			continue
		}
		if !t.draining.Load() && t.cfg.ExcludeCleanupTag != "" &&
			hasTag(torrent.Tags, t.cfg.ExcludeCleanupTag) {
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

	// Phase 1: stat each file, build inode -> []torrentHash map
	inodeToHashes := make(map[uint64][]string)
	for _, torrent := range torrents {
		filesPtr, err := t.srcClient.GetFilesInformationCtx(ctx, torrent.Hash)
		if err != nil {
			t.logger.WarnContext(ctx, "failed to get files", "hash", torrent.Hash, "error", err)
			continue
		}
		if filesPtr == nil {
			continue
		}

		contentDir := t.source.ResolveContentDir(torrent.SavePath)
		for _, f := range *filesPtr {
			path := filepath.Join(contentDir, f.Name)
			inode, statErr := utils.GetInode(path)
			if statErr != nil || inode == 0 {
				continue
			}
			inodeToHashes[inode] = append(inodeToHashes[inode], torrent.Hash)
		}
	}

	// Phase 2: Union-find — for each inode shared by multiple torrents, union their groups
	uf := newUnionFind()
	for _, hashes := range inodeToHashes {
		if len(hashes) < 2 { //nolint:mnd // minimum count for a shared inode
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

func newTorrentGroup(torrents []qbittorrent.Torrent) torrentGroup {
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

// deleteGroupFromHot deletes a group of torrents complete on cold from hot storage.
// Returns the number of torrents successfully handed off.
// Uses a 3-step handoff to prevent dual seeding:
//  1. Stop on hot → fails? skip torrent (hot keeps seeding, cold stays stopped)
//  2. Start on cold → fails? resume on hot (rollback, nobody left seeding otherwise)
//  3. Delete from hot → fails? log it (cold is seeding, next cycle retries)
func (t *QBTask) deleteGroupFromHot(ctx context.Context, group torrentGroup) (int, error) {
	var handed, failed int
	for _, torrent := range group.torrents {
		t.logger.InfoContext(ctx, "handing off torrent from hot to cold",
			"name", torrent.Name,
			"hash", torrent.Hash,
		)

		if t.cfg.DryRun {
			continue
		}

		if stopErr := t.srcClient.StopCtx(ctx, []string{torrent.Hash}); stopErr != nil {
			metrics.TorrentStopErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
			t.logger.WarnContext(ctx, "failed to stop torrent on hot, skipping handoff",
				"hash", torrent.Hash, "error", stopErr)
			failed++
			continue
		}

		if startErr := t.grpcDest.StartTorrent(ctx, torrent.Hash, t.cfg.SourceRemovedTag); startErr != nil {
			t.logger.ErrorContext(ctx, "failed to start torrent on cold, resuming on hot",
				"hash", torrent.Hash, "error", startErr)
			if resumeErr := t.srcClient.ResumeCtx(ctx, []string{torrent.Hash}); resumeErr != nil {
				metrics.TorrentResumeErrorsTotal.WithLabelValues(metrics.ModeHot).Inc()
				t.logger.WarnContext(ctx, "failed to resume torrent on hot after cold start failure",
					"hash", torrent.Hash, "error", resumeErr)
			}
			failed++
			continue
		}

		if deleteErr := t.srcClient.DeleteTorrentsCtx(ctx, []string{torrent.Hash}, true); deleteErr != nil {
			t.logger.ErrorContext(ctx, "failed to delete torrent from hot (cold is seeding, will retry)",
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
