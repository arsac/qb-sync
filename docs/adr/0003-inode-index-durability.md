# The inode index's durable record is a trimmed per-torrent .meta

The destination maps a source file's device+inode to its local copy so a later
cross-seeded torrent hardlinks instead of re-transferring the bytes. That index
lives in memory and is rebuilt at startup by scanning every per-torrent `.meta`.
`markFinalized` deleted `.meta`, so the rebuild only ever saw in-flight torrents
and every restart silently dropped every previously synced file. It now keeps
`.meta`, trimmed of the piece hashes and the `.torrent` blob, which are dead once
verification has passed. Registration at finalization admits on the same
question the index answers - does this server hold the bytes for that source
file - rather than on `skipForWriteData`, which excluded files adopted from
pre-existing data and so kept them out of the index even within one session.

This restores a documented intent rather than establishing a new one. Commit
72ee118, "eliminate .inode_map.json, rebuild inode registry from .meta at
startup", states the rebuild scans ".meta files (finalized and in-flight)"; the
`markFinalized` in that same tree already deleted `.meta`. The replacement for
the old global index never covered the finalized case.

## Considered Options

- **A single global index file.** This is what `.inode_map.json` was, and
  `recovery.go` still deletes the legacy file on first startup. Rejected, but
  not for the reason its removal suggests: it died because `Inodes().Save()`
  marshalled and rewrote the entire map on every per-file registration, an O(N)
  NFS write per file and quadratic across a library import. The write path sank
  it, not the storage location. Reinstating it with a fixed write path would
  still buy little - an entry cannot be trusted without confirming the file
  still exists, and that per-file `stat` dominates the rebuild, so a single file
  saves M small concurrent reads and saves nothing on the stats.
- **Keeping `.meta` whole.** Rejected as unnecessary: `piece_hashes` is a
  `repeated string` of hex digests, so a 50 GB torrent at 4 MB pieces carries
  roughly 500 KB of hashes plus the `.torrent` blob, against a file list of a
  few KB. Only `collectInodeEntries` reads a finalized `.meta`, and it touches
  `save_sub_path` and `files` alone.
- **A separate index record written alongside the marker.** Rejected: a second
  format and a second reader for data `.meta` already carries, with two writers
  to keep in agreement. Disagreement between two writers is precisely the bug
  being fixed here.
- **Retiring finalized metadata once destination qB confirms the torrent
  seeding.** Implemented, then reverted before shipping. Its trigger is the
  strongest available evidence the record should be kept: qB holding the torrent
  means the bytes are present, which is exactly when the entry is worth most.

## Consequences

- **The index record's lifetime is the file's, not the torrent's.** It stays
  true for as long as the bytes are on this server, which outlasts the source's
  interest in the torrent by an unbounded margin. Anything that deletes metadata
  on a torrent-lifecycle event - a handoff, a source decommission - deletes a
  live hardlink source. Retirement is therefore keyed on every file the record
  describes being confirmed absent, and lives in the inode cleaner beside
  `CleanupStale`, which is the in-memory half of the same computation.
- **Retirement fails closed on anything but `ErrNotExist`.** An `EIO` or
  `ESTALE` from a mount that blipped means "cannot tell". Reading that as "gone"
  would retire the whole library in a single pass.
- **The marker is written before `.meta` is trimmed.** Trimming strips the piece
  hashes, and a torrent recovered without them can never finalize:
  `verifyFinalizedPieces` refuses outright, and `resumeTorrent` adopts a
  resuming source's torrent file but not its hashes, so nothing restores them
  short of a re-sync. Ordering the marker first leaves only two states a crash
  can produce - full `.meta` with no marker, which recovers and re-streams as it
  always did, and marker present, which is finalized whether or not the trim
  landed. `.meta` is never deleted and rewritten, because `savePersistedMeta` is
  atomic and the delete would only open a window with no record at all.
- **Adopted pre-existing files are safe to publish as link sources because of
  where registration runs.** The disk stage calls it only after
  `verifyFinalizedPieces`, so those bytes have been read back and hashed.
  Admitting them any earlier would offer unverified content to every later
  cross-seed.
- **The startup scan now reads a `.meta` for every torrent ever synced, not just
  in-flight ones.** That cost is inherent to having the index at all, and it is
  paid on the synchronous path ahead of the readiness signal. A missing entry is
  a performance miss and never a correctness failure, so moving the rebuild off
  that path is available if the scan becomes a startup constraint.
- **`cleanupForResync` deletes `.meta`, which also drops the index records for
  that torrent's files.** Correct, since the selection changed, and they
  re-register at the next finalization - but a restart inside that window loses
  them.
- **The mapping is naturally replicated.** Every cross-seeded torrent's `.meta`
  records the same source device+inode against its own path, so losing one
  directory does not lose the mapping. A single global index had no equivalent.

## Citation

Restores the behaviour asserted by commit 72ee118. Go standard library offers no
comparable file-identity value usable as a map key - `os.SameFile` compares two
`FileInfo` values - so `FileID{Dev, Ino}` is required rather than hand-rolled.
No dependency in `go.mod` provides a persistent index primitive.
