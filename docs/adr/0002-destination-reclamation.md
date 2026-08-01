# Destination reclamation is based on source contact, and deletes only .partial files

The orphan cleaner could not fire for the case its documentation describes. It
skipped any torrent present in the in-memory store, and recovery repopulates
that store from every non-finalized metadata directory at startup, so an
abandoned transfer was permanently shielded and its data never reclaimed.
Orphan eligibility is now based on time since the last source contact for that
torrent rather than store membership, and reclamation deletes only `.partial`
files.

## Considered Options

- **`.state` file mtime as the activity signal.** Rejected: mtime is a flush
  artifact, not activity. A healthy fully streamed torrent has a frozen mtime
  for hours while it waits through the finalization queue and a congestion
  streak, and the configured orphan timeout may be as low as one hour.
- **A recovery-time filter.** Skip repopulating the store for stale metadata
  directories. Rejected: only reclaims on restart, so a long-lived destination
  still accumulates abandoned transfers indefinitely.
- **Persisting file provenance.** Add `pre_existing` and `hardlinked` to the
  persisted metadata so orphan cleanup could apply the same deletion predicate
  as abort. Rejected as unnecessary: the path suffix already encodes it.

## Consequences

- **Deleting only `.partial` paths makes it structurally impossible for
  reclamation to destroy operator data.** Every file qb-sync writes lives at a
  `.partial` path until finalization renames it, so on an unfinalized torrent a
  bare path is always a pre-existing file, a hardlink, or a deselected file.
  Previously `deleteOrphanFiles` removed bare paths unconditionally while
  `AbortTorrent` explicitly preserved pre-existing files. That asymmetry was
  latent only because the cleaner almost never fired; making the cleaner work
  would have activated it as data loss on the operator's own files.
- **`AbortTorrent` keeps its richer guard.** It holds the hardlink results in
  memory and can safely delete a hardlinked bare path, which drops a link
  rather than data. Orphan cleanup does not have that information, so it is
  deliberately more conservative. The asymmetry is now intentional and stated,
  rather than accidental.
- **Files left bare by a crashed mid-finalization rename are not reclaimed.**
  They are adopted as pre-existing on any retry, so they are reusable rather
  than garbage. This is the accepted cost of the rule above.
- **Quarantine releases destination state without deleting bytes.** The source
  sends an abort with file deletion disabled, so a release within the orphan
  timeout resumes from the persisted piece bitmap instead of re-streaming the
  whole torrent. If that abort fails, the tag still applies and the cleaner is
  the backstop.
- **The contact stamp lives in the store's `Get` accessors, not at each RPC
  entry point.** Every request path resolves state through them, so a new RPC
  gets the stamp for free rather than having to remember a convention. The
  failure modes are asymmetric and decide the direction: a missed stamp makes a
  live transfer look abandoned and deletes its data, while a scanner that
  forgets to bypass the stamp only leaks disk. Orphan scanning uses a separate
  non-stamping accessor, because stamping there would refresh the very
  timestamp being tested.
- **Removing store membership as the gate is not sufficient on its own.** The
  cleanup registration also refused entries present in the store, so the shield
  existed at two layers. Fixing only the predicate would look correct and still
  reclaim nothing.
- **Staleness is re-tested while the store lock is held.** The refusal that
  formed the shield was also protecting a race: a source can resume a torrent
  between the scan judging it stale and the cleanup deleting files. Testing
  staleness outside the lock and then removing would delete files from under an
  active transfer, so removal and re-test are one atomic operation.
- **Torrents mid-finalization are never reclaimed.** Verifying a large torrent
  runs for a long time with no source contact, and reclaiming it would delete
  the data being verified.
