# qb-sync

Copies torrent data between two qBittorrent instances by streaming pieces from a
source to a destination as they become available, then handing over seeding
responsibility. This glossary fixes the vocabulary used across the codebase,
logs, metrics, and docs.

## Language

### Roles

**Source**:
The qBittorrent instance a torrent is copied from, together with the qb-sync
process that reads its data.
_Avoid_: hot, primary, origin

**Destination**:
The qBittorrent instance a torrent is copied to, together with the qb-sync
server that receives and verifies its data.
_Avoid_: cold, secondary, target

### Sync lifecycle

**Tracked**:
A torrent the source is actively streaming pieces for.
_Avoid_: active, in-flight, syncing

**Selected file**:
A file with non-zero priority in source qBittorrent, and therefore one whose
pieces are streamed. Everything else is deselected and never read from disk.
_Avoid_: included file, wanted file

**Advance**:
An increase in a torrent's streamed piece count.
_Avoid_: progress, movement

**Finalization**:
The destination-side process that turns a fully streamed torrent into one
seeding in destination qBittorrent.
_Avoid_: completion, commit, publish

**Handoff**:
The transfer of seeding responsibility from source to destination.
_Avoid_: migration, move, cutover

**Drain**:
Evacuation of fully synced torrents from the source ahead of shutdown.
_Avoid_: eviction, shutdown sync

### Failure and recovery

**Stall**:
A condition in which a torrent has pieces available on the source but is not
advancing. Distinct from a source that is merely slow to download.
_Avoid_: stuck, wedged, hung, frozen

**Failure streak**:
An unbroken run of finalization failures for a single torrent, measured from
the first failure rather than counted in attempts.
_Avoid_: retry count, attempt count

**Guard**:
The minimum continuous duration a fault must persist before it is treated as
terminal.
_Avoid_: timeout, grace period, deadline

**Quarantine**:
The state a torrent enters when a fault outlasts its guard. Quarantined
torrents are excluded from tracking until released.
_Avoid_: failed, dead, blacklisted, given up

**sync-failed tag**:
The representation of [[Quarantine]] on the source torrent in qBittorrent. The
tag is how the state is recorded, not the state itself.

**Release**:
Removal of the sync-failed tag, returning a quarantined torrent to
eligibility.
_Avoid_: retry, unblock, reset, requeue

### Destination storage

**Orphan**:
An unfinalized torrent on the destination that no source has contacted within
the orphan timeout.
_Avoid_: abandoned transfer, stale torrent, dead transfer

**Pre-existing file**:
A destination file qb-sync adopted rather than wrote, because it was already on
disk at the expected size. Owned by the operator, never deleted by qb-sync.
_Avoid_: existing file, operator file, foreign file
