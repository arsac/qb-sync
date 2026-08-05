# Quarantine timing and destination reclamation

## Problem Statement

Torrents that hit trouble during a sync end up in one of two bad states, and an
operator has no good way out of either.

The first is premature quarantine. A torrent is quarantined after three
consecutive finalization failures, and because the retry backoff is always
shorter than the orchestrator's cycle interval, those three attempts land about
ninety seconds apart. Any fault lasting two minutes, a destination qBittorrent
restart, a brief storage stall, a transient full disk, permanently quarantines
every torrent that happened to be finalizing. The only way back is a human
opening qBittorrent and removing the tag from each one by hand. The fault that
caused it was over minutes ago.

The second is worse because it is silent. When a piece cannot be read from the
source at all, the torrent never stops being tracked and never reaches
finalization, so it never reaches quarantine either. It stays tracked forever,
retrying the same unreadable piece indefinitely. There is no log that says it
has given up, because it has not given up. An operator only notices by watching
a progress gauge stay flat.

Underneath both, the destination never reclaims disk from a sync that stopped.
Quarantining a torrent tells the destination nothing, so its partial data stays
on disk. The periodic reclamation that is supposed to catch this cannot fire,
because it skips any torrent the server is holding in memory, and the server
reloads every unfinished torrent into memory when it starts. Partial data from
quarantined torrents, crashed sources, and decommissioned sources accumulates
with no path to being cleaned up.

## Solution

Quarantine becomes something a torrent has to earn. Instead of counting three
failures, we require that the trouble has persisted continuously for longer
than a configurable guard, four hours by default. A destination restart or a
storage blip is retried and recovered from, and the torrent is never
quarantined. A torrent that is genuinely broken still gets quarantined, and by
then the tag means what an operator expects it to mean.

Stalls get the same treatment. A torrent that has pieces waiting on the source
but is not advancing is now detected and, if it stays that way past the guard,
quarantined through exactly the same path. The wedged-forever state stops
existing. Crucially this is distinguished from a source that is simply slow to
download, which is not a fault and is never quarantined.

Quarantining now also tells the destination to let go of the torrent while
keeping its bytes. If an operator releases the torrent within the reclamation
window, the sync picks up from the pieces already transferred rather than
starting over. If nobody releases it, the destination reclaims the disk on its
own.

Reclamation is fixed so it can actually run, by judging a torrent on how long
it has been since a source last asked about it rather than on whether the
server happens to be holding it in memory. And it is made safe: reclamation now
deletes only files this system wrote, so it can never remove a pre-existing
file an operator had on disk before the sync started.

## User Stories

1. As an operator, I want a brief destination outage to not quarantine my
   torrents, so that I do not have to manually release dozens of torrents after
   every restart.
2. As an operator, I want a torrent that fails continuously for hours to be
   quarantined, so that the tag still tells me which torrents genuinely need my
   attention.
3. As an operator, I want to tune how long a fault must persist before
   quarantine, so that I can match it to how reliable my own storage and
   network are.
4. As an operator, I want the default guard to be conservative, so that I get
   sensible behaviour without tuning anything.
5. As an operator, I want a source restart to not reset the clock on a
   long-running fault, so that a source that restarts frequently still
   quarantines genuinely broken torrents.
6. As an operator, I want a torrent whose data cannot be read from the source to
   eventually be quarantined, so that it stops consuming sync capacity forever.
7. As an operator, I want a torrent that is merely slow to download on the
   source to never be quarantined, so that I do not lose torrents that are
   waiting on peers.
8. As an operator, I want a stalled torrent that starts advancing again to be
   forgiven, so that a temporary interruption does not accumulate toward
   quarantine.
9. As an operator, I want repeated verification failures to still reach
   quarantine even though the torrent re-streams pieces between attempts, so
   that genuinely corrupt data does not retry forever.
10. As an operator, I want a torrent under congestion to keep retrying without
    penalty, so that a busy destination does not quarantine work that would
    have succeeded.
11. As an operator, I want retries to slow down as a fault persists, so that one
    broken torrent cannot saturate my destination and starve every other
    torrent's finalization.
12. As an operator, I want the retry pacing to scale with the guard, so that
    there is only one number to reason about.
13. As an operator, I want to see how many torrents are currently quarantined,
    so that I can alert on the standing population rather than only on the rate
    of new failures.
14. As an operator, I want to see how many torrents are currently stalled, so
    that I can catch a systemic problem before anything gets quarantined.
15. As an operator, I want to see how many torrents are being skipped and why,
    so that a torrent broken on the source does not silently never sync.
16. As an operator, I want these counts to not add per-torrent metric series, so
    that my monitoring cost does not scale with library size.
17. As an operator, I want quarantining a torrent to release the destination's
    hold on it, so that the destination can eventually reclaim the disk.
18. As an operator, I want quarantining a torrent to preserve the data already
    transferred, so that releasing it does not mean re-copying everything.
19. As an operator, I want releasing a quarantined torrent to resume from what
    was already transferred, so that a four-hour outage does not cost me a full
    re-copy of a large torrent.
20. As an operator, I want the destination to reclaim disk from a sync whose
    source has crashed, so that a dead source does not silently fill my
    destination.
21. As an operator, I want the destination to reclaim disk from a sync whose
    source was decommissioned, so that retiring a source does not strand its
    partial transfers forever.
22. As an operator, I want reclamation to keep working across destination
    restarts, so that a server that restarts regularly still reclaims disk.
23. As an operator, I want reclamation to never delete a pre-existing file I put
    on disk myself, so that adopting my existing library into a sync cannot
    destroy it.
24. As an operator, I want reclamation to never delete a hardlinked file, so
    that cleaning up one torrent cannot disturb another that shares its data.
25. As an operator, I want a torrent waiting through a long finalization queue
    to never be mistaken for abandoned, so that a congested destination does not
    delete data it is about to finalize.
26. As an operator, I want a torrent in the middle of verification to never be
    mistaken for abandoned, so that a slow verification of a large torrent is
    not interrupted by its own data being deleted.
27. As an operator, I want a torrent that destination qBittorrent already owns
    to never have its files deleted by reclamation, so that a seeding torrent
    never loses its data.
28. As a developer, I want one place that holds a torrent's sync state, so that
    a lifecycle change does not mean remembering to update several maps.
29. As a developer, I want one way to tear down a torrent's state, so that new
    lifecycle paths cannot forget part of the cleanup.
30. As a developer, I want the guard and its pacing to be exercisable
    end to end, so that the timing behaviour is tested rather than assumed.
31. As a developer, I want the two bugs reproduced before they are fixed, so
    that the fix is verified against the real failure rather than against my
    reading of the code.

## Implementation Decisions

### Quarantine is duration-based

Quarantine requires that a fault has persisted continuously past a guard, not
that a number of attempts have failed. Two conditions can reach it, and they
are modelled differently because they are different things.

A failure streak requires both a minimum failure count and that the elapsed
time since the first failure exceeds the guard. A stall requires only that the
elapsed time since the stall began exceeds the guard, with no attempt counting,
because counting attempts is meaningless for a continuous condition.

The guard is operator-configurable with a four hour default. The existing
congestion guard is deliberately left as a separate, longer value: congestion
is a destination-wide condition that legitimately lasts hours, whereas a
failure streak is a per-torrent fault.

### Retry pacing is derived from the guard

The finalization retry backoff cap is computed from the guard rather than being
an independent constant:

```
backoffCap = min(30m, guard/8)
```

This keeps roughly a dozen attempts inside any guard window. It is required for
correctness, not tidiness: without it, the guard window permits hundreds of
finalization attempts, and the disk-error retry path re-verifies every piece
while holding the destination's disk-stage serialization point, starving all
other finalization. Deriving it from the guard also means one knob shrinks the
whole mechanism for testing, rather than exposing an operator-facing knob that
exists only for tests.

### Stall detection

A torrent is stalled when it has pieces available on the source that have not
been streamed, is not otherwise complete, and has not advanced. The piece
monitor exposes both the count of available-but-unstreamed pieces and the time
of the last advance; the orchestrator makes the judgement.

The available-piece condition is what distinguishes a stall from a source that
is merely slow to download. A source waiting on peers has nothing available, so
it can never be judged stalled.

The stall clock is cleared by any advance. The failure clock is not, because
the verification-failure path re-streams pieces between attempts, and clearing
on advance would mean repeated verification failures never reach quarantine.

### Consolidated source state

The three separate per-torrent state holders on the source, tracked torrents,
completion fingerprints, and retry backoff, are collapsed into a single
per-torrent record behind one lock. The two new clocks live there rather than
becoming a fourth holder.

Persistence stays split across two files: the existing completion cache keeps
its current format and a new sidecar holds the streak clocks. One store, two
save targets. This avoids a migration on a file already in production and keeps
a downgrade safe.

Streak clocks must be persisted. With in-memory state, a source restarting more
often than the guard could never quarantine anything, which would silently
deliver "never terminal" behaviour while the configuration claims otherwise.
The sidecar is written on state transitions rather than every cycle, because
the stall condition is set and cleared frequently during normal congestion.

Teardown is unified behind a single operation. There are currently five
different combinations of the same six cleanup steps across six call sites, and
this work would otherwise add a sixth variant.

### Quarantine releases destination state without deleting data

Quarantining sends the destination an abort with file deletion disabled. The
destination drops its in-memory hold and keeps the bytes, so a release within
the reclamation window resumes from the persisted piece bitmap. If the abort
call fails, the tag is still applied and reclamation is the backstop.

### Reclamation eligibility

A destination torrent is eligible for reclamation when it is not finalized, is
not currently finalizing, its owning qBittorrent check passes, and no source has
contacted it within the reclamation timeout.

Contact time is recorded on the torrent's state and stamped by every request
that names its hash. Any future request that names a hash must stamp it, or its
torrents will look abandoned.

This replaces the previous test, which skipped any torrent held in memory.
Because startup reloads every unfinished torrent into memory, that test could
never pass for the case it was written for.

Persisted state file modification time was considered and rejected as the
activity signal. It records flushes, not activity: a healthy fully-streamed
torrent has a frozen modification time for hours while it waits through the
finalization queue and a congestion streak, and the configured timeout can be
set as low as an hour.

### Reclamation deletes only what we wrote

Reclamation removes only partial files and the metadata directory. It never
removes a file at its final path.

Every file this system writes lives at a partial path until finalization
renames it, so on an unfinalized torrent a file at its final path is always a
pre-existing file, a hardlink, or a deselected file. None of those are ours to
delete. This makes it structurally impossible for reclamation to destroy
operator data, without needing to record file provenance in the persisted
metadata.

The abort path keeps its richer deletion rules, because it holds the hardlink
results in memory and can safely remove a hardlinked file, which drops a link
rather than data. Reclamation does not have that information and is therefore
deliberately more conservative. The asymmetry is intentional and documented;
previously it existed by accident and in the dangerous direction.

Files left at their final path by a crashed mid-finalization rename are not
reclaimed. They are adopted as pre-existing on any retry, so they are reusable
rather than garbage.

### Observability

Three aggregate gauges are added: the count of quarantined torrents, the count
of stalled torrents, and the count of skipped torrents broken down by the
reason they were skipped. The skip reasons close a blind spot where a torrent
broken on the source is silently never synced.

All three are aggregates. The existing per-torrent gauges already carry both
hash and name labels, so no new per-torrent series are introduced.

## Testing Decisions

A good test here asserts what an operator or a source would observe: whether a
torrent ends up quarantined, whether data survives on disk, whether a release
resumes or re-copies. It does not assert which internal map holds a timestamp
or how many times a private method was called. That matters more than usual for
this work, because the state consolidation deliberately changes the internal
shape while preserving every externally visible behaviour, and tests coupled to
the old shape would have to be rewritten for a change that alters nothing an
operator can see.

### Seams

No new seams are introduced. Both seams used already exist.

The primary seam is the end-to-end environment, which runs real source and
destination qBittorrent instances against both halves of the system. All
timing is reachable through configuration that already flows through the
existing setup options: the guard through the source config option pattern, and
the reclamation timeout through the existing destination server config mutator.
The end-to-end environment builds the destination config directly without
validating it, so the minimum reclamation timeout does not obstruct tests.
Deriving the retry pacing from the guard is what makes the failure-streak path
reachable here through the same single knob.

The secondary seam is the source package's existing tests, used only for cases
the end-to-end seam cannot express precisely: guard arithmetic at boundary
values, and the asymmetric clearing rules between the two clocks. Those are
observable end to end in principle but would require either multi-hour
wall-clock or timing too tight to be reliable.

### Reproduce before fixing

Both bugs are currently unreproduced; everything above rests on reading the
code. The first work is two end-to-end tests that demonstrate them: a torrent
whose source data becomes unreadable stays tracked indefinitely and never
reaches quarantine, and a quarantined torrent's partial data is never reclaimed
from the destination. These are expected to fail before the fix and pass after,
and they become the regression tests.

If either fails to reproduce, the corresponding design decision should be
revisited rather than implemented.

### Coverage

End to end: premature quarantine no longer happens across a short destination
outage; a genuinely failing torrent still reaches quarantine; a torrent with
unreadable source data reaches quarantine through the stall path; a torrent
that is slow to download is never quarantined; releasing a quarantined torrent
resumes rather than re-copies; an abandoned transfer has its partial files
reclaimed; a pre-existing file placed on the destination before the sync
survives reclamation; a torrent owned by destination qBittorrent is never
reclaimed.

Package level: the guard boundary in both directions; the stall clock clearing
on advance while the failure clock does not; streak clocks surviving a restart;
the derived backoff at both default and small guard values.

### Prior art

The end-to-end suite already has tests covering orphan cleanup on torrent
removal, file selection changes, and early finalization, all built on the same
setup helpers with functional options. The destination package already has
tests that drive the reclamation loop directly by mutating the timeout on a
constructed server. Both patterns are the right models to follow.

## Out of Scope

No new component is added. An earlier framing of this work imagined a separate
repair layer that would sweep and fix failed syncs. Investigation showed every
failure it would have handled was a gap in the existing path, and a compensating
layer would have hidden the gaps rather than closing them.

Post-transfer drift is not addressed. Once a torrent is finalized nothing ever
re-verifies it, so damage to the destination copy afterwards, whether from
storage faults or external processes, goes unnoticed. This is the one failure
mode the existing design genuinely cannot detect, and it needs its own
justification rather than riding along here.

The per-cycle orchestration passes are left alone. Collapsing them into a single
iteration would be a structural improvement, but their ordering is load-bearing
for the per-cycle caching, and the risk outweighs the benefit in this change.

The streaming stack is untouched, apart from the piece monitor exposing two
additional pieces of progress information.

Congestion handling is unchanged. Its guard keeps its current, longer value and
stays a constant.

Documentation for the new metrics and the new configuration flag ships with the
implementation.

## Further Notes

The most serious problem found during this work was not in the failure
behaviour at all. Reclamation deletes files at their final path, which on an
unfinalized torrent are exactly the pre-existing files an operator put there.
The abort path explicitly preserves those files; reclamation destroys them.

This has been close to harmless only because reclamation almost never runs.
Making it run, which this work does, would have turned a dormant asymmetry into
live deletion of operator data. It was found by looking for duplication rather
than by looking for failures, which is worth remembering: the two deletion
implementations were suspicious because they were near-duplicates, and only then
did the difference between them turn out to matter.

The consolidation of source state is the larger part of the effort, and almost
all of that is reworking existing tests rather than production code. Tests
written against the current three-holder shape will need to move to the
consolidated one even though no behaviour changes.

The domain glossary records that this codebase still carries older vocabulary in
places, using hot and cold for source and destination. The consolidation work
touches several of those names and should move them toward the current terms
rather than preserving the old ones by copying.
