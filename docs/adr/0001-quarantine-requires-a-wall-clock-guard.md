# Quarantine requires a wall-clock guard, not an attempt count

Quarantine was reachable after three consecutive finalization failures, and
because the exponential backoff was always shorter than the orchestrator cycle
interval, those three attempts landed roughly ninety seconds apart. Any fault
lasting two minutes therefore quarantined every finalizing torrent permanently,
recoverable only by a human removing the tag. Quarantine now requires both a
failure count and a continuous-failure duration exceeding a configurable guard
(`--sync-failed-guard`, default 4h), stalls are tracked on a separate
duration-only clock, and both clocks are persisted.

## Considered Options

- **Never terminal.** Retry quarantined torrents indefinitely on a long
  backoff. Rejected: a permanently broken torrent would burn cycles forever and
  the tag would stop signalling "needs attention".
- **Bounded auto-retry after quarantine.** A few widely spaced retries before
  going truly terminal. Rejected: two retry mechanisms to reason about, for a
  case the guard already covers.
- **Fix the backoff alone.** Raise the cap so three attempts span an hour.
  Rejected: still attempt-based, so the window depends on cycle interval and
  error class rather than on how long the fault actually lasted.

## Consequences

- **The finalization backoff cap is now derived from the guard, and that change
  is load-bearing.** The cap was previously a constant so small that the
  orchestrator cycle interval always dominated it, making the backoff
  vestigial. Under a 4h guard with no working backoff, a torrent may make
  roughly 480 finalization attempts, and the disk-stage-error path re-verifies
  every piece while holding the destination's disk-stage semaphore. That
  starves all other finalization into congestion responses and their own
  guards. Without the backoff fix the guard is a self-inflicted denial of
  service.

  The cap is `min(30m, guard/8)` rather than an independent constant. The
  backoff exists only to bound attempts inside the guard window, so coupling
  the two keeps roughly a dozen attempts per window at any guard value. At the
  4h default this yields 30m. It also means a single knob shrinks the whole
  mechanism for end-to-end tests, instead of an operator-facing knob that
  exists only so tests can run.
- **Stalls use a duration-only clock.** "Three failed attempts" is a category
  error for a continuous condition. Reusing the failure counter would also
  leave a recovered torrent carrying a large finalization backoff it never
  earned.
- **The stall clock clears on advance; the failure clock does not.** The
  INCOMPLETE path re-streams pieces between attempts, so clearing the failure
  clock on advance would mean repeated verification failures never quarantine
  at all.
- **Streak state is persisted** to `.qb-sync/failure_streaks.json`. With
  in-memory state, a source restarting more often than the guard could never
  quarantine anything, silently delivering "never terminal" while the design
  claimed otherwise.
- **The BUSY guard is left at 8h and remains a constant.** Congestion is a
  destination-wide condition that legitimately lasts hours; a failure streak is
  a per-torrent fault. They are deliberately not the same knob.
