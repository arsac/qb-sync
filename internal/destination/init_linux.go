package destination

import "syscall"

// sameDev reports whether two stat results share the same device ID.
// Linux Stat_t.Dev is already uint64; no conversion is needed.
func sameDev(a, b *syscall.Stat_t) bool { return a.Dev == b.Dev }
