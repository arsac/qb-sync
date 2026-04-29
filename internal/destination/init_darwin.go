package destination

import "syscall"

// sameDev reports whether two stat results share the same device ID.
// Darwin Stat_t.Dev is int32; both sides go through the same zero-extension
// so consistent comparison is preserved.
//

func sameDev(a, b *syscall.Stat_t) bool { return uint32(a.Dev) == uint32(b.Dev) }
