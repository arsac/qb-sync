package utils

import "syscall"

// statDev returns the device ID as uint64. On Darwin Stat_t.Dev is int32;
// zero-extend through uint32 so negative device numbers don't sign-extend
// into the high 32 bits.
//
//nolint:gosec // intentional bit-pattern reinterpret, not a numeric narrowing
func statDev(stat *syscall.Stat_t) uint64 { return uint64(uint32(stat.Dev)) }
