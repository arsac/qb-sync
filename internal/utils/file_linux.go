package utils

import "syscall"

// statDev returns the device ID as uint64. On Linux Stat_t.Dev is already uint64.
func statDev(stat *syscall.Stat_t) uint64 { return stat.Dev }
