package utils

import (
	"os"
	"syscall"
)

func AreHardlinked(path1, path2 string) (bool, error) {
	info1, err := os.Stat(path1)
	if err != nil {
		return false, err
	}

	info2, err := os.Stat(path2)
	if err != nil {
		return false, err
	}

	return os.SameFile(info1, info2), nil
}

// GetInode returns the inode number for a file.
func GetInode(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, nil // Platform doesn't support inodes
	}

	return stat.Ino, nil
}

func FileExistsWithSize(path string, expectedSize int64) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Size() == expectedSize, nil
}
