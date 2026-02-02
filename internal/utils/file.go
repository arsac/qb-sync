package utils

import (
	"os"
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
