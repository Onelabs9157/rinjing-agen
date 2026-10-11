//go:build linux

package store

import (
	"errors"
	"syscall"
)

func diskStat(dir string) (DiskStat, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return DiskStat{}, err
	}
	bs := uint64(st.Bsize) //nolint:gosec // ukuran blok selalu positif
	return DiskStat{Known: true, Total: st.Blocks * bs, Free: st.Bavail * bs}, nil
}

func isNoSpace(err error) bool { return errors.Is(err, syscall.ENOSPC) }
