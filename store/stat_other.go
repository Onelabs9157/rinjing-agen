//go:build !linux

package store

// Agen produksi berjalan di Linux; di OS lain (pengembangan) kapasitas disk tidak dibaca.
func diskStat(string) (DiskStat, error) { return DiskStat{}, nil }

func isNoSpace(error) bool { return false }
