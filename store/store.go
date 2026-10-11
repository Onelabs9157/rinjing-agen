// Package store — penyimpanan shard di disk agen.
//
//	<Dir>/.rinjing-agen/format.json   penanda folder milik node mana (mencegah salah mount)
//	<Dir>/shards/<2>/<2>/<blob>.<idx> file shard (format paket shard)
//	<Dir>/tmp/                        unggahan yang sedang berjalan
//
// Paket publik: Rinjing utama yang ikut menyimpan shard (CLUSTER_LOCAL_AGENT) memakai paket ini
// langsung, tanpa lewat HTTP.
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/protocol"
	"github.com/Onelabs9157/rinjing-agen/shard"
)

var (
	ErrNotFound = errors.New("shard tidak ada")
	ErrNoSpace  = errors.New("ruang disk tidak cukup")
	// ErrMismatch — header di body tidak sama dengan blob/idx di URL.
	ErrMismatch = errors.New("header shard tidak cocok dengan alamatnya")
)

const (
	markerDir  = ".rinjing-agen"
	markerFile = "format.json"
	writeBuf   = 256 << 10
)

// DiskStat — kapasitas filesystem tempat Dir berada. Known=false di OS tanpa statfs.
type DiskStat struct {
	Known bool
	Total uint64
	Free  uint64
}

// Store — aman dipakai bersamaan. Shard immutable: dua PUT ke alamat sama berisi byte yang sama,
// jadi rename terakhir yang menang tidak mengubah apa pun.
type Store struct {
	dir  string
	node string
	// MinFree — unggahan ditolak (ErrNoSpace) bila sisa disk di bawah ini.
	MinFree uint64
	// MaxPayload — batas payload satu shard (0 = tanpa batas).
	MaxPayload uint64
}

type marker struct {
	FormatVersion int       `json:"format_version"`
	Node          string    `json:"node"`
	CreatedAt     time.Time `json:"created_at"`
}

// Open — siapkan dir untuk node. Folder kosong dicap milik node ini; folder milik node lain
// atau berisi shard tanpa penanda ditolak (salah mount/konfigurasi lebih aman berhenti).
func Open(dir, node string) (*Store, error) {
	if node == "" {
		return nil, errors.New("store: kode node kosong")
	}
	for _, d := range []string{dir, filepath.Join(dir, "shards"), filepath.Join(dir, "tmp"), filepath.Join(dir, markerDir)} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, markerDir, markerFile)
	raw, err := os.ReadFile(path) //nolint:gosec // path dari konfigurasi operator
	switch {
	case err == nil:
		var m marker
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("store: %s rusak: %w", path, err)
		}
		if m.Node != node {
			return nil, fmt.Errorf("store: folder %s milik node %s, bukan %s", dir, m.Node, node)
		}
	case errors.Is(err, fs.ErrNotExist):
		entries, err := os.ReadDir(filepath.Join(dir, "shards"))
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			return nil, fmt.Errorf("store: %s berisi shard tanpa penanda node — periksa mount", dir)
		}
		raw, _ := json.MarshalIndent(marker{FormatVersion: shard.FormatVersion, Node: node, CreatedAt: time.Now().UTC()}, "", "  ")
		if err := writeAtomic(path, raw); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return &Store{dir: dir, node: node}, nil
}

// Dir — folder data store.
func (s *Store) Dir() string { return s.dir }

// Node — kode node pemilik store.
func (s *Store) Node() string { return s.node }

func (s *Store) path(ref protocol.ShardRef) string {
	id := ref.Blob.String()
	return filepath.Join(s.dir, "shards", id[0:2], id[2:4], ref.String())
}

// Stat — kapasitas disk.
func (s *Store) Stat() (DiskStat, error) { return diskStat(s.dir) }

// Put — terima body PUT (format shard.Receive) untuk ref. Atomik: file tujuan hanya muncul bila
// semua blok & trailer lolos checksum dan sudah di-fsync.
func (s *Store) Put(ctx context.Context, ref protocol.ShardRef, body io.Reader) (shard.Header, shard.Tail, error) {
	if s.MinFree > 0 {
		if st, err := s.Stat(); err == nil && st.Known && st.Free < s.MinFree {
			return shard.Header{}, shard.Tail{}, ErrNoSpace
		}
	}
	tmp, err := os.CreateTemp(filepath.Join(s.dir, "tmp"), ref.String()+".*")
	if err != nil {
		return shard.Header{}, shard.Tail{}, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	bw := bufio.NewWriterSize(tmp, writeBuf)
	h, t, err := shard.Receive(bw, ctxReader{ctx, body}, s.MaxPayload, func(h shard.Header) error {
		if h.BlobID != ref.Blob || h.Index != ref.Index {
			return fmt.Errorf("%w: body %s.%d, alamat %s", ErrMismatch, h.BlobID, h.Index, ref)
		}
		return nil
	})
	if err == nil {
		err = bw.Flush()
	}
	if err == nil {
		err = tmp.Sync()
	}
	if err != nil {
		return h, t, classify(err)
	}
	if err := tmp.Close(); err != nil {
		return h, t, classify(err)
	}
	dst := s.path(ref)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return h, t, err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return h, t, err
	}
	keep = true
	syncDir(filepath.Dir(dst))
	return h, t, nil
}

// Shard — file shard terbuka. Pemanggil wajib Close.
type Shard struct {
	*shard.File
	f    *os.File
	Info protocol.ShardInfo
}

func (s *Shard) Close() error { return s.f.Close() }

// Open — buka & verifikasi kerangka shard (header, tail, tabel). Isi blok diverifikasi saat dibaca.
func (s *Store) Open(ref protocol.ShardRef) (*Shard, error) {
	f, err := os.Open(s.path(ref))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sf, err := shard.OpenFile(f, st.Size())
	if err == nil && (sf.Header().BlobID != ref.Blob || sf.Header().Index != ref.Index) {
		err = fmt.Errorf("%w: %w", shard.ErrCorrupt, ErrMismatch)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Shard{File: sf, f: f, Info: protocol.ShardInfo{ShardRef: ref, FileSize: st.Size(), ModTime: st.ModTime().UTC()}}, nil
}

// Delete — idempoten.
func (s *Store) Delete(ref protocol.ShardRef) error {
	err := os.Remove(s.path(ref))
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// List — shard terurut nama (`<blob>.<idx>`), mulai SETELAH after. next kosong = habis.
func (s *Store) List(ctx context.Context, after string, limit int) ([]protocol.ShardInfo, string, error) {
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	out := make([]protocol.ShardInfo, 0, min(limit, 256))
	root := filepath.Join(s.dir, "shards")
	done := errors.New("cukup")
	err := walkSorted(root, func(dir string, depth int, name string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if depth < 2 {
			// folder prefiks p hanya berisi nama berawalan p: lewati bila seluruhnya sebelum cursor
			p := prefixOf(dir, root) + name
			return p >= after[:min(len(after), len(p))], nil
		}
		if name <= after {
			return false, nil
		}
		ref, ok := ParseName(name)
		if !ok {
			return false, nil // bukan file shard (sisa manual) — abaikan
		}
		st, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return false, nil
		}
		out = append(out, protocol.ShardInfo{ShardRef: ref, FileSize: st.Size(), ModTime: st.ModTime().UTC()})
		if len(out) == limit {
			return false, done
		}
		return false, nil
	})
	if errors.Is(err, done) {
		return out, out[len(out)-1].String(), nil
	}
	return out, "", err
}

// prefixOf — gabungan nama folder prefiks antara root dan dir ("ab", "abcd").
func prefixOf(dir, root string) string {
	rel, _ := filepath.Rel(root, dir)
	if rel == "." {
		return ""
	}
	return strings.ReplaceAll(rel, string(filepath.Separator), "")
}

// walkSorted — telusuri root (2 tingkat folder + file) terurut nama. visit mengembalikan true
// untuk masuk ke folder.
func walkSorted(root string, visit func(dir string, depth int, name string) (bool, error)) error {
	var walk func(dir string, depth int) error
	walk = func(dir string, depth int) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if (depth < 2) == e.IsDir() {
				names = append(names, e.Name())
			}
		}
		slices.Sort(names)
		for _, n := range names {
			enter, err := visit(dir, depth, n)
			if err != nil {
				return err
			}
			if enter {
				if err := walk(filepath.Join(dir, n), depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(root, 0)
}

// Verify — cek checksum penuh shard-shard ref di disk ini.
func (s *Store) Verify(ctx context.Context, refs []protocol.ShardRef) (missing, corrupt []protocol.ShardRef, err error) {
	for _, ref := range refs {
		if err := ctx.Err(); err != nil {
			return missing, corrupt, err
		}
		sh, err := s.Open(ref)
		switch {
		case errors.Is(err, ErrNotFound):
			missing = append(missing, ref)
			continue
		case errors.Is(err, shard.ErrCorrupt), errors.Is(err, shard.ErrFormat):
			corrupt = append(corrupt, ref)
			continue
		case err != nil:
			return missing, corrupt, err
		}
		verr := sh.Verify()
		_ = sh.Close()
		if errors.Is(verr, shard.ErrCorrupt) {
			corrupt = append(corrupt, ref)
		} else if verr != nil {
			return missing, corrupt, verr
		}
	}
	return missing, corrupt, nil
}

// SweepTmp — hapus sisa unggahan yang mati di tengah, lebih tua dari olderThan.
func (s *Store) SweepTmp(olderThan time.Duration) (int, error) {
	dir := filepath.Join(s.dir, "tmp")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	n := 0
	cutoff := time.Now().Add(-olderThan)
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if os.Remove(filepath.Join(dir, e.Name())) == nil {
			n++
		}
	}
	return n, nil
}

// ---- utilitas ----

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// classify — galat disk penuh → ErrNoSpace (agen menjawab 507).
func classify(err error) error {
	if isNoSpace(err) {
		return fmt.Errorf("%w: %w", ErrNoSpace, err)
	}
	return err
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// syncDir — fsync folder supaya rename tahan mati listrik (tidak didukung di Windows).
func syncDir(dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if d, err := os.Open(dir); err == nil { //nolint:gosec // folder milik store
		_ = d.Sync()
		_ = d.Close()
	}
}

// ParseName — "<blob>.<idx>" → ref (untuk alat fsck / pembangunan ulang lokasi).
func ParseName(name string) (protocol.ShardRef, bool) {
	blob, idx, ok := strings.Cut(name, ".")
	if !ok {
		return protocol.ShardRef{}, false
	}
	ref, err := protocol.ParseShardRef(blob, idx)
	return ref, err == nil && ref.Blob != uuid.Nil
}
