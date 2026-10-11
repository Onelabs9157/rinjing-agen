package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/protocol"
	"github.com/Onelabs9157/rinjing-agen/shard"
)

func body(t *testing.T, ref protocol.ShardRef, payload []byte) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	sw, err := shard.NewStreamWriter(&b, shard.Header{BlobID: ref.Blob, Layout: shard.LayoutReplica, Index: ref.Index,
		DataShards: 1, ParityShards: 2, BlockSize: shard.MinBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = sw.Write(payload)
	_ = sw.Close(uint64(len(payload)), [32]byte{})
	return &b
}

func TestPenandaFolder(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(dir, ""); err == nil {
		t.Fatal("node kosong")
	}
	s, err := Open(dir, "NODE_A")
	if err != nil || s.Dir() != dir || s.Node() != "NODE_A" {
		t.Fatal(err)
	}
	if _, err := Open(dir, "NODE_A"); err != nil {
		t.Fatalf("buka ulang: %v", err)
	}
	if _, err := Open(dir, "NODE_B"); err == nil || !strings.Contains(err.Error(), "milik node NODE_A") {
		t.Fatalf("folder node lain: %v", err)
	}
	// marker rusak
	_ = os.WriteFile(filepath.Join(dir, markerDir, markerFile), []byte("{"), 0o600)
	if _, err := Open(dir, "NODE_A"); err == nil {
		t.Fatal("marker rusak")
	}
	// shard tanpa marker (folder dipakai tanpa cap) → tolak
	other := t.TempDir()
	_ = os.MkdirAll(filepath.Join(other, "shards", "ab"), 0o750)
	if _, err := Open(other, "NODE_A"); err == nil {
		t.Fatal("shard tanpa marker")
	}
	// dir berupa file → gagal mkdir
	f := filepath.Join(t.TempDir(), "file")
	_ = os.WriteFile(f, nil, 0o600)
	if _, err := Open(f, "NODE_A"); err == nil {
		t.Fatal("dir berupa file")
	}
}

func TestPutOpenDelete(t *testing.T) {
	s, _ := Open(t.TempDir(), "NODE_A")
	ctx := context.Background()
	ref := protocol.ShardRef{Blob: uuid.New(), Index: 1}
	if _, _, err := s.Put(ctx, protocol.ShardRef{Blob: ref.Blob}, body(t, ref, []byte("isi"))); !errors.Is(err, ErrMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	h, tail, err := s.Put(ctx, ref, body(t, ref, []byte("isi")))
	if err != nil || h.Index != 1 || tail.PayloadSize != 3 {
		t.Fatal(err)
	}
	sh, err := s.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := sh.ReadBlock(0, nil)
	if err != nil || string(b) != "isi" || sh.Info.ShardRef != ref {
		t.Fatalf("baca: %q %v", b, err)
	}
	_ = sh.Close()
	// file dipindah ke alamat shard lain → header tidak cocok = rusak
	moved := protocol.ShardRef{Blob: ref.Blob, Index: 2}
	if err := os.Rename(s.path(ref), s.path(moved)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(moved); !errors.Is(err, shard.ErrCorrupt) {
		t.Fatalf("salah tempat: %v", err)
	}
	if _, corrupt, err := s.Verify(ctx, []protocol.ShardRef{moved}); err != nil || len(corrupt) != 1 {
		t.Fatalf("verify salah tempat: %v %v", corrupt, err)
	}
	if err := s.Delete(moved); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(moved); !errors.Is(err, ErrNotFound) {
		t.Fatalf("setelah hapus: %v", err)
	}
	// ctx batal di tengah unggahan
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := s.Put(cctx, ref, body(t, ref, []byte("x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx batal: %v", err)
	}
	if _, _, err := s.Verify(cctx, []protocol.ShardRef{ref}); !errors.Is(err, context.Canceled) {
		t.Fatalf("verify ctx batal: %v", err)
	}
	if _, _, err := s.List(cctx, "", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("list ctx batal: %v", err)
	}
}

func TestListCursorMelewatiFolder(t *testing.T) {
	s, _ := Open(t.TempDir(), "NODE_A")
	ctx := context.Background()
	ids := []string{
		"00aa0000-0000-7000-8000-000000000001",
		"00bb0000-0000-7000-8000-000000000002",
		"ff000000-0000-7000-8000-000000000003",
	}
	for _, id := range ids {
		ref := protocol.ShardRef{Blob: uuid.MustParse(id)}
		if _, _, err := s.Put(ctx, ref, body(t, ref, []byte("a"))); err != nil {
			t.Fatal(err)
		}
	}
	// file asing di folder shard diabaikan
	_ = os.WriteFile(filepath.Join(s.dir, "shards", "00", "aa", "catatan.txt"), nil, 0o600)
	all, next, err := s.List(ctx, "", 0)
	if err != nil || len(all) != 3 || next != "" {
		t.Fatalf("semua: %v %q %v", all, next, err)
	}
	got, _, _ := s.List(ctx, ids[0]+".0", 10)
	if len(got) != 2 || got[0].Blob.String() != ids[1] {
		t.Fatalf("setelah cursor: %v", got)
	}
	got, _, _ = s.List(ctx, "fe", 10)
	if len(got) != 1 || got[0].Blob.String() != ids[2] {
		t.Fatalf("cursor pendek: %v", got)
	}
	page, next, _ := s.List(ctx, "", 2)
	if len(page) != 2 || next != ids[1]+".0" {
		t.Fatalf("halaman: %v %q", page, next)
	}
}

func TestSweepTmpDanParseName(t *testing.T) {
	s, _ := Open(t.TempDir(), "NODE_A")
	old := filepath.Join(s.dir, "tmp", "lama")
	_ = os.WriteFile(old, nil, 0o600)
	_ = os.Chtimes(old, time.Now().Add(-48*time.Hour), time.Now().Add(-48*time.Hour))
	_ = os.WriteFile(filepath.Join(s.dir, "tmp", "baru"), nil, 0o600)
	if n, err := s.SweepTmp(24 * time.Hour); err != nil || n != 1 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	id := uuid.New()
	if ref, ok := ParseName(id.String() + ".3"); !ok || ref.Index != 3 {
		t.Fatal("nama sah")
	}
	for _, bad := range []string{"tanpatitik", "x.1", id.String() + ".z"} {
		if _, ok := ParseName(bad); ok {
			t.Errorf("%q diterima", bad)
		}
	}
	if st, err := s.Stat(); err != nil || (st.Known && st.Total == 0) {
		t.Fatalf("stat: %+v %v", st, err)
	}
	_ = os.RemoveAll(filepath.Join(s.dir, "tmp"))
	if _, err := s.SweepTmp(time.Hour); err == nil {
		t.Fatal("folder tmp hilang")
	}
	ref := protocol.ShardRef{Blob: id}
	if _, _, err := s.Put(context.Background(), ref, body(t, ref, nil)); err == nil {
		t.Fatal("put tanpa folder tmp")
	}
}

func TestMinFree(t *testing.T) {
	s, _ := Open(t.TempDir(), "NODE_A")
	st, _ := s.Stat()
	if !st.Known {
		t.Skip("kapasitas disk tidak terbaca di OS ini")
	}
	s.MinFree = st.Free + 1<<40
	ref := protocol.ShardRef{Blob: uuid.New()}
	if _, _, err := s.Put(context.Background(), ref, body(t, ref, nil)); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("disk hampir penuh: %v", err)
	}
}
