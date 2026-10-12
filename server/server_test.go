package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/protocol"
	"github.com/Onelabs9157/rinjing-agen/server"
	"github.com/Onelabs9157/rinjing-agen/shard"
	"github.com/Onelabs9157/rinjing-agen/store"
)

const (
	node  = "NODE_UJI"
	token = "0123456789abcdef0123456789abcdef-token-uji"
	bs    = shard.MinBlockSize
)

type env struct {
	st  *store.Store
	srv *httptest.Server
	c   *protocol.Client
}

func newEnv(t *testing.T, allow ...string) *env {
	t.Helper()
	st, err := store.Open(t.TempDir(), node)
	if err != nil {
		t.Fatal(err)
	}
	s := &server.Server{Store: st, Token: token, Version: "uji", Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, a := range allow {
		s.Allow = append(s.Allow, netip.MustParsePrefix(a))
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &env{st: st, srv: srv, c: &protocol.Client{BaseURL: srv.URL, Node: node, Token: token, HTTP: srv.Client()}}
}

func data(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 1)) //nolint:gosec // data uji
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	return b
}

func hdr(id uuid.UUID, idx uint8) shard.Header {
	return shard.Header{BlobID: id, Layout: shard.LayoutEC, Index: idx, DataShards: 3, ParityShards: 2, BlockSize: bs}
}

func upload(t *testing.T, c *protocol.Client, h shard.Header, payload []byte) error {
	t.Helper()
	u, err := c.Upload(context.Background(), h)
	if err != nil {
		return err
	}
	for p := payload; len(p) > 0; { // potongan tak sejajar blok
		n := min(3000, len(p))
		if _, err := u.Write(p[:n]); err != nil {
			return u.Close(0, [32]byte{})
		}
		p = p[n:]
	}
	return u.Close(uint64(len(payload)), sha256.Sum256(payload))
}

func readAll(t *testing.T, c *protocol.Client, ref protocol.ShardRef, first, count uint32) ([]byte, error) {
	t.Helper()
	b, err := c.Get(context.Background(), ref, first, count)
	if err != nil {
		return nil, err
	}
	defer b.Close()
	var out []byte
	for {
		blk, err := b.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, blk...)
	}
}

func TestSiklusShard(t *testing.T) {
	e := newEnv(t, "127.0.0.1/32", "::1/128")
	ctx := context.Background()
	h, err := e.c.Health(ctx)
	if err != nil || h.Node != node || h.Compatible() != nil {
		t.Fatalf("health: %+v %v", h, err)
	}

	id := uuid.New()
	ref := protocol.ShardRef{Blob: id, Index: 2}
	payload := data(3*bs + 123)
	if err := upload(t, e.c, hdr(id, 2), payload); err != nil {
		t.Fatal(err)
	}
	m, err := e.c.Head(ctx, ref)
	if err != nil || m.PayloadSize != uint64(len(payload)) || m.Blocks != 4 || m.BlobSHA256 != sha256.Sum256(payload) ||
		m.Header != hdr(id, 2) {
		t.Fatalf("head: %+v %v", m, err)
	}
	got, err := readAll(t, e.c, ref, 0, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("get penuh: %v", err)
	}
	got, err = readAll(t, e.c, ref, 1, 2)
	if err != nil || !bytes.Equal(got, payload[bs:3*bs]) {
		t.Fatalf("get blok 1–2: %v", err)
	}
	got, err = readAll(t, e.c, ref, 3, 100)
	if err != nil || !bytes.Equal(got, payload[3*bs:]) {
		t.Fatalf("get blok terakhir (count lewat): %v", err)
	}
	if got, err = readAll(t, e.c, ref, 4, 0); err != nil || len(got) != 0 {
		t.Fatalf("first = jumlah blok → kosong: %d %v", len(got), err)
	}
	if _, err := readAll(t, e.c, ref, 5, 0); err == nil {
		t.Fatal("first di luar shard")
	}

	// shard kosong (blob 0 byte)
	empty := protocol.ShardRef{Blob: uuid.New()}
	if err := upload(t, e.c, hdr(empty.Blob, 0), nil); err != nil {
		t.Fatal(err)
	}
	if got, err := readAll(t, e.c, empty, 0, 0); err != nil || len(got) != 0 {
		t.Fatalf("shard kosong: %v", err)
	}

	if err := e.c.Delete(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := e.c.Delete(ctx, ref); err != nil {
		t.Fatalf("hapus idempoten: %v", err)
	}
	if _, err := e.c.Head(ctx, ref); !errors.Is(err, protocol.ErrNotFound) {
		t.Fatalf("setelah hapus: %v", err)
	}
	if _, err := e.c.Get(ctx, ref, 0, 0); !errors.Is(err, protocol.ErrNotFound) {
		t.Fatalf("get setelah hapus: %v", err)
	}
}

func TestAutentikasi(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cases := map[string]*protocol.Client{
		"token salah": {BaseURL: e.srv.URL, Node: node, Token: strings.Repeat("x", 40)},
		"node lain":   {BaseURL: e.srv.URL, Node: "NODE_LAIN", Token: token},
		"jam meleset": {BaseURL: e.srv.URL, Node: node, Token: token, Now: func() time.Time { return time.Now().Add(10 * time.Minute) }},
	}
	for name, c := range cases {
		if _, err := c.Health(ctx); !errors.Is(err, protocol.ErrUnauthorized) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// tanpa header sama sekali
	resp, err := http.Get(e.srv.URL + "/v1/health")
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("tanpa tanda tangan: %v %v", resp.StatusCode, err)
	}
	resp.Body.Close()
	// tanda tangan untuk path lain tidak berlaku
	req, _ := http.NewRequest(http.MethodDelete, e.srv.URL+"/v1/shards/"+uuid.NewString()+"/0", nil)
	protocol.Sign(req, node, token, time.Now())
	req.URL.Path = "/v1/shards/" + uuid.NewString() + "/0"
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("path ditukar: %v %v", resp.StatusCode, err)
	}
	resp.Body.Close()
	// /healthz tanpa autentikasi, tanpa informasi
	resp, err = http.Get(e.srv.URL + "/healthz")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("healthz: %d %q", resp.StatusCode, body)
	}
}

func TestAllowlistIP(t *testing.T) {
	e := newEnv(t, "10.10.10.25/32")
	if _, err := e.c.Health(context.Background()); !errors.Is(err, protocol.ErrUnauthorized) {
		t.Fatalf("IP di luar allowlist → 403: %v", err)
	}
	var pe *protocol.Error
	_, err := e.c.Health(context.Background())
	if !errors.As(err, &pe) || pe.Status != http.StatusForbidden {
		t.Fatalf("status: %v", err)
	}
}

func TestUnggahanDitolakTidakMeninggalkanFile(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.st.MaxPayload = 2 * bs

	// melebihi batas
	big := uuid.New()
	if err := upload(t, e.c, hdr(big, 0), data(3*bs)); !errors.Is(err, shard.ErrTooLarge) {
		t.Fatalf("terlalu besar: %v", err)
	}
	// header body tidak cocok dengan URL: kirim body shard idx 1 ke alamat idx 0
	id := uuid.New()
	var body bytes.Buffer
	sw, _ := shard.NewStreamWriter(&body, hdr(id, 1))
	_, _ = sw.Write(data(10))
	_ = sw.Close(10, [32]byte{})
	req, _ := http.NewRequest(http.MethodPut, e.srv.URL+protocol.ShardPath(protocol.ShardRef{Blob: id}), &body)
	protocol.Sign(req, node, token, time.Now())
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("mismatch: %v %v", resp.StatusCode, err)
	}
	resp.Body.Close()
	// dibatalkan di tengah
	ab := uuid.New()
	u, err := e.c.Upload(ctx, hdr(ab, 0))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = u.Write(data(bs + 10))
	if err := u.Abort(errors.New("sumber putus")); err != nil {
		t.Fatalf("abort: %v", err)
	}
	for _, r := range []protocol.ShardRef{{Blob: big}, {Blob: id}, {Blob: id, Index: 1}, {Blob: ab}} {
		if _, err := e.c.Head(ctx, r); !errors.Is(err, protocol.ErrNotFound) {
			t.Errorf("%s tidak boleh tersimpan: %v", r, err)
		}
	}
	tmp, _ := os.ReadDir(filepath.Join(e.st.Dir(), "tmp"))
	if len(tmp) != 0 {
		t.Errorf("sisa tmp: %d", len(tmp))
	}
}

// pathOf — lokasi file shard di disk (mengikuti tata letak store).
func pathOf(st *store.Store, ref protocol.ShardRef) string {
	id := ref.Blob.String()
	return filepath.Join(st.Dir(), "shards", id[0:2], id[2:4], ref.String())
}

func corrupt(t *testing.T, st *store.Store, ref protocol.ShardRef, off int64) {
	t.Helper()
	f, err := os.OpenFile(pathOf(st, ref), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := []byte{0}
	_, _ = f.ReadAt(b, off)
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func TestShardRusakTerdeteksi(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	refs := make([]protocol.ShardRef, 3)
	for i := range refs {
		refs[i] = protocol.ShardRef{Blob: uuid.New()}
		if err := upload(t, e.c, hdr(refs[i].Blob, 0), data(3*bs)); err != nil {
			t.Fatal(err)
		}
	}
	corrupt(t, e.st, refs[0], shard.HeaderSize+5)      // blok 0
	corrupt(t, e.st, refs[1], shard.HeaderSize+2*bs+7) // blok 2

	// blok pertama rusak → 422 sebelum data dikirim
	if _, err := e.c.Get(ctx, refs[0], 0, 0); !errors.Is(err, shard.ErrCorrupt) {
		t.Fatalf("422: %v", err)
	}
	// blok tengah rusak → koneksi putus tanpa frame penutup
	if _, err := readAll(t, e.c, refs[1], 0, 0); !errors.Is(err, shard.ErrCorrupt) {
		t.Fatalf("rusak di tengah: %v", err)
	}
	// blok sehat dari shard yang sama masih bisa dibaca
	if _, err := readAll(t, e.c, refs[1], 0, 2); err != nil {
		t.Fatalf("blok 0–1 sehat: %v", err)
	}
	gone := protocol.ShardRef{Blob: uuid.New()}
	v, err := e.c.Verify(ctx, append(refs, gone))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Missing) != 1 || v.Missing[0] != gone || len(v.Corrupt) != 2 {
		t.Fatalf("verify: %+v", v)
	}
	// tail rusak → shard tidak bisa dibuka sama sekali
	corrupt(t, e.st, refs[2], int64(shard.FileSize(3*bs, bs))-10)
	if _, err := e.c.Head(ctx, refs[2]); !errors.Is(err, shard.ErrCorrupt) {
		t.Fatalf("head tail rusak: %v", err)
	}
}

func TestDaftarBerhalaman(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	want := map[string]bool{}
	for range 7 {
		id := uuid.New()
		for idx := range uint8(2) {
			if err := upload(t, e.c, hdr(id, idx), data(10)); err != nil {
				t.Fatal(err)
			}
			want[protocol.ShardRef{Blob: id, Index: idx}.String()] = true
		}
	}
	var seen []string
	after := ""
	for page := 0; ; page++ {
		l, err := e.c.List(ctx, after, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range l.Shards {
			seen = append(seen, s.String())
			if s.FileSize != shard.FileSize(10, bs) {
				t.Errorf("ukuran %d", s.FileSize)
			}
		}
		if l.Next == "" {
			break
		}
		after = l.Next
		if page > 10 {
			t.Fatal("paging tidak berhenti")
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("dapat %d, mau %d: %v", len(seen), len(want), seen)
	}
	for i, s := range seen {
		if !want[s] || (i > 0 && seen[i-1] >= s) {
			t.Fatalf("urutan/isi salah di %d: %v", i, seen)
		}
	}
}

func TestRequestTidakSah(t *testing.T) {
	e := newEnv(t)
	do := func(method, path, body string) int {
		req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
		protocol.Sign(req, node, token, time.Now())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	id := uuid.NewString()
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{
		{http.MethodGet, "/v1/shards/bukan-uuid/0", "", http.StatusBadRequest},
		{http.MethodGet, "/v1/shards/" + id + "/999", "", http.StatusBadRequest},
		{http.MethodGet, "/v1/shards/" + id + "/0?first=x", "", http.StatusBadRequest},
		{http.MethodPost, "/v1/verify", "{bukan json", http.StatusBadRequest},
		{http.MethodPost, "/v1/verify", `{"shards":[` + strings.Repeat(`{"blob":"`+id+`","idx":0},`, protocol.MaxVerifyBatch) + `{"blob":"` + id + `","idx":0}]}`, http.StatusBadRequest},
		{http.MethodPut, "/v1/shards/" + id + "/0", "sampah", http.StatusUnprocessableEntity},              // terpotong sebelum header lengkap
		{http.MethodPut, "/v1/shards/" + id + "/0", strings.Repeat("sampah!!", 10), http.StatusBadRequest}, // bukan format shard
		{http.MethodDelete, "/v1/shards/" + id + "/x", "", http.StatusBadRequest},
		{http.MethodHead, "/v1/shards/" + id + "/0", "", http.StatusNotFound},
	} {
		if got := do(tc.method, tc.path, tc.body); got != tc.want {
			t.Errorf("%s %s: %d, mau %d", tc.method, tc.path, got, tc.want)
		}
	}
}
