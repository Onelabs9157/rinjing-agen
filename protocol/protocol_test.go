package protocol

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/shard"
)

const (
	node  = "NODE_A"
	token = "token-uji-yang-panjangnya-lebih-dari-32"
)

func signed(method, target string, now time.Time) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	Sign(r, node, token, now)
	return r
}

func TestTandaTangan(t *testing.T) {
	now := time.Unix(1_790_000_000, 0)
	r := signed(http.MethodGet, "/v1/shards?limit=5&after=x", now)
	if err := Verify(r, node, token, now.Add(MaxClockSkew)); err != nil {
		t.Fatalf("di batas skew: %v", err)
	}
	// urutan query tidak berpengaruh (dinormalisasi)
	r2 := httptest.NewRequest(http.MethodGet, "/v1/shards?after=x&limit=5", nil)
	r2.Header.Set("Authorization", r.Header.Get("Authorization"))
	if err := Verify(r2, node, token, now); err != nil {
		t.Fatalf("query diurutkan: %v", err)
	}
	cases := map[string]func() *http.Request{
		"kedaluwarsa": func() *http.Request { return signed(http.MethodGet, "/v1/health", now.Add(-MaxClockSkew-time.Second)) },
		"dari masa depan": func() *http.Request {
			return signed(http.MethodGet, "/v1/health", now.Add(MaxClockSkew+time.Second))
		},
		"metode ditukar": func() *http.Request {
			x := signed(http.MethodGet, "/v1/shards/a/0", now)
			x.Method = http.MethodDelete
			return x
		},
		"query ditambah": func() *http.Request {
			x := signed(http.MethodGet, "/v1/shards/a/0?first=0", now)
			x.URL.RawQuery = "first=9"
			return x
		},
		"skema lain": func() *http.Request {
			x := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
			x.Header.Set("Authorization", "Bearer "+token)
			return x
		},
		"ts rusak": func() *http.Request {
			x := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
			x.Header.Set("Authorization", "RJ1 node=NODE_A,ts=abc,sig=00")
			return x
		},
		"tanpa header": func() *http.Request { return httptest.NewRequest(http.MethodGet, "/v1/health", nil) },
	}
	for name, mk := range cases {
		if err := Verify(mk(), node, token, now); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := Verify(signed(http.MethodGet, "/v1/health", now), node, "token-lain-yang-panjangnya-juga-cukup", now); !errors.Is(err, ErrUnauthorized) {
		t.Error("token lain")
	}
}

func TestShardRefDanMeta(t *testing.T) {
	id := uuid.New()
	ref, err := ParseShardRef(id.String(), "4")
	if err != nil || ref.Index != 4 || ShardPath(ref) != "/v1/shards/"+id.String()+"/4" || ref.String() != id.String()+".4" {
		t.Fatalf("ref: %+v %v", ref, err)
	}
	for _, bad := range [][2]string{{"x", "0"}, {uuid.Nil.String(), "0"}, {id.String(), "256"}, {id.String(), "-1"}} {
		if _, err := ParseShardRef(bad[0], bad[1]); err == nil {
			t.Errorf("%v diterima", bad)
		}
	}
	m := Meta{Header: shard.Header{BlobID: id, Layout: shard.LayoutReplica, Index: 1, DataShards: 1, ParityShards: 2,
		BlockSize: shard.MinBlockSize}, PayloadSize: 10_000, BlobSize: 10_000, Blocks: 3, BlobSHA256: [32]byte{1, 2}}
	h := http.Header{}
	SetMeta(h, m)
	got, err := ParseMeta(h, ShardRef{Blob: id, Index: 1})
	if err != nil || got != m {
		t.Fatalf("meta bolak-balik: %+v %v", got, err)
	}
	h.Set(HeaderLayout, "ec") // replika dengan DataShards 1 + parity → EC 1+2 tetap sah; ubah blok
	h.Set(HeaderBlocks, "9")
	if _, err := ParseMeta(h, ShardRef{Blob: id, Index: 1}); err == nil {
		t.Error("jumlah blok tidak cocok")
	}
	h.Set(HeaderBlobSHA256, "zz")
	if _, err := ParseMeta(h, ShardRef{Blob: id, Index: 1}); err == nil {
		t.Error("sha256 rusak")
	}
	h = http.Header{}
	SetMeta(h, m)
	h.Set(HeaderLayout, "aneh")
	if _, err := ParseMeta(h, ShardRef{Blob: id, Index: 1}); err == nil {
		t.Error("layout tak dikenal")
	}
	if (Health{APIVersion: 9, FormatVersion: shard.FormatVersion}).Compatible() == nil {
		t.Error("API beda versi")
	}
}

func TestGalatAgen(t *testing.T) {
	for code, target := range map[string]error{
		CodeNotFound: ErrNotFound, CodeCorrupt: shard.ErrCorrupt, CodeForbidden: ErrUnauthorized,
		CodeInsufficientStorage: ErrInsufficientStorage, CodeTooLarge: shard.ErrTooLarge,
	} {
		e := &Error{Status: 400, Code: code}
		if !errors.Is(e, target) || errors.Is(e, io.EOF) {
			t.Errorf("%s ↛ %v", code, target)
		}
	}
	for status, code := range map[int]string{
		404: CodeNotFound, 401: CodeUnauthorized, 403: CodeForbidden, 422: CodeCorrupt,
		507: CodeInsufficientStorage, 413: CodeTooLarge, 400: CodeBadRequest, 502: CodeInternal,
	} {
		if statusCode(status) != code {
			t.Errorf("%d → %s", status, statusCode(status))
		}
	}
	if !strings.Contains((&Error{Status: 500, Code: "x", Message: "y"}).Error(), "500 x: y") {
		t.Error("pesan galat")
	}
}

// stub — agen palsu yang menjawab apa adanya (untuk jalur galat klien).
func stub(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &Client{BaseURL: srv.URL + "/", Node: node, Token: token}
}

func TestKlienJalurGalat(t *testing.T) {
	ctx := context.Background()
	c := stub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "proxy mati")
	})
	_, err := c.Health(ctx)
	var e *Error
	if !errors.As(err, &e) || e.Status != 502 || e.Code != CodeInternal || e.Message != "proxy mati" {
		t.Fatalf("body bukan JSON: %v", err)
	}
	if _, err := c.List(ctx, "a", 5); err == nil {
		t.Error("list")
	}
	if _, err := c.Verify(ctx, nil); err == nil {
		t.Error("verify")
	}
	if err := c.Delete(ctx, ShardRef{Blob: uuid.New()}); err == nil {
		t.Error("delete")
	}

	// agen menolak unggahan lebih dulu: galat agen yang dilaporkan, bukan "pipe tertutup"
	c = stub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInsufficientStorage)
		_, _ = io.WriteString(w, `{"code":"insufficient_storage","message":"penuh"}`)
	})
	u, err := c.Upload(ctx, shard.Header{BlobID: uuid.New(), Layout: shard.LayoutReplica, DataShards: 1, BlockSize: shard.MinBlockSize})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ { // tulis sampai pipe ditutup oleh jawaban agen
		if _, err := u.Write(make([]byte, 64<<10)); err != nil {
			break
		}
	}
	if err := u.Close(1, [32]byte{}); !errors.Is(err, ErrInsufficientStorage) {
		t.Fatalf("galat agen: %v", err)
	}
	if _, err := c.Upload(ctx, shard.Header{}); err == nil {
		t.Error("header tidak sah")
	}

	// meta di respons GET/HEAD rusak
	c = stub(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if _, err := c.Head(ctx, ShardRef{Blob: uuid.New()}); err == nil {
		t.Error("head tanpa meta")
	}
	if _, err := c.Get(ctx, ShardRef{Blob: uuid.New()}, 0, 1); err == nil {
		t.Error("get tanpa meta")
	}
	// URL tidak sah & koneksi gagal
	bad := &Client{BaseURL: "http://[::1]:namaport", Node: node, Token: token}
	if _, err := bad.Health(ctx); err == nil {
		t.Error("URL tidak sah")
	}
	if _, err := (&Client{BaseURL: "http://127.0.0.1:1", Node: node, Token: token}).Health(ctx); err == nil {
		t.Error("koneksi ditolak")
	}
}
