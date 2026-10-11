package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Onelabs9157/rinjing-agen/internal/server"
	"github.com/Onelabs9157/rinjing-agen/store"
)

func TestPerintah(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	if err := run(ctx, "token", &out); err != nil || len(strings.TrimSpace(out.String())) != 64 {
		t.Fatalf("token: %q %v", out.String(), err)
	}
	out.Reset()
	if err := run(ctx, "version", &out); err != nil || strings.TrimSpace(out.String()) != version {
		t.Fatalf("version: %q", out.String())
	}
	if err := run(ctx, "hapus-semua", &out); err == nil {
		t.Fatal("perintah tak dikenal")
	}
	t.Setenv("AGENT_NODE_CODE", "")
	if err := run(ctx, "", &out); err == nil {
		t.Fatal("serve tanpa konfigurasi harus gagal")
	}
}

func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ok.Close()
	u, _ := url.Parse(ok.URL)
	if err := healthcheck(":"+u.Port(), false); err != nil {
		t.Fatal(err)
	}
	if err := healthcheck("127.0.0.1:"+u.Port(), false); err != nil {
		t.Fatalf("IP spesifik: %v", err)
	}
	if err := healthcheck("10.255.255.1:"+u.Port(), false); err == nil {
		t.Fatal("harus memanggil IP yang didengar, bukan selalu loopback")
	}
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	u, _ = url.Parse(down.URL)
	if err := healthcheck(":"+u.Port(), false); err == nil {
		t.Fatal("503 harus gagal")
	}
	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer tlsSrv.Close()
	u, _ = url.Parse(tlsSrv.URL)
	if err := healthcheck("0.0.0.0:"+u.Port(), true); err != nil {
		t.Fatalf("tls: %v", err)
	}
	if err := healthcheck(":1", false); err == nil {
		t.Fatal("port tertutup")
	}
}

func TestServeBerhentiRapi(t *testing.T) {
	dir := t.TempDir()
	env := map[string]string{
		"AGENT_LISTEN": "127.0.0.1:0", "AGENT_DATA_DIR": dir, "AGENT_NODE_CODE": "NODE_UJI",
		"AGENT_TOKEN": strings.Repeat("t", 40), "AGENT_ALLOW_FROM": "*",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, func(k string) string { return env[k] }) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve tidak berhenti setelah ctx selesai")
	}
	// folder yang sama untuk node lain ditolak
	env["AGENT_NODE_CODE"] = "NODE_LAIN"
	if err := serve(context.Background(), func(k string) string { return env[k] }); err == nil {
		t.Fatal("folder milik node lain")
	}
	// port sudah dipakai → ListenAndServe gagal dan serve mengembalikannya (ctx bertenggat
	// supaya test tidak macet bila Listen ternyata berhasil)
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	env["AGENT_NODE_CODE"], env["AGENT_LISTEN"] = "NODE_UJI", busy.Addr().String()
	tctx, tcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tcancel()
	if err := serve(tctx, func(k string) string { return env[k] }); err == nil {
		t.Fatal("port bentrok harus galat")
	}
}

func TestProbe(t *testing.T) {
	st, err := store.Open(t.TempDir(), "NODE_UJI")
	if err != nil {
		t.Fatal(err)
	}
	tok := strings.Repeat("k", 40)
	srv := httptest.NewServer((&server.Server{Store: st, Token: tok, Version: "uji",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler())
	defer srv.Close()
	env := map[string]string{"AGENT_PROBE_URL": srv.URL, "AGENT_NODE_CODE": "NODE_UJI", "AGENT_TOKEN": tok, "AGENT_PROBE_SIZE": "2500KiB"}
	get := func(k string) string { return env[k] }
	var out bytes.Buffer
	if err := probe(context.Background(), get, &out); err != nil || !strings.Contains(out.String(), "PROBE LULUS") {
		t.Fatalf("probe: %v — %s", err, out.String())
	}
	if l, _, _ := st.List(context.Background(), "", 0); len(l) != 0 {
		t.Fatalf("shard uji tertinggal: %v", l)
	}
	env["AGENT_TOKEN"] = strings.Repeat("x", 40)
	if err := probe(context.Background(), get, &out); err == nil || !strings.Contains(err.Error(), "health") {
		t.Fatalf("token salah: %v", err)
	}
	env["AGENT_PROBE_SIZE"] = "banyak"
	if err := probe(context.Background(), get, &out); err == nil {
		t.Fatal("ukuran tidak sah")
	}
	if err := probe(context.Background(), func(string) string { return "" }, &out); err == nil {
		t.Fatal("tanpa konfigurasi")
	}
	if human(0, false) != "tidak diketahui" || human(3<<30, true) != "3.0 GiB" || !strings.Contains(rate(1e6, 0), "MB/s") {
		t.Fatal("format angka")
	}
}
