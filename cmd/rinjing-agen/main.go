// rinjing-agen — penyimpan shard untuk Rinjing cluster.
//
//	rinjing-agen              jalankan server (konfigurasi dari env, lihat .env.example)
//	rinjing-agen token        buat AGENT_TOKEN acak
//	rinjing-agen healthcheck  cek /healthz lokal (dipakai HEALTHCHECK Docker)
//	rinjing-agen probe        uji ujung-ke-ujung agen lain (AGENT_PROBE_URL, AGENT_NODE_CODE, AGENT_TOKEN)
//	rinjing-agen version
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Onelabs9157/rinjing-agen/internal/config"
	"github.com/Onelabs9157/rinjing-agen/internal/server"
	"github.com/Onelabs9157/rinjing-agen/store"
)

// version — diisi saat build: -ldflags "-X main.version=v0.1.0".
var version = "dev"

func main() {
	cmd := ""
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cmd, os.Stdout); err != nil {
		stop()
		fmt.Fprintln(os.Stderr, "rinjing-agen:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, out io.Writer) error {
	switch cmd {
	case "":
		return serve(ctx, os.Getenv)
	case "token":
		return newToken(out)
	case "healthcheck":
		return healthcheck(os.Getenv("AGENT_LISTEN"), os.Getenv("AGENT_TLS_CERT") != "")
	case "probe":
		return probe(ctx, os.Getenv, out)
	case "version":
		_, err := fmt.Fprintln(out, version)
		return err
	}
	return fmt.Errorf("perintah %q tidak dikenal (token | healthcheck | probe | version)", cmd)
}

func newToken(out io.Writer) error {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, hex.EncodeToString(b))
	return err
}

// healthcheck — GET /healthz ke agen di mesin yang sama, di alamat yang benar-benar didengar
// (AGENT_LISTEN=10.10.10.60:9100 tidak menjawab di 127.0.0.1).
func healthcheck(listen string, useTLS bool) error {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		host, port = "", "9100"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	scheme := "http"
	hc := &http.Client{Timeout: 5 * time.Second}
	if useTLS {
		scheme = "https"
		// sertifikat diterbitkan untuk IP/nama agen, bukan 127.0.0.1; yang dicek hanya "hidup"
		hc.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // loopback
	}
	resp, err := hc.Get(scheme + "://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz %d", resp.StatusCode)
	}
	return nil
}

// serve — jalan sampai ctx selesai (SIGINT/SIGTERM), lalu berhenti dengan rapi.
func serve(ctx context.Context, getenv func(string) string) error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}
	st, err := store.Open(cfg.DataDir, cfg.Node)
	if err != nil {
		return err
	}
	st.MinFree, st.MaxPayload = cfg.MinFree, cfg.MaxPayload
	if cfg.AllowAny {
		log.Warn("AGENT_ALLOW_FROM=* — API agen menerima koneksi dari alamat mana pun; batasi dengan firewall")
	}
	srv := &server.Server{Store: st, Token: cfg.Token, Version: version, Allow: cfg.Allow, Log: log}
	hs := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		// tanpa ReadTimeout/WriteTimeout: shard besar di-stream lama; koneksi mati dideteksi TCP
	}

	go sweepLoop(ctx, st, cfg.TmpMaxAge, log)

	errc := make(chan error, 1)
	go func() {
		log.Info("rinjing-agen jalan", "version", version, "node", cfg.Node, "listen", cfg.Listen,
			"data", cfg.DataDir, "tls", cfg.TLSCert != "")
		if cfg.TLSCert != "" {
			errc <- hs.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
		} else {
			errc <- hs.ListenAndServe()
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("berhenti — menunggu request berjalan selesai")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := hs.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// sweepLoop — bersihkan tmp unggahan yang mati di tengah, tiap jam.
func sweepLoop(ctx context.Context, st *store.Store, maxAge time.Duration, log *slog.Logger) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		if n, err := st.SweepTmp(maxAge); err != nil {
			log.Warn("sapu tmp", "err", err)
		} else if n > 0 {
			log.Info("sapu tmp", "dihapus", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
