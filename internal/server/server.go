// Package server — HTTP API agen /v1 (kontrak di paket protocol).
package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"time"

	"github.com/Onelabs9157/rinjing-agen/protocol"
	"github.com/Onelabs9157/rinjing-agen/shard"
	"github.com/Onelabs9157/rinjing-agen/store"
)

// Server — dirakit di cmd/rinjing-agen.
type Server struct {
	Store   *store.Store
	Token   string
	Version string
	// Allow — IP/CIDR yang boleh memanggil /v1 (Rinjing utama). Kosong = semua (hanya bila
	// operator eksplisit memilih AGENT_ALLOW_FROM=*).
	Allow []netip.Prefix
	Now   func() time.Time
	Log   *slog.Logger

	verifySem chan struct{}
}

// Handler — /healthz (tanpa autentikasi, hanya "ok") + /v1/* (allowlist IP + tanda tangan).
func (s *Server) Handler() http.Handler {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	s.verifySem = make(chan struct{}, 1) // scrub membaca disk penuh: satu per agen sudah cukup berat
	api := http.NewServeMux()
	api.HandleFunc("GET /v1/health", s.health)
	api.HandleFunc("GET /v1/shards", s.list)
	api.HandleFunc("PUT /v1/shards/{blob}/{idx}", s.put)
	api.HandleFunc("GET /v1/shards/{blob}/{idx}", s.get)
	api.HandleFunc("HEAD /v1/shards/{blob}/{idx}", s.head)
	api.HandleFunc("DELETE /v1/shards/{blob}/{idx}", s.delete)
	api.HandleFunc("POST /v1/verify", s.verify)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.Handle("/v1/", s.guard(api))
	return s.logging(mux)
}

func (s *Server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.allowed(r.RemoteAddr) {
			writeErr(w, http.StatusForbidden, protocol.CodeForbidden, "alamat tidak diizinkan")
			return
		}
		if err := protocol.Verify(r, s.Store.Node(), s.Token, s.Now()); err != nil {
			writeErr(w, http.StatusUnauthorized, protocol.CodeUnauthorized, err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowed(remote string) bool {
	if len(s.Allow) == 0 {
		return true
	}
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	for _, p := range s.Allow {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// ---- handler ----

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	st, err := s.Store.Stat()
	if err != nil {
		s.Log.Warn("baca kapasitas disk", "err", err)
	}
	writeJSON(w, http.StatusOK, protocol.Health{Status: "ok", Node: s.Store.Node(), Version: s.Version,
		APIVersion: protocol.APIVersion, FormatVersion: shard.FormatVersion,
		DiskKnown: st.Known, TotalBytes: st.Total, FreeBytes: st.Free, MinFreeBytes: s.Store.MinFree})
}

func ref(w http.ResponseWriter, r *http.Request) (protocol.ShardRef, bool) {
	ref, err := protocol.ParseShardRef(r.PathValue("blob"), r.PathValue("idx"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, err.Error())
		return ref, false
	}
	return ref, true
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	ref, ok := ref(w, r)
	if !ok {
		return
	}
	_, t, err := s.Store.Put(r.Context(), ref, r.Body)
	if err != nil {
		s.storeErr(w, ref, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"payload_size": t.PayloadSize, "blocks": t.Blocks})
}

func (s *Server) open(w http.ResponseWriter, r *http.Request) (*store.Shard, bool) {
	ref, ok := ref(w, r)
	if !ok {
		return nil, false
	}
	sh, err := s.Store.Open(ref)
	if err != nil {
		s.storeErr(w, ref, err)
		return nil, false
	}
	return sh, true
}

func meta(sh *store.Shard) protocol.Meta {
	t := sh.Tail()
	return protocol.Meta{Header: sh.Header(), PayloadSize: t.PayloadSize, BlobSize: t.BlobSize,
		BlobSHA256: t.BlobSHA256, Blocks: t.Blocks}
}

func (s *Server) head(w http.ResponseWriter, r *http.Request) {
	sh, ok := s.open(w, r)
	if !ok {
		return
	}
	defer sh.Close()
	protocol.SetMeta(w.Header(), meta(sh))
	w.WriteHeader(http.StatusOK)
}

// get — blok [first, first+count) sebagai frame (paket shard). Blok pertama dibaca & diverifikasi
// SEBELUM status dikirim, jadi shard yang rusak di awal dijawab 422. Rusak di tengah → koneksi
// diputus tanpa frame penutup, dan klien melihatnya sebagai shard.ErrCorrupt.
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	first, err1 := parseUint32(q.Get("first"), 0)
	count, err2 := parseUint32(q.Get("count"), 0)
	if err1 != nil || err2 != nil {
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, "first/count tidak sah")
		return
	}
	sh, ok := s.open(w, r)
	if !ok {
		return
	}
	defer sh.Close()
	blocks := sh.Tail().Blocks
	if first > blocks {
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, "blok di luar shard")
		return
	}
	end := blocks
	if count > 0 && uint64(first)+uint64(count) < uint64(blocks) {
		end = first + count
	}
	buf := make([]byte, sh.Header().BlockSize)
	var pending []byte
	var pendingCRC uint32
	if first < end {
		var err error
		if pending, pendingCRC, err = sh.ReadBlock(first, buf); err != nil {
			s.storeErr(w, sh.Info.ShardRef, err)
			return
		}
	}
	protocol.SetMeta(w.Header(), meta(sh))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	for i := first; i < end; i++ {
		if i > first {
			var err error
			if pending, pendingCRC, err = sh.ReadBlock(i, buf); err != nil {
				s.Log.Error("shard rusak saat dikirim", "shard", sh.Info.ShardRef.String(), "blok", i, "err", err)
				panic(http.ErrAbortHandler) // putus tanpa frame penutup
			}
		}
		if err := shard.WriteBlock(w, pending, pendingCRC); err != nil {
			return // klien pergi
		}
	}
	_ = shard.WriteEnd(w)
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	ref, ok := ref(w, r)
	if !ok {
		return
	}
	if err := s.Store.Delete(ref); err != nil {
		s.storeErr(w, ref, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	shards, next, err := s.Store.List(r.Context(), q.Get("after"), limit)
	if err != nil {
		s.storeErr(w, protocol.ShardRef{}, err)
		return
	}
	if shards == nil {
		shards = []protocol.ShardInfo{}
	}
	writeJSON(w, http.StatusOK, protocol.ListResponse{Shards: shards, Next: next})
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	var req protocol.VerifyRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, "body JSON tidak sah")
		return
	}
	if len(req.Shards) > protocol.MaxVerifyBatch {
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, "terlalu banyak shard dalam satu permintaan")
		return
	}
	select {
	case s.verifySem <- struct{}{}:
		defer func() { <-s.verifySem }()
	case <-r.Context().Done():
		return
	}
	missing, corrupt, err := s.Store.Verify(r.Context(), req.Shards)
	if err != nil {
		s.storeErr(w, protocol.ShardRef{}, err)
		return
	}
	if missing == nil {
		missing = []protocol.ShardRef{}
	}
	if corrupt == nil {
		corrupt = []protocol.ShardRef{}
	}
	writeJSON(w, http.StatusOK, protocol.VerifyResponse{Missing: missing, Corrupt: corrupt})
}

// ---- respons ----

func (s *Server) storeErr(w http.ResponseWriter, ref protocol.ShardRef, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, protocol.CodeNotFound, "shard tidak ada")
	case errors.Is(err, store.ErrMismatch), errors.Is(err, shard.ErrFormat):
		writeErr(w, http.StatusBadRequest, protocol.CodeBadRequest, err.Error())
	case errors.Is(err, shard.ErrCorrupt):
		s.Log.Warn("shard rusak", "shard", ref.String(), "err", err)
		writeErr(w, http.StatusUnprocessableEntity, protocol.CodeCorrupt, err.Error())
	case errors.Is(err, shard.ErrTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, protocol.CodeTooLarge, err.Error())
	case errors.Is(err, store.ErrNoSpace):
		writeErr(w, http.StatusInsufficientStorage, protocol.CodeInsufficientStorage, "ruang disk agen tidak cukup")
	default:
		s.Log.Error("galat store", "shard", ref.String(), "err", err)
		writeErr(w, http.StatusInternalServerError, protocol.CodeInternal, "galat internal agen")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, protocol.ErrorResponse{Code: code, Message: msg})
}

func parseUint32(v string, def uint32) (uint32, error) {
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	return uint32(n), err
}

// ---- log ----

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if r.URL.Path == "/healthz" {
				return
			}
			s.Log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.status,
				"ms", time.Since(start).Milliseconds(), "remote", r.RemoteAddr)
		}()
		next.ServeHTTP(sw, r)
	})
}
