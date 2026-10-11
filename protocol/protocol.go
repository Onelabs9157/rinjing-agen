// Package protocol — kontrak API agen /v1: path, tanda tangan request, tipe JSON, header meta
// shard, dan klien Go. Diimpor Rinjing utama; server agen memakai fungsi yang sama.
package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/shard"
)

const (
	// APIVersion — naik bila endpoint/semantik /v1 berubah tidak kompatibel.
	APIVersion = 1

	// AuthScheme — Authorization: RJ1 node=<kode>,ts=<unix>,sig=<hex hmac-sha256>
	AuthScheme = "RJ1"
	// MaxClockSkew — selisih jam utama↔agen yang masih diterima (semua node wajib NTP).
	MaxClockSkew = 5 * time.Minute

	// MaxVerifyBatch — batas jumlah shard per POST /v1/verify.
	MaxVerifyBatch = 1000
)

// Kode galat JSON {"code","message"}.
const (
	CodeBadRequest          = "bad_request"
	CodeUnauthorized        = "unauthorized"
	CodeForbidden           = "forbidden"
	CodeNotFound            = "not_found"
	CodeCorrupt             = "corrupt"
	CodeTooLarge            = "too_large"
	CodeInsufficientStorage = "insufficient_storage"
	CodeInternal            = "internal"
)

// Header meta shard pada respons HEAD/GET.
const (
	HeaderLayout       = "X-Rinjing-Layout"
	HeaderDataShards   = "X-Rinjing-Data-Shards"
	HeaderParityShards = "X-Rinjing-Parity-Shards"
	HeaderBlockSize    = "X-Rinjing-Block-Size"
	HeaderPayloadSize  = "X-Rinjing-Payload-Size"
	HeaderBlobSize     = "X-Rinjing-Blob-Size"
	HeaderBlobSHA256   = "X-Rinjing-Blob-Sha256"
	HeaderBlocks       = "X-Rinjing-Blocks"
)

// ShardPath — /v1/shards/{blob}/{idx}.
func ShardPath(ref ShardRef) string {
	return "/v1/shards/" + ref.Blob.String() + "/" + strconv.Itoa(int(ref.Index))
}

// ParseShardRef — dari nilai path {blob} & {idx}.
func ParseShardRef(blob, idx string) (ShardRef, error) {
	id, err := uuid.Parse(blob)
	if err != nil || id == uuid.Nil {
		return ShardRef{}, fmt.Errorf("blob id %q tidak sah", blob)
	}
	n, err := strconv.ParseUint(idx, 10, 8)
	if err != nil {
		return ShardRef{}, fmt.Errorf("nomor shard %q tidak sah", idx)
	}
	return ShardRef{Blob: id, Index: uint8(n)}, nil
}

// ---- tanda tangan ----

// ErrUnauthorized — tanda tangan tidak ada/salah/kedaluwarsa.
var ErrUnauthorized = errors.New("tanda tangan request tidak sah")

// canonical — yang ditandatangani: skema, metode, path, query terurut, node, waktu.
// Body TIDAK ditandatangani: integritasnya dijaga checksum per blok (paket shard), dan
// kerahasiaan oleh TLS. Request yang direkam ulang dalam MaxClockSkew hanya bisa mengulang
// operasi idempoten atas blob immutable (lihat README §Keamanan).
func canonical(method, path, rawQuery, node string, ts int64) string {
	q, _ := url.ParseQuery(rawQuery)
	return strings.Join([]string{AuthScheme, method, path, q.Encode(), node, strconv.FormatInt(ts, 10)}, "\n")
}

func signature(token, msg string) string {
	m := hmac.New(sha256.New, []byte(token))
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))
}

// Sign — pasang header Authorization pada req.
func Sign(req *http.Request, node, token string, now time.Time) {
	ts := now.Unix()
	sig := signature(token, canonical(req.Method, req.URL.Path, req.URL.RawQuery, node, ts))
	req.Header.Set("Authorization", fmt.Sprintf("%s node=%s,ts=%d,sig=%s", AuthScheme, node, ts, sig))
}

// Verify — cek tanda tangan req untuk node & token agen ini.
func Verify(req *http.Request, node, token string, now time.Time) error {
	scheme, rest, ok := strings.Cut(req.Header.Get("Authorization"), " ")
	if !ok || scheme != AuthScheme {
		return ErrUnauthorized
	}
	f := map[string]string{}
	for part := range strings.SplitSeq(rest, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		f[k] = v
	}
	ts, err := strconv.ParseInt(f["ts"], 10, 64)
	if err != nil || f["node"] != node {
		return ErrUnauthorized
	}
	if d := now.Sub(time.Unix(ts, 0)); d > MaxClockSkew || d < -MaxClockSkew {
		return fmt.Errorf("%w: selisih jam %s", ErrUnauthorized, d.Round(time.Second))
	}
	want := signature(token, canonical(req.Method, req.URL.Path, req.URL.RawQuery, node, ts))
	if !hmac.Equal([]byte(want), []byte(f["sig"])) {
		return ErrUnauthorized
	}
	return nil
}

// ---- tipe JSON ----

// ShardRef — identitas shard di agen.
type ShardRef struct {
	Blob  uuid.UUID `json:"blob"`
	Index uint8     `json:"idx"`
}

func (r ShardRef) String() string { return r.Blob.String() + "." + strconv.Itoa(int(r.Index)) }

// Health — GET /v1/health (butuh tanda tangan).
type Health struct {
	Status        string `json:"status"` // "ok"
	Node          string `json:"node"`
	Version       string `json:"version"` // versi build agen
	APIVersion    int    `json:"api_version"`
	FormatVersion int    `json:"format_version"`
	DiskKnown     bool   `json:"disk_known"`
	TotalBytes    uint64 `json:"total_bytes"`
	FreeBytes     uint64 `json:"free_bytes"`
	MinFreeBytes  uint64 `json:"min_free_bytes"`
}

// Compatible — utama hanya menaruh shard di agen yang lolos cek ini.
func (h Health) Compatible() error {
	if h.APIVersion != APIVersion || h.FormatVersion != shard.FormatVersion {
		return fmt.Errorf("agen %s: API v%d format v%d, utama API v%d format v%d",
			h.Node, h.APIVersion, h.FormatVersion, APIVersion, shard.FormatVersion)
	}
	return nil
}

// ShardInfo — satu baris GET /v1/shards.
type ShardInfo struct {
	ShardRef
	FileSize int64     `json:"file_size"`
	ModTime  time.Time `json:"mod_time"`
}

// ListResponse — Next kosong = halaman terakhir; selain itu kirim sebagai ?after=.
type ListResponse struct {
	Shards []ShardInfo `json:"shards"`
	Next   string      `json:"next"`
}

type VerifyRequest struct {
	Shards []ShardRef `json:"shards"`
}

// VerifyResponse — shard yang tidak ada / gagal checksum. Yang tidak disebut = sehat.
type VerifyResponse struct {
	Missing []ShardRef `json:"missing"`
	Corrupt []ShardRef `json:"corrupt"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ---- meta shard di header HTTP ----

// Meta — kerangka shard (header + tail) tanpa isi blok.
type Meta struct {
	Header      shard.Header
	PayloadSize uint64
	BlobSize    uint64
	BlobSHA256  [32]byte
	Blocks      uint32
}

// SetMeta — tulis meta ke header respons (dipakai server).
func SetMeta(h http.Header, m Meta) {
	h.Set(HeaderLayout, m.Header.Layout.String())
	h.Set(HeaderDataShards, strconv.Itoa(int(m.Header.DataShards)))
	h.Set(HeaderParityShards, strconv.Itoa(int(m.Header.ParityShards)))
	h.Set(HeaderBlockSize, strconv.FormatUint(uint64(m.Header.BlockSize), 10))
	h.Set(HeaderPayloadSize, strconv.FormatUint(m.PayloadSize, 10))
	h.Set(HeaderBlobSize, strconv.FormatUint(m.BlobSize, 10))
	h.Set(HeaderBlobSHA256, hex.EncodeToString(m.BlobSHA256[:]))
	h.Set(HeaderBlocks, strconv.FormatUint(uint64(m.Blocks), 10))
}

// ParseMeta — kebalikan SetMeta (dipakai klien).
func ParseMeta(h http.Header, ref ShardRef) (Meta, error) {
	num := func(k string, bits int) uint64 {
		v, err := strconv.ParseUint(h.Get(k), 10, bits)
		if err != nil {
			return 0
		}
		return v
	}
	m := Meta{Header: shard.Header{BlobID: ref.Blob, Index: ref.Index,
		DataShards: uint8(num(HeaderDataShards, 8)), ParityShards: uint8(num(HeaderParityShards, 8)), //nolint:gosec // bits 8
		BlockSize: uint32(num(HeaderBlockSize, 32))}, //nolint:gosec // bits 32
		PayloadSize: num(HeaderPayloadSize, 64), BlobSize: num(HeaderBlobSize, 64),
		Blocks: uint32(num(HeaderBlocks, 32))} //nolint:gosec // bits 32
	switch h.Get(HeaderLayout) {
	case "replica":
		m.Header.Layout = shard.LayoutReplica
	case "ec":
		m.Header.Layout = shard.LayoutEC
	}
	sum, err := hex.DecodeString(h.Get(HeaderBlobSHA256))
	if err != nil || len(sum) != 32 {
		return Meta{}, fmt.Errorf("meta shard %s: sha256 tidak sah", ref)
	}
	copy(m.BlobSHA256[:], sum)
	if err := m.Header.Validate(); err != nil {
		return Meta{}, fmt.Errorf("meta shard %s: %w", ref, err)
	}
	if m.Blocks != shard.Blocks(m.PayloadSize, m.Header.BlockSize) {
		return Meta{}, fmt.Errorf("meta shard %s: jumlah blok tidak cocok", ref)
	}
	return m, nil
}
