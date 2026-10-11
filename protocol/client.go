package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Onelabs9157/rinjing-agen/shard"
)

// Galat klien yang bisa dicek dengan errors.Is.
var (
	ErrNotFound            = errors.New("shard tidak ada di agen")
	ErrInsufficientStorage = errors.New("ruang disk agen tidak cukup")
)

// Error — respons galat dari agen.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("agen %d %s: %s", e.Status, e.Code, e.Message)
}

// Is — memetakan kode galat agen ke galat yang bisa dicek pemanggil.
func (e *Error) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.Code == CodeNotFound
	case shard.ErrCorrupt:
		return e.Code == CodeCorrupt
	case ErrUnauthorized:
		return e.Code == CodeUnauthorized || e.Code == CodeForbidden
	case ErrInsufficientStorage:
		return e.Code == CodeInsufficientStorage
	case shard.ErrTooLarge:
		return e.Code == CodeTooLarge
	}
	return false
}

// Client — klien satu agen. Aman dipakai bersamaan.
type Client struct {
	BaseURL string // mis. https://10.10.10.31:9100
	Node    string // kode node agen (NODE_...)
	Token   string
	HTTP    *http.Client     // nil = http.DefaultClient (sebaiknya isi: transport dengan TLS & timeout dial)
	Now     func() time.Time // nil = time.Now
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	u := strings.TrimRight(c.BaseURL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	Sign(req, c.Node, c.Token, now())
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, readError(resp)
	}
	return resp, nil
}

func readError(resp *http.Response) error {
	var e ErrorResponse
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if json.Unmarshal(raw, &e) != nil || e.Code == "" {
		e.Code, e.Message = statusCode(resp.StatusCode), strings.TrimSpace(string(raw))
	}
	return &Error{Status: resp.StatusCode, Code: e.Code, Message: e.Message}
}

// statusCode — kode galat untuk respons tanpa body JSON (mis. HEAD).
func statusCode(status int) string {
	switch status {
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusUnauthorized:
		return CodeUnauthorized
	case http.StatusForbidden:
		return CodeForbidden
	case http.StatusUnprocessableEntity:
		return CodeCorrupt
	case http.StatusInsufficientStorage:
		return CodeInsufficientStorage
	case http.StatusRequestEntityTooLarge:
		return CodeTooLarge
	case http.StatusBadRequest:
		return CodeBadRequest
	}
	return CodeInternal
}

func (c *Client) getJSON(ctx context.Context, method, path string, query url.Values, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	resp, err := c.do(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// Health — status & kompatibilitas agen.
func (c *Client) Health(ctx context.Context) (*Health, error) {
	var h Health
	if err := c.getJSON(ctx, http.MethodGet, "/v1/health", nil, nil, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// Head — meta shard tanpa isi; ErrNotFound bila tidak ada.
func (c *Client) Head(ctx context.Context, ref ShardRef) (*Meta, error) {
	resp, err := c.do(ctx, http.MethodHead, ShardPath(ref), nil, nil)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	m, err := ParseMeta(resp.Header, ref)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Delete — idempoten: shard yang sudah tidak ada bukan galat.
func (c *Client) Delete(ctx context.Context, ref ShardRef) error {
	resp, err := c.do(ctx, http.MethodDelete, ShardPath(ref), nil, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// List — daftar shard terurut, mulai setelah cursor after ("" = awal).
func (c *Client) List(ctx context.Context, after string, limit int) (*ListResponse, error) {
	q := url.Values{}
	if after != "" {
		q.Set("after", after)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out ListResponse
	if err := c.getJSON(ctx, http.MethodGet, "/v1/shards", q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Verify — minta agen memeriksa checksum shard di disknya sendiri (data tidak lewat jaringan).
func (c *Client) Verify(ctx context.Context, refs []ShardRef) (*VerifyResponse, error) {
	var out VerifyResponse
	if err := c.getJSON(ctx, http.MethodPost, "/v1/verify", nil, VerifyRequest{Shards: refs}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Blocks — respons GET: meta shard + blok-blok terverifikasi.
type Blocks struct {
	Meta  Meta
	First uint32 // nomor blok pertama di respons
	r     *shard.BlockReader
	body  io.ReadCloser
}

// Next — blok berikutnya (berlaku sampai panggilan berikutnya); io.EOF bila selesai; galat
// shard.ErrCorrupt bila checksum salah atau koneksi putus sebelum frame penutup.
func (b *Blocks) Next() ([]byte, error) { return b.r.Next() }

func (b *Blocks) Close() error { return b.body.Close() }

// Get — blok [first, first+count) shard ref. count 0 = sampai blok terakhir.
func (c *Client) Get(ctx context.Context, ref ShardRef, first, count uint32) (*Blocks, error) {
	q := url.Values{"first": {strconv.FormatUint(uint64(first), 10)}}
	if count > 0 {
		q.Set("count", strconv.FormatUint(uint64(count), 10))
	}
	resp, err := c.do(ctx, http.MethodGet, ShardPath(ref), q, nil)
	if err != nil {
		return nil, err
	}
	m, err := ParseMeta(resp.Header, ref)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	return &Blocks{Meta: m, First: first, r: shard.NewBlockReader(resp.Body, m.Header.BlockSize), body: resp.Body}, nil
}

// Upload — PUT shard yang sedang berjalan. Tulis payload shard lewat Write, lalu Close.
// Bila sumber gagal di tengah, panggil Abort: agen membuang file sementaranya.
type Upload struct {
	pw   *io.PipeWriter
	sw   *shard.StreamWriter
	done chan error
}

// Upload — mulai PUT shard h ke agen (body di-stream, ukuran tidak perlu diketahui di awal).
func (c *Client) Upload(ctx context.Context, h shard.Header) (*Upload, error) {
	pr, pw := io.Pipe()
	sw, err := shard.NewStreamWriter(&lazyHeader{pw: pw}, h)
	if err != nil {
		return nil, err
	}
	u := &Upload{pw: pw, sw: sw, done: make(chan error, 1)}
	ref := ShardRef{Blob: h.BlobID, Index: h.Index}
	go func() {
		resp, err := c.do(ctx, http.MethodPut, ShardPath(ref), nil, pr)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			err = resp.Body.Close()
		}
		pr.CloseWithError(err) // membebaskan Write yang masih menunggu bila agen sudah menjawab
		u.done <- err
	}()
	return u, nil
}

// lazyHeader — header shard ditulis NewStreamWriter sebelum goroutine request jalan; tahan dulu
// di memori supaya NewStreamWriter tidak macet menunggu pembaca pipe.
type lazyHeader struct {
	pw      *io.PipeWriter
	pending []byte
	started bool
}

func (l *lazyHeader) Write(p []byte) (int, error) {
	if !l.started {
		l.started = true
		l.pending = append([]byte(nil), p...)
		return len(p), nil
	}
	if l.pending != nil {
		head := l.pending
		l.pending = nil
		if _, err := l.pw.Write(head); err != nil {
			return 0, err
		}
	}
	return l.pw.Write(p)
}

func (u *Upload) Write(p []byte) (int, error) { return u.sw.Write(p) }

// Close — selesaikan stream (trailer berisi ukuran & sha256 blob asli) lalu tunggu jawaban agen.
func (u *Upload) Close(blobSize uint64, blobSHA256 [32]byte) error {
	if err := u.sw.Close(blobSize, blobSHA256); err != nil {
		u.pw.CloseWithError(err)
		if agentErr := <-u.done; agentErr != nil {
			return agentErr // agen sudah menolak lebih dulu (mis. 401/507): itu penyebab sebenarnya
		}
		return err
	}
	_ = u.pw.Close()
	return <-u.done
}

// Abort — batalkan unggahan; agen tidak menyimpan apa pun.
func (u *Upload) Abort(cause error) error {
	if cause == nil {
		cause = errors.New("unggahan shard dibatalkan")
	}
	u.pw.CloseWithError(cause)
	err := <-u.done
	if err == nil {
		return errors.New("shard: agen menerima unggahan yang dibatalkan")
	}
	return nil
}
