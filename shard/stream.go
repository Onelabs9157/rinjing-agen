package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Format di jaringan. Setiap blok dibungkus frame dengan checksum-nya sendiri, sehingga penerima
// menolak blok rusak SAAT ITU JUGA (tidak menunggu akhir stream, tidak bergantung pada TLS):
//
//	frame     = [panjang u32][crc32c u32][data]
//	frame end = [0 u32][0 u32]
//
//	body PUT  = header 64 B · frame… · frame end · trailer (ukuran blob u64, sha256 blob 32 B, crc u32)
//	body GET  = frame… · frame end
//
// Semua frame berisi tepat BlockSize byte kecuali frame data terakhir.

const (
	frameHeaderSize = 8
	trailerSize     = 8 + 32 + 4
)

// ErrTooLarge — payload melebihi batas penerima.
var ErrTooLarge = errors.New("shard melebihi batas ukuran")

func writeFrame(w io.Writer, data []byte, crc uint32) error {
	var h [frameHeaderSize]byte
	binary.BigEndian.PutUint32(h[0:4], uint32(len(data))) //nolint:gosec // ≤ MaxBlockSize
	binary.BigEndian.PutUint32(h[4:8], crc)
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

// frameReader — membaca & memverifikasi frame, menegakkan aturan ukuran blok.
type frameReader struct {
	r         io.Reader
	blockSize uint32
	buf       []byte
	short     bool // blok pendek sudah lewat → frame berikutnya wajib end
	payload   uint64
	max       uint64 // 0 = tanpa batas
}

func unexpected(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: stream terpotong", ErrCorrupt)
	}
	return err
}

// next — blok berikutnya yang sudah lolos checksum (slice berlaku sampai panggilan berikutnya),
// atau io.EOF saat frame end.
func (f *frameReader) next() ([]byte, uint32, error) {
	var h [frameHeaderSize]byte
	if _, err := io.ReadFull(f.r, h[:]); err != nil {
		return nil, 0, unexpected(err)
	}
	n, crc := binary.BigEndian.Uint32(h[0:4]), binary.BigEndian.Uint32(h[4:8])
	if n == 0 {
		if crc != 0 {
			return nil, 0, fmt.Errorf("%w: frame end tidak sah", ErrFormat)
		}
		return nil, 0, io.EOF
	}
	if n > f.blockSize || f.short {
		return nil, 0, fmt.Errorf("%w: frame %d byte (blok %d, sudah ada blok pendek: %v)", ErrFormat, n, f.blockSize, f.short)
	}
	if f.max > 0 && f.payload+uint64(n) > f.max {
		return nil, 0, ErrTooLarge
	}
	if cap(f.buf) < int(n) {
		f.buf = make([]byte, f.blockSize)
	}
	data := f.buf[:n]
	if _, err := io.ReadFull(f.r, data); err != nil {
		return nil, 0, unexpected(err)
	}
	if Checksum(data) != crc {
		return nil, 0, fmt.Errorf("%w: blok ke-%d", ErrCorrupt, Blocks(f.payload, f.blockSize))
	}
	f.payload += uint64(n)
	f.short = n < f.blockSize
	return data, crc, nil
}

// ---- PUT: utama → agen ----

// StreamWriter — membuat body PUT dari aliran byte shard. Dipakai Rinjing utama.
type StreamWriter struct {
	w       io.Writer
	h       Header
	buf     []byte
	payload uint64
	closed  bool
}

// NewStreamWriter memvalidasi h dan langsung menulis header.
func NewStreamWriter(w io.Writer, h Header) (*StreamWriter, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	if _, err := w.Write(h.Marshal()); err != nil {
		return nil, err
	}
	return &StreamWriter{w: w, h: h, buf: make([]byte, 0, h.BlockSize)}, nil
}

// Write — byte payload shard; dipotong per BlockSize. Menulis blok penuh berarti tanpa salinan.
func (s *StreamWriter) Write(p []byte) (int, error) {
	if s.closed {
		return 0, errors.New("shard: Write setelah Close")
	}
	written := 0
	bs := int(s.h.BlockSize)
	for len(p) > 0 {
		if len(s.buf) == 0 && len(p) >= bs { // blok penuh langsung dari p
			if err := writeFrame(s.w, p[:bs], Checksum(p[:bs])); err != nil {
				return written, err
			}
			s.payload += uint64(bs) //nolint:gosec // bs > 0
			p, written = p[bs:], written+bs
			continue
		}
		n := min(bs-len(s.buf), len(p))
		s.buf = append(s.buf, p[:n]...)
		p, written = p[n:], written+n
		if len(s.buf) == bs {
			if err := s.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (s *StreamWriter) flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	if err := writeFrame(s.w, s.buf, Checksum(s.buf)); err != nil {
		return err
	}
	s.payload += uint64(len(s.buf))
	s.buf = s.buf[:0]
	return nil
}

// Close — sisa blok, frame end, lalu trailer berisi ukuran & sha256 blob ASLI (bukan shard).
func (s *StreamWriter) Close(blobSize uint64, blobSHA256 [32]byte) error {
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.flush(); err != nil {
		return err
	}
	if err := writeFrame(s.w, nil, 0); err != nil {
		return err
	}
	t := make([]byte, trailerSize)
	binary.BigEndian.PutUint64(t[0:8], blobSize)
	copy(t[8:40], blobSHA256[:])
	binary.BigEndian.PutUint32(t[40:44], Checksum(t[0:40]))
	_, err := s.w.Write(t)
	return err
}

// Receive — membaca body PUT dari src, memverifikasi setiap blok, dan menulis file shard ke dst.
// check dipanggil dengan header sebelum byte payload apa pun ditulis (mis. cocokkan dengan URL).
// maxPayload 0 = tanpa batas. Dipakai agen.
func Receive(dst io.Writer, src io.Reader, maxPayload uint64, check func(Header) error) (Header, Tail, error) {
	hb := make([]byte, HeaderSize)
	if _, err := io.ReadFull(src, hb); err != nil {
		return Header{}, Tail{}, unexpected(err)
	}
	h, err := ParseHeader(hb)
	if err != nil {
		return Header{}, Tail{}, err
	}
	if check != nil {
		if err := check(h); err != nil {
			return h, Tail{}, err
		}
	}
	fw, err := NewFileWriter(dst, h)
	if err != nil {
		return h, Tail{}, err
	}
	fr := &frameReader{r: src, blockSize: h.BlockSize, max: maxPayload}
	for {
		data, crc, err := fr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return h, Tail{}, err
		}
		if err := fw.WriteBlock(data, crc); err != nil {
			return h, Tail{}, err
		}
	}
	tb := make([]byte, trailerSize)
	if _, err := io.ReadFull(src, tb); err != nil {
		return h, Tail{}, unexpected(err)
	}
	if Checksum(tb[0:40]) != binary.BigEndian.Uint32(tb[40:44]) {
		return h, Tail{}, fmt.Errorf("%w: trailer", ErrCorrupt)
	}
	var sum [32]byte
	copy(sum[:], tb[8:40])
	t, err := fw.Finish(binary.BigEndian.Uint64(tb[0:8]), sum)
	return h, t, err
}

// ---- GET: agen → utama ----

// WriteBlock — satu frame blok untuk respons GET (crc = checksum yang sudah diverifikasi).
func WriteBlock(w io.Writer, data []byte, crc uint32) error {
	if len(data) == 0 {
		return errors.New("shard: blok kosong")
	}
	return writeFrame(w, data, crc)
}

// WriteEnd — frame penutup respons GET. Tanpa ini penerima menganggap respons terpotong.
func WriteEnd(w io.Writer) error { return writeFrame(w, nil, 0) }

// BlockReader — membaca respons GET, mengembalikan blok yang sudah lolos checksum.
type BlockReader struct{ fr frameReader }

// NewBlockReader — blockSize = Header.BlockSize shard yang diminta.
func NewBlockReader(r io.Reader, blockSize uint32) *BlockReader {
	return &BlockReader{fr: frameReader{r: r, blockSize: blockSize}}
}

// Next — blok berikutnya (berlaku sampai panggilan berikutnya), io.EOF di frame end,
// ErrCorrupt bila checksum salah atau stream terputus sebelum frame end.
func (b *BlockReader) Next() ([]byte, error) {
	data, _, err := b.fr.next()
	return data, err
}
