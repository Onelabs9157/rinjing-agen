// Package shard — format potongan (shard) blob Rinjing: di disk agen dan di jaringan.
//
// Paket ini SUMBER TUNGGAL format. Rinjing utama mengimpornya untuk menulis & membaca shard,
// agen memakainya untuk menyimpan & memverifikasi. Jangan menyalin kodenya ke repo lain.
//
// File shard di disk (FormatVersion 1):
//
//	[header 64 B][blok 0][blok 1]…[blok n-1][tabel CRC32C n×4 B][tail 64 B]
//
// Semua blok berukuran tepat Header.BlockSize kecuali blok terakhir (boleh lebih pendek), jadi
// offset byte → nomor blok cukup dibagi. Header mendeskripsikan diri (blob id, nomor shard, k, m),
// sehingga peta lokasi bisa dibangun ulang dengan memindai agen bila metadata Postgres hilang.
// Semua angka big-endian.
package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/google/uuid"
)

const (
	// FormatVersion — naik bila tata letak byte berubah. Agen selalu bisa membaca versi lama.
	FormatVersion = 1

	HeaderSize = 64
	TailSize   = 64

	MinBlockSize = 4 << 10
	MaxBlockSize = 16 << 20
)

var (
	headerMagic = [8]byte{'R', 'J', 'S', 'H', 'A', 'R', 'D', 0}
	tailMagic   = [4]byte{'R', 'J', 'S', 'E'}

	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

var (
	// ErrCorrupt — byte tidak cocok dengan checksum-nya (bitrot, file terpotong, transmisi rusak).
	ErrCorrupt = errors.New("shard rusak")
	// ErrFormat — bukan file/stream shard, atau versi format tidak dikenal.
	ErrFormat = errors.New("format shard tidak dikenal")
)

// Checksum — CRC32C (Castagnoli) yang dipakai untuk setiap blok, tabel, header, dan tail.
func Checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Layout — cara blob disebar.
type Layout uint8

const (
	// LayoutReplica — shard berisi blob utuh; Index = nomor salinan, DataShards = 1.
	LayoutReplica Layout = 1
	// LayoutEC — erasure coding Reed-Solomon; shard ke-Index dari DataShards+ParityShards.
	LayoutEC Layout = 2
)

func (l Layout) String() string {
	switch l {
	case LayoutReplica:
		return "replica"
	case LayoutEC:
		return "ec"
	}
	return fmt.Sprintf("layout(%d)", uint8(l))
}

// Header — diketahui SEBELUM data dikirim (ukuran & hash blob ada di Tail).
type Header struct {
	BlobID       uuid.UUID
	Layout       Layout
	Index        uint8 // nomor shard (EC) atau nomor salinan (replika)
	DataShards   uint8 // k (replika: 1)
	ParityShards uint8 // m (replika: jumlah salinan − 1)
	BlockSize    uint32
}

// Validate — kombinasi field yang sah. Dipanggil saat menulis maupun membaca.
func (h Header) Validate() error {
	if h.BlobID == uuid.Nil {
		return fmt.Errorf("%w: blob id kosong", ErrFormat)
	}
	if h.BlockSize < MinBlockSize || h.BlockSize > MaxBlockSize {
		return fmt.Errorf("%w: block size %d di luar %d–%d", ErrFormat, h.BlockSize, MinBlockSize, MaxBlockSize)
	}
	total := int(h.DataShards) + int(h.ParityShards)
	switch h.Layout {
	case LayoutReplica:
		if h.DataShards != 1 {
			return fmt.Errorf("%w: replika harus DataShards=1", ErrFormat)
		}
	case LayoutEC:
		if h.DataShards < 1 || h.ParityShards < 1 || total > 256 {
			return fmt.Errorf("%w: EC %d+%d tidak sah", ErrFormat, h.DataShards, h.ParityShards)
		}
	default:
		return fmt.Errorf("%w: layout %d", ErrFormat, h.Layout)
	}
	if int(h.Index) >= total {
		return fmt.Errorf("%w: index %d ≥ %d shard", ErrFormat, h.Index, total)
	}
	return nil
}

// Marshal — 64 byte header. Pemanggil wajib Validate dulu.
func (h Header) Marshal() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:8], headerMagic[:])
	binary.BigEndian.PutUint16(b[8:10], FormatVersion)
	b[10] = byte(h.Layout)
	b[11] = h.Index
	b[12] = h.DataShards
	b[13] = h.ParityShards
	binary.BigEndian.PutUint32(b[16:20], h.BlockSize)
	copy(b[20:36], h.BlobID[:])
	binary.BigEndian.PutUint32(b[60:64], Checksum(b[0:60]))
	return b
}

// ParseHeader — kebalikan Marshal, termasuk verifikasi checksum & Validate.
func ParseHeader(b []byte) (Header, error) {
	if len(b) != HeaderSize || !bytes.Equal(b[0:8], headerMagic[:]) {
		return Header{}, ErrFormat
	}
	if Checksum(b[0:60]) != binary.BigEndian.Uint32(b[60:64]) {
		return Header{}, fmt.Errorf("%w: header", ErrCorrupt)
	}
	if v := binary.BigEndian.Uint16(b[8:10]); v != FormatVersion {
		return Header{}, fmt.Errorf("%w: versi %d", ErrFormat, v)
	}
	h := Header{Layout: Layout(b[10]), Index: b[11], DataShards: b[12], ParityShards: b[13],
		BlockSize: binary.BigEndian.Uint32(b[16:20])}
	copy(h.BlobID[:], b[20:36])
	return h, h.Validate()
}

// Tail — diketahui SETELAH byte terakhir blob.
type Tail struct {
	PayloadSize uint64   // byte data di shard ini (EC: kelipatan BlockSize, stripe terakhir diisi nol)
	BlobSize    uint64   // ukuran blob asli
	BlobSHA256  [32]byte // hash blob asli (bukan shard) — dipakai fsck & rekonstruksi
	Blocks      uint32
	TableCRC    uint32 // checksum tabel CRC blok
}

// Blocks — jumlah blok untuk payload sebesar size.
func Blocks(size uint64, blockSize uint32) uint32 {
	bs := uint64(blockSize)
	return uint32((size + bs - 1) / bs) //nolint:gosec // dibatasi ukuran shard maksimum
}

func (t Tail) marshal() []byte {
	b := make([]byte, TailSize)
	binary.BigEndian.PutUint64(b[0:8], t.PayloadSize)
	binary.BigEndian.PutUint64(b[8:16], t.BlobSize)
	copy(b[16:48], t.BlobSHA256[:])
	binary.BigEndian.PutUint32(b[48:52], t.Blocks)
	binary.BigEndian.PutUint32(b[52:56], t.TableCRC)
	binary.BigEndian.PutUint32(b[56:60], Checksum(b[0:56]))
	copy(b[60:64], tailMagic[:])
	return b
}

func parseTail(b []byte, blockSize uint32) (Tail, error) {
	if len(b) != TailSize || !bytes.Equal(b[60:64], tailMagic[:]) {
		return Tail{}, fmt.Errorf("%w: tail tidak ada (file terpotong?)", ErrCorrupt)
	}
	if Checksum(b[0:56]) != binary.BigEndian.Uint32(b[56:60]) {
		return Tail{}, fmt.Errorf("%w: tail", ErrCorrupt)
	}
	t := Tail{PayloadSize: binary.BigEndian.Uint64(b[0:8]), BlobSize: binary.BigEndian.Uint64(b[8:16]),
		Blocks: binary.BigEndian.Uint32(b[48:52]), TableCRC: binary.BigEndian.Uint32(b[52:56])}
	copy(t.BlobSHA256[:], b[16:48])
	if t.Blocks != Blocks(t.PayloadSize, blockSize) {
		return Tail{}, fmt.Errorf("%w: %d blok untuk %d byte", ErrCorrupt, t.Blocks, t.PayloadSize)
	}
	return t, nil
}

// FileSize — ukuran file shard di disk untuk payload sebesar size.
func FileSize(size uint64, blockSize uint32) int64 {
	return int64(HeaderSize) + int64(size) + 4*int64(Blocks(size, blockSize)) + TailSize //nolint:gosec // dibatasi
}
