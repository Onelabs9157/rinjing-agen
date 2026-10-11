package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FileWriter — menulis file shard: header, blok, lalu tabel CRC + tail saat Finish.
type FileWriter struct {
	w       io.Writer
	h       Header
	crcs    []uint32
	payload uint64
	short   bool
	done    bool
}

// NewFileWriter memvalidasi h dan menulis header.
func NewFileWriter(w io.Writer, h Header) (*FileWriter, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	if _, err := w.Write(h.Marshal()); err != nil {
		return nil, err
	}
	return &FileWriter{w: w, h: h}, nil
}

// WriteBlock — satu blok berikut checksum-nya (pemanggil sudah memverifikasi data ↔ crc).
// Semua blok wajib berukuran BlockSize kecuali yang terakhir.
func (fw *FileWriter) WriteBlock(data []byte, crc uint32) error {
	if fw.done {
		return errors.New("shard: WriteBlock setelah Finish")
	}
	if len(data) == 0 || len(data) > int(fw.h.BlockSize) || fw.short {
		return fmt.Errorf("%w: blok %d byte tidak sah di posisi ini", ErrFormat, len(data))
	}
	if _, err := fw.w.Write(data); err != nil {
		return err
	}
	fw.crcs = append(fw.crcs, crc)
	fw.payload += uint64(len(data))
	fw.short = len(data) < int(fw.h.BlockSize)
	return nil
}

// Finish — tulis tabel CRC & tail. Tidak melakukan fsync (urusan pemilik file).
func (fw *FileWriter) Finish(blobSize uint64, blobSHA256 [32]byte) (Tail, error) {
	if fw.done {
		return Tail{}, errors.New("shard: Finish dua kali")
	}
	fw.done = true
	table := make([]byte, 4*len(fw.crcs))
	for i, c := range fw.crcs {
		binary.BigEndian.PutUint32(table[4*i:], c)
	}
	t := Tail{PayloadSize: fw.payload, BlobSize: blobSize, BlobSHA256: blobSHA256,
		Blocks: uint32(len(fw.crcs)), TableCRC: Checksum(table)} //nolint:gosec // dibatasi ukuran shard
	if _, err := fw.w.Write(table); err != nil {
		return Tail{}, err
	}
	if _, err := fw.w.Write(t.marshal()); err != nil {
		return Tail{}, err
	}
	return t, nil
}

// File — file shard yang sudah dibuka: header, tail, dan tabel CRC sudah terverifikasi.
type File struct {
	r     io.ReaderAt
	h     Header
	t     Tail
	table []uint32
}

// OpenFile membaca & memverifikasi kerangka file (bukan isi blok — itu Verify/ReadBlock).
func OpenFile(r io.ReaderAt, size int64) (*File, error) {
	if size < HeaderSize+TailSize {
		return nil, fmt.Errorf("%w: file %d byte", ErrCorrupt, size)
	}
	hb := make([]byte, HeaderSize)
	if _, err := r.ReadAt(hb, 0); err != nil {
		return nil, err
	}
	h, err := ParseHeader(hb)
	if err != nil {
		return nil, err
	}
	tb := make([]byte, TailSize)
	if _, err := r.ReadAt(tb, size-TailSize); err != nil {
		return nil, err
	}
	t, err := parseTail(tb, h.BlockSize)
	if err != nil {
		return nil, err
	}
	if want := FileSize(t.PayloadSize, h.BlockSize); want != size {
		return nil, fmt.Errorf("%w: ukuran file %d, seharusnya %d", ErrCorrupt, size, want)
	}
	raw := make([]byte, 4*int(t.Blocks))
	if _, err := r.ReadAt(raw, size-TailSize-int64(len(raw))); err != nil {
		return nil, err
	}
	if Checksum(raw) != t.TableCRC {
		return nil, fmt.Errorf("%w: tabel checksum", ErrCorrupt)
	}
	table := make([]uint32, t.Blocks)
	for i := range table {
		table[i] = binary.BigEndian.Uint32(raw[4*i:])
	}
	return &File{r: r, h: h, t: t, table: table}, nil
}

func (f *File) Header() Header { return f.h }
func (f *File) Tail() Tail     { return f.t }

// BlockLen — ukuran blok ke-i (blok terakhir bisa lebih pendek).
func (f *File) BlockLen(i uint32) int {
	start := uint64(i) * uint64(f.h.BlockSize)
	return int(min(uint64(f.h.BlockSize), f.t.PayloadSize-start)) //nolint:gosec // ≤ BlockSize
}

// ReadBlock — blok ke-i ke buf (cap ≥ BlockSize dianjurkan), diverifikasi terhadap tabel.
func (f *File) ReadBlock(i uint32, buf []byte) ([]byte, uint32, error) {
	if i >= f.t.Blocks {
		return nil, 0, fmt.Errorf("shard: blok %d di luar %d", i, f.t.Blocks)
	}
	n := f.BlockLen(i)
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	off := int64(HeaderSize) + int64(i)*int64(f.h.BlockSize)
	if _, err := f.r.ReadAt(buf, off); err != nil {
		return nil, 0, err
	}
	if Checksum(buf) != f.table[i] {
		return nil, 0, fmt.Errorf("%w: blok %d", ErrCorrupt, i)
	}
	return buf, f.table[i], nil
}

// Verify — baca & cek semua blok (scrub).
func (f *File) Verify() error {
	buf := make([]byte, f.h.BlockSize)
	for i := range f.t.Blocks {
		if _, _, err := f.ReadBlock(i, buf); err != nil {
			return err
		}
	}
	return nil
}
