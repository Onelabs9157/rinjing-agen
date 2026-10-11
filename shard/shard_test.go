package shard

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
)

const bs = MinBlockSize // 4 KiB supaya test cepat

func hdr() Header {
	return Header{BlobID: uuid.MustParse("0192f3a4-5b6c-7d8e-9f00-112233445566"), Layout: LayoutEC,
		Index: 1, DataShards: 3, ParityShards: 2, BlockSize: bs}
}

func payload(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 7)) //nolint:gosec // data uji
	for i := range b {
		b[i] = byte(r.UintN(256))
	}
	return b
}

// put — body PUT untuk data, ditulis dengan potongan Write berukuran chunk.
func put(t *testing.T, h Header, data []byte, chunk int) []byte {
	t.Helper()
	var body bytes.Buffer
	sw, err := NewStreamWriter(&body, h)
	if err != nil {
		t.Fatal(err)
	}
	for p := data; len(p) > 0; {
		n := min(chunk, len(p))
		if _, err := sw.Write(p[:n]); err != nil {
			t.Fatal(err)
		}
		p = p[n:]
	}
	if err := sw.Close(uint64(len(data))*3, sha256.Sum256(data)); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}

func TestBolakBalikUtuh(t *testing.T) {
	for _, size := range []int{0, 1, bs - 1, bs, bs + 1, 3*bs + 17} {
		for _, chunk := range []int{1, 1000, bs, 10 * bs} {
			data := payload(size)
			var file bytes.Buffer
			h, tail, err := Receive(&file, bytes.NewReader(put(t, hdr(), data, chunk)), 0, nil)
			if err != nil {
				t.Fatalf("size %d chunk %d: %v", size, chunk, err)
			}
			if h != hdr() || tail.PayloadSize != uint64(size) || tail.BlobSize != uint64(size)*3 ||
				tail.BlobSHA256 != sha256.Sum256(data) {
				t.Fatalf("size %d: header/tail %+v %+v", size, h, tail)
			}
			if int64(file.Len()) != FileSize(uint64(size), bs) {
				t.Fatalf("ukuran file %d ≠ %d", file.Len(), FileSize(uint64(size), bs))
			}
			f, err := OpenFile(bytes.NewReader(file.Bytes()), int64(file.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Verify(); err != nil {
				t.Fatal(err)
			}
			var got []byte
			for i := range f.Tail().Blocks {
				b, _, err := f.ReadBlock(i, nil)
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, b...)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("size %d chunk %d: isi berbeda", size, chunk)
			}
		}
	}
}

func receiveErr(body []byte, max uint64) error {
	_, _, err := Receive(io.Discard, bytes.NewReader(body), max, nil)
	return err
}

func TestStreamRusakDitolak(t *testing.T) {
	data := payload(2*bs + 100)
	good := put(t, hdr(), data, bs)
	flip := func(off int) []byte {
		b := bytes.Clone(good)
		b[off] ^= 0x40
		return b
	}
	firstData := HeaderSize + frameHeaderSize + 10
	cases := map[string]struct {
		body []byte
		want error
	}{
		"byte blok terbalik":    {flip(firstData), ErrCorrupt},
		"header terbalik":       {flip(30), ErrCorrupt},
		"magic salah":           {flip(0), ErrFormat},
		"trailer terbalik":      {flip(len(good) - 10), ErrCorrupt},
		"terpotong di blok":     {good[:firstData+5], ErrCorrupt},
		"tanpa trailer":         {good[:len(good)-trailerSize], ErrCorrupt},
		"terpotong di header":   {good[:10], ErrCorrupt},
		"melebihi batas ukuran": {good, ErrTooLarge},
	}
	for name, tc := range cases {
		max := uint64(0)
		if tc.want == ErrTooLarge {
			max = bs
		}
		if err := receiveErr(tc.body, max); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, mau %v", name, err, tc.want)
		}
	}
	if err := receiveErr(good, 0); err != nil {
		t.Fatal(err)
	}
}

// frame buatan tangan untuk aturan ukuran blok
func TestAturanUkuranFrame(t *testing.T) {
	build := func(sizes ...int) []byte {
		var b bytes.Buffer
		b.Write(hdr().Marshal())
		for _, n := range sizes {
			d := payload(n)
			_ = writeFrame(&b, d, Checksum(d))
		}
		_ = writeFrame(&b, nil, 0)
		tr := make([]byte, trailerSize)
		binary.BigEndian.PutUint32(tr[40:], Checksum(tr[0:40]))
		b.Write(tr)
		return b.Bytes()
	}
	if err := receiveErr(build(bs, bs, 5), 0); err != nil {
		t.Fatalf("pola sah: %v", err)
	}
	if err := receiveErr(build(5, bs), 0); !errors.Is(err, ErrFormat) {
		t.Errorf("blok pendek di tengah: %v", err)
	}
	if err := receiveErr(build(bs+1), 0); !errors.Is(err, ErrFormat) {
		t.Errorf("frame melebihi block size: %v", err)
	}
	bad := build(bs)
	end := HeaderSize + frameHeaderSize + bs
	binary.BigEndian.PutUint32(bad[end+4:], 9) // frame end dengan crc ≠ 0
	if err := receiveErr(bad, 0); !errors.Is(err, ErrFormat) {
		t.Errorf("frame end tidak sah: %v", err)
	}
}

func TestCheckDanHeaderTidakSah(t *testing.T) {
	stop := errors.New("tolak")
	if _, _, err := Receive(io.Discard, bytes.NewReader(put(t, hdr(), payload(10), 10)), 0,
		func(Header) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("check: %v", err)
	}
	bad := []Header{
		{Layout: LayoutEC, DataShards: 3, ParityShards: 2, BlockSize: bs},                      // blob id kosong
		{BlobID: uuid.New(), Layout: LayoutEC, DataShards: 3, ParityShards: 2, BlockSize: 100}, // blok kecil
		{BlobID: uuid.New(), Layout: LayoutEC, DataShards: 3, ParityShards: 0, BlockSize: bs},  // tanpa parity
		{BlobID: uuid.New(), Layout: LayoutEC, Index: 5, DataShards: 3, ParityShards: 2, BlockSize: bs},
		{BlobID: uuid.New(), Layout: LayoutReplica, DataShards: 2, BlockSize: bs},
		{BlobID: uuid.New(), Layout: 9, DataShards: 1, BlockSize: bs},
	}
	for _, h := range bad {
		if _, err := NewStreamWriter(io.Discard, h); !errors.Is(err, ErrFormat) {
			t.Errorf("%+v: %v", h, err)
		}
	}
	ok := Header{BlobID: uuid.New(), Layout: LayoutReplica, Index: 2, DataShards: 1, ParityShards: 2, BlockSize: bs}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	if LayoutReplica.String() != "replica" || LayoutEC.String() != "ec" || Layout(9).String() != "layout(9)" {
		t.Error("String layout")
	}
	// versi format tidak dikenal
	b := hdr().Marshal()
	binary.BigEndian.PutUint16(b[8:10], 99)
	binary.BigEndian.PutUint32(b[60:64], Checksum(b[0:60]))
	if _, err := ParseHeader(b); !errors.Is(err, ErrFormat) {
		t.Errorf("versi: %v", err)
	}
}

func fileOf(t *testing.T, data []byte) []byte {
	t.Helper()
	var f bytes.Buffer
	if _, _, err := Receive(&f, bytes.NewReader(put(t, hdr(), data, bs)), 0, nil); err != nil {
		t.Fatal(err)
	}
	return f.Bytes()
}

func TestFileRusakTerdeteksi(t *testing.T) {
	good := fileOf(t, payload(2*bs+9))
	open := func(b []byte) (*File, error) { return OpenFile(bytes.NewReader(b), int64(len(b))) }

	blk := bytes.Clone(good)
	blk[HeaderSize+bs+3] ^= 1 // bitrot di blok 1
	f, err := open(blk)
	if err != nil {
		t.Fatalf("kerangka masih sehat: %v", err)
	}
	if _, _, err := f.ReadBlock(0, nil); err != nil {
		t.Fatalf("blok 0 sehat: %v", err)
	}
	if _, _, err := f.ReadBlock(1, nil); !errors.Is(err, ErrCorrupt) {
		t.Errorf("blok 1: %v", err)
	}
	if err := f.Verify(); !errors.Is(err, ErrCorrupt) {
		t.Errorf("verify: %v", err)
	}
	if _, _, err := f.ReadBlock(9, nil); err == nil {
		t.Error("blok di luar jangkauan")
	}

	table := bytes.Clone(good)
	table[len(table)-TailSize-2] ^= 1
	tail := bytes.Clone(good)
	tail[len(tail)-20] ^= 1
	for name, b := range map[string][]byte{
		"tabel":     table,
		"tail":      tail,
		"terpotong": good[:len(good)-1],
		"kekecilan": good[:20],
		"ditambahi": append(bytes.Clone(good[:len(good)-TailSize]), append(make([]byte, 4), good[len(good)-TailSize:]...)...),
	} {
		if _, err := open(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestBlockReader(t *testing.T) {
	data := payload(bs + 50)
	f, err := OpenFile(bytes.NewReader(fileOf(t, data)), FileSize(uint64(len(data)), bs))
	if err != nil {
		t.Fatal(err)
	}
	var resp bytes.Buffer
	for i := range f.Tail().Blocks {
		b, crc, err := f.ReadBlock(i, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := WriteBlock(&resp, b, crc); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteEnd(&resp); err != nil {
		t.Fatal(err)
	}
	read := func(b []byte) ([]byte, error) {
		br := NewBlockReader(bytes.NewReader(b), bs)
		var got []byte
		for {
			blk, err := br.Next()
			if errors.Is(err, io.EOF) {
				return got, nil
			}
			if err != nil {
				return got, err
			}
			got = append(got, blk...)
		}
	}
	got, err := read(resp.Bytes())
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("respons GET: %v", err)
	}
	if _, err := read(resp.Bytes()[:resp.Len()-frameHeaderSize]); !errors.Is(err, ErrCorrupt) {
		t.Errorf("tanpa frame end = terpotong: %v", err)
	}
	if err := WriteBlock(io.Discard, nil, 0); err == nil {
		t.Error("blok kosong ditolak")
	}
}

func TestPemakaianSalah(t *testing.T) {
	sw, _ := NewStreamWriter(io.Discard, hdr())
	_ = sw.Close(0, [32]byte{})
	if err := sw.Close(0, [32]byte{}); err != nil {
		t.Error("Close kedua no-op")
	}
	if _, err := sw.Write([]byte("x")); err == nil {
		t.Error("Write setelah Close")
	}
	fw, _ := NewFileWriter(io.Discard, hdr())
	if err := fw.WriteBlock(nil, 0); !errors.Is(err, ErrFormat) {
		t.Error("blok kosong")
	}
	_, _ = fw.Finish(0, [32]byte{})
	if _, err := fw.Finish(0, [32]byte{}); err == nil {
		t.Error("Finish dua kali")
	}
	if err := fw.WriteBlock([]byte("x"), 0); err == nil {
		t.Error("WriteBlock setelah Finish")
	}
	if _, err := NewFileWriter(io.Discard, Header{}); err == nil {
		t.Error("header kosong")
	}
	if _, err := NewStreamWriter(errWriter{}, hdr()); err == nil {
		t.Error("galat tulis header")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("disk penuh") }
