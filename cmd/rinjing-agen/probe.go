package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/Onelabs9157/rinjing-agen/internal/config"
	"github.com/Onelabs9157/rinjing-agen/protocol"
	"github.com/Onelabs9157/rinjing-agen/shard"
)

// probe — uji ujung-ke-ujung sebuah agen dari mesin lain (biasanya Rinjing utama):
// health → unggah shard acak → HEAD → baca balik & bandingkan → verify → hapus.
// Shard uji selalu dihapus, juga bila langkah tengah gagal.
//
//	AGENT_PROBE_URL=http://10.10.10.60:9100 AGENT_NODE_CODE=… AGENT_TOKEN=… rinjing-agen probe
func probe(ctx context.Context, getenv func(string) string, out io.Writer) error {
	size, err := config.ParseSize(firstNonEmpty(getenv("AGENT_PROBE_SIZE"), "8MiB"))
	if err != nil {
		return fmt.Errorf("AGENT_PROBE_SIZE: %w", err)
	}
	c := &protocol.Client{BaseURL: getenv("AGENT_PROBE_URL"), Node: getenv("AGENT_NODE_CODE"), Token: getenv("AGENT_TOKEN"),
		HTTP: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: getenv("AGENT_PROBE_INSECURE") == "1", //nolint:gosec // hanya bila operator meminta (sertifikat uji)
		}}}}
	if c.BaseURL == "" || c.Node == "" || c.Token == "" {
		return errors.New("probe butuh AGENT_PROBE_URL, AGENT_NODE_CODE, AGENT_TOKEN")
	}
	step := func(name string, start time.Time, extra string) {
		fmt.Fprintf(out, "  %-8s ok  %6.0f ms  %s\n", name, float64(time.Since(start).Microseconds())/1000, extra)
	}

	t := time.Now()
	h, err := c.Health(ctx)
	if err != nil {
		return fmt.Errorf("health: %w", err)
	}
	if err := h.Compatible(); err != nil {
		return err
	}
	step("health", t, fmt.Sprintf("node %s, agen %s, API v%d, format v%d, disk bebas %s", h.Node, h.Version,
		h.APIVersion, h.FormatVersion, human(h.FreeBytes, h.DiskKnown)))

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		return err
	}
	sum := sha256.Sum256(payload)
	hdr := shard.Header{BlobID: uuid.New(), Layout: shard.LayoutReplica, DataShards: 1, BlockSize: 1 << 20}
	ref := protocol.ShardRef{Blob: hdr.BlobID}
	defer func() { // bersihkan apa pun hasilnya
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = c.Delete(dctx, ref)
	}()

	t = time.Now()
	u, err := c.Upload(ctx, hdr)
	if err != nil {
		return err
	}
	if _, err := u.Write(payload); err != nil {
		_ = u.Abort(err)
		return fmt.Errorf("unggah: %w", err)
	}
	if err := u.Close(uint64(len(payload)), sum); err != nil {
		return fmt.Errorf("unggah: %w", err)
	}
	step("put", t, rate(len(payload), time.Since(t)))

	t = time.Now()
	m, err := c.Head(ctx, ref)
	if err != nil {
		return fmt.Errorf("head: %w", err)
	}
	if m.PayloadSize != uint64(len(payload)) || m.BlobSHA256 != sum {
		return fmt.Errorf("head: meta tidak cocok (%d byte)", m.PayloadSize)
	}
	step("head", t, fmt.Sprintf("%d blok", m.Blocks))

	t = time.Now()
	b, err := c.Get(ctx, ref, 0, 0)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	got := make([]byte, 0, len(payload))
	for {
		blk, err := b.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = b.Close()
			return fmt.Errorf("get: %w", err)
		}
		got = append(got, blk...)
	}
	_ = b.Close()
	if !bytes.Equal(got, payload) {
		return errors.New("get: isi berbeda dari yang diunggah")
	}
	step("get", t, rate(len(got), time.Since(t))+", isi identik")

	t = time.Now()
	v, err := c.Verify(ctx, []protocol.ShardRef{ref})
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	if len(v.Missing)+len(v.Corrupt) > 0 {
		return fmt.Errorf("verify: %+v", v)
	}
	step("verify", t, "checksum di disk agen sehat")

	t = time.Now()
	if err := c.Delete(ctx, ref); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if _, err := c.Head(ctx, ref); !errors.Is(err, protocol.ErrNotFound) {
		return fmt.Errorf("delete: shard masih ada (%v)", err)
	}
	step("delete", t, "")
	fmt.Fprintln(out, "PROBE LULUS")
	return nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func rate(n int, d time.Duration) string {
	if d <= 0 {
		d = time.Microsecond
	}
	return fmt.Sprintf("%s, %.1f MB/s", human(uint64(n), true), float64(n)/1e6/d.Seconds()) //nolint:gosec // n ≥ 0
}

func human(n uint64, known bool) string {
	if !known {
		return "tidak diketahui"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}
