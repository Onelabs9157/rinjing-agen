// Package config — konfigurasi agen dari environment (lihat .env.example).
package config

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Listen     string
	DataDir    string
	Node       string
	Token      string
	Allow      []netip.Prefix // kosong + AllowAny = semua alamat
	AllowAny   bool
	TLSCert    string
	TLSKey     string
	MinFree    uint64
	MaxPayload uint64
	TmpMaxAge  time.Duration
}

var nodeCode = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// MinTokenLen — token pendek mudah ditebak; `rinjing-agen token` membuat 64 karakter hex.
const MinTokenLen = 32

// Load — baca & validasi. getenv biasanya os.Getenv.
func Load(getenv func(string) string) (Config, error) {
	get := func(k, def string) string {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v
		}
		return def
	}
	c := Config{
		Listen:  get("AGENT_LISTEN", ":9100"),
		DataDir: get("AGENT_DATA_DIR", "/data"),
		Node:    get("AGENT_NODE_CODE", ""),
		Token:   get("AGENT_TOKEN", ""),
		TLSCert: get("AGENT_TLS_CERT", ""),
		TLSKey:  get("AGENT_TLS_KEY", ""),
	}
	var errs []error
	if !nodeCode.MatchString(c.Node) {
		errs = append(errs, errors.New("AGENT_NODE_CODE wajib diisi (huruf, angka, _ atau -), mis. NODE_SRV2"))
	}
	if len(c.Token) < MinTokenLen {
		errs = append(errs, fmt.Errorf("AGENT_TOKEN minimal %d karakter — buat dengan `rinjing-agen token`", MinTokenLen))
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		errs = append(errs, errors.New("AGENT_TLS_CERT dan AGENT_TLS_KEY harus diisi berdua"))
	}
	allow := get("AGENT_ALLOW_FROM", "")
	switch allow {
	case "":
		errs = append(errs, errors.New("AGENT_ALLOW_FROM wajib diisi: IP Rinjing utama (mis. 10.10.10.25) atau * untuk semua"))
	case "*":
		c.AllowAny = true
	default:
		for _, part := range strings.Split(allow, ",") {
			p, err := parsePrefix(strings.TrimSpace(part))
			if err != nil {
				errs = append(errs, fmt.Errorf("AGENT_ALLOW_FROM: %w", err))
				continue
			}
			c.Allow = append(c.Allow, p)
		}
	}
	var err error
	if c.MinFree, err = ParseSize(get("AGENT_MIN_FREE", "5GiB")); err != nil {
		errs = append(errs, fmt.Errorf("AGENT_MIN_FREE: %w", err))
	}
	if c.MaxPayload, err = ParseSize(get("AGENT_MAX_SHARD", "6GiB")); err != nil {
		errs = append(errs, fmt.Errorf("AGENT_MAX_SHARD: %w", err))
	}
	if c.TmpMaxAge, err = time.ParseDuration(get("AGENT_TMP_MAX_AGE", "24h")); err != nil || c.TmpMaxAge <= 0 {
		errs = append(errs, errors.New("AGENT_TMP_MAX_AGE: durasi tidak sah (mis. 24h)"))
	}
	return c, errors.Join(errs...)
}

func parsePrefix(s string) (netip.Prefix, error) {
	if strings.Contains(s, "/") {
		return netip.ParsePrefix(s)
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	return netip.PrefixFrom(a, a.BitLen()), nil
}

var units = map[string]uint64{
	"": 1, "B": 1,
	"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
	"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
}

// ParseSize — "5GiB", "500 MB", "1048576" → byte (sama dengan format ukuran di Rinjing).
func ParseSize(s string) (uint64, error) {
	t := strings.TrimSpace(s)
	i := strings.IndexFunc(t, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	num, unit := t, ""
	if i >= 0 {
		num, unit = strings.TrimSpace(t[:i]), strings.TrimSpace(t[i:])
	}
	mult, ok := units[strings.ToUpper(unit)]
	if !ok || num == "" {
		return 0, fmt.Errorf("ukuran %q tidak dikenali", s)
	}
	v, err := strconv.ParseFloat(num, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("ukuran %q tidak dikenali", s)
	}
	return uint64(v * float64(mult)), nil
}
