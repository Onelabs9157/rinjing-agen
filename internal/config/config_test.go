package config

import (
	"strings"
	"testing"
	"time"
)

func env(kv map[string]string) func(string) string { return func(k string) string { return kv[k] } }

func TestLoadSah(t *testing.T) {
	c, err := Load(env(map[string]string{
		"AGENT_NODE_CODE":  "NODE_SRV2",
		"AGENT_TOKEN":      strings.Repeat("a", 64),
		"AGENT_ALLOW_FROM": "10.10.10.25, 10.10.20.0/24, ::1",
		"AGENT_MIN_FREE":   "1 GiB",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":9100" || c.DataDir != "/data" || len(c.Allow) != 3 || c.Allow[0].Bits() != 32 ||
		c.MinFree != 1<<30 || c.MaxPayload != 6<<30 || c.TmpMaxAge != 24*time.Hour || c.AllowAny {
		t.Fatalf("%+v", c)
	}
	c, err = Load(env(map[string]string{"AGENT_NODE_CODE": "n", "AGENT_TOKEN": strings.Repeat("a", 32), "AGENT_ALLOW_FROM": "*"}))
	if err != nil || !c.AllowAny || len(c.Allow) != 0 {
		t.Fatalf("allow *: %+v %v", c, err)
	}
}

func TestLoadGalat(t *testing.T) {
	_, err := Load(env(map[string]string{
		"AGENT_NODE_CODE":   "node dengan spasi",
		"AGENT_TOKEN":       "pendek",
		"AGENT_TLS_CERT":    "/c.pem",
		"AGENT_ALLOW_FROM":  "bukan-ip",
		"AGENT_MIN_FREE":    "banyak",
		"AGENT_MAX_SHARD":   "-1",
		"AGENT_TMP_MAX_AGE": "sehari",
	}))
	if err == nil {
		t.Fatal("harus galat")
	}
	for _, want := range []string{"AGENT_NODE_CODE", "AGENT_TOKEN", "AGENT_TLS_KEY", "AGENT_ALLOW_FROM", "AGENT_MIN_FREE", "AGENT_MAX_SHARD", "AGENT_TMP_MAX_AGE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("galat tidak menyebut %s: %v", want, err)
		}
	}
	if _, err := Load(env(map[string]string{"AGENT_NODE_CODE": "n", "AGENT_TOKEN": strings.Repeat("a", 32)})); err == nil ||
		!strings.Contains(err.Error(), "AGENT_ALLOW_FROM wajib") {
		t.Fatalf("allow kosong: %v", err)
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]uint64{"1048576": 1 << 20, "5GiB": 5 << 30, "500 MB": 500e6, "1.5KiB": 1536, "2tib": 2 << 40, "7B": 7} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%q → %d %v, mau %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "GiB", "5 XB", "1.2.3"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("%q diterima", bad)
		}
	}
}
