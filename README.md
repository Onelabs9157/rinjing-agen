# rinjing-agen

Penyimpan **potongan (shard)** file untuk **Rinjing cluster**. Rinjing utama (S3 API + console +
PostgreSQL) memecah setiap file dengan erasure coding Reed-Solomon dan menyebar potongannya ke
agen-agen di server fisik berbeda. File tetap bisa dibaca walaupun beberapa server mati.

Agen sengaja dibuat "bodoh": hanya menyimpan, mengirim, menghapus, dan memeriksa shard. Agen tidak
tahu nama bucket/objek, tidak terhubung ke database, dan tidak menyimpan access key maupun kunci
enkripsi sistem. Rancangan lengkap: `docs/plan-cluster.md` di repo Rinjing.

```
                    Rinjing utama (onelabs, 10.10.10.25)
                     │ encode Reed-Solomon, placement, heal
       ┌─────────────┼─────────────┬─────────────┐   API agen :9100 (jaringan privat)
   rinjing-agen   rinjing-agen  rinjing-agen  rinjing-agen      ← repo ini, 1 per server fisik
   /data/…        /data/…       /data/…       /data/…
```

> **Status:** v0.1 — agen siap dipasang dan diuji. Rinjing utama belum memakainya (fase C2–C4
> di plan). Memasang agen sekarang aman: tidak ada data produksi yang dikirim ke agen sebelum
> bucket diubah ke `placement=cluster`.

## Isi repo

| Paket | Isi | Dipakai |
|---|---|---|
| [`shard/`](shard/) | format file shard & format di jaringan (frame per blok + CRC32C) | agen **dan** Rinjing utama |
| [`protocol/`](protocol/) | API `/v1`, tanda tangan request HMAC, tipe JSON, klien Go | agen **dan** Rinjing utama |
| [`store/`](store/) | penyimpanan shard di disk (atomik, fsync, verifikasi) | agen, dan Rinjing utama yang ikut menyimpan shard |
| [`server/`](server/) | handler HTTP | agen, dan test integrasi Rinjing utama |
| `internal/config/` | konfigurasi dari env | agen |
| `cmd/rinjing-agen/` | binary | agen |

`shard/` dan `protocol/` adalah **kontrak** antara agen dan Rinjing utama. Rinjing utama
mengimpornya sebagai modul Go pada versi tag tertentu, jadi kodenya tidak disalin ke dua tempat.

## Pasang di server baru

**Syarat**
- VM/server di **host fisik yang belum punya agen**. Dua agen di satu host fisik = satu zona; hal itu
  tidak menambah ketahanan.
- Disk data = **disk lokal host itu sendiri**. Jangan storage bersama Proxmox lintas host (Ceph/NFS).
- Docker + plugin compose, jam tersinkron (`timedatectl` → `System clock synchronized: yes`).
- Jaringan privat yang sama dengan Rinjing utama.

**Langkah**

```bash
# 1. ambil kode (repo privat: pakai deploy key read-only / token GitHub)
git clone git@github.com:Onelabs9157/rinjing-agen.git ~/rinjing-agen
cd ~/rinjing-agen

# 2. folder data di disk lokal, dimiliki uid container (10001)
sudo mkdir -p /data/rinjing-agen && sudo chown 10001:10001 /data/rinjing-agen

# 3. konfigurasi
cp .env.example .env
openssl rand -hex 32          # → AGENT_TOKEN
nano .env                     # AGENT_NODE_CODE, AGENT_TOKEN, AGENT_ALLOW_FROM, AGENT_LISTEN

# 4. firewall: port agen hanya dari Rinjing utama (contoh ufw)
sudo ufw allow from 10.10.10.25 to any port 9100 proto tcp

# 5. jalankan
docker compose up -d --build
docker compose ps             # STATUS: healthy
docker compose logs --tail 20
```

**Cek dari Rinjing utama:**

```bash
curl http://<IP-agen>:9100/healthz        # → ok (hanya "hidup", tanpa autentikasi)

# uji penuh: tanda tangan, unggah 8 MiB acak, baca balik & bandingkan, verify, hapus
cat > /tmp/probe.env <<EOF
AGENT_PROBE_URL=http://<IP-agen>:9100
AGENT_NODE_CODE=<kode node agen>
AGENT_TOKEN=<token agen>
EOF
docker run --rm --network host --env-file /tmp/probe.env rinjing-agen:local probe; rm /tmp/probe.env
```

Harus diakhiri `PROBE LULUS`. Shard uji selalu dihapus, juga bila ada langkah yang gagal.

Simpan `AGENT_NODE_CODE`, IP:port, dan `AGENT_TOKEN`. Ketiganya didaftarkan di Rinjing utama
(console **Node**, fase C2). Token adalah rahasia: jangan dikirim lewat chat, simpan di password
manager.

**Penamaan node:** satu kode per server fisik dan permanen, mis. `NODE_SRV2`. Folder data dicap
dengan kode ini (`/data/rinjing-agen/.rinjing-agen/format.json`). Agen menolak start bila folder
itu milik node lain, untuk mencegah salah mount disk.

## Update

```bash
cd ~/rinjing-agen && git pull --ff-only
AGENT_VERSION=$(git describe --tags --always) docker compose up -d --build
```

**Urutan update:** perbarui **semua agen dulu, baru Rinjing utama**. Agen versi baru selalu bisa
membaca format lama. `/v1/health` melaporkan `api_version` & `format_version`, dan Rinjing utama
tidak menaruh shard di agen yang tidak kompatibel.

## Konfigurasi

Semua lewat `.env` (lihat [.env.example](.env.example)).

| Var | Default | Arti |
|---|---|---|
| `AGENT_NODE_CODE` | — wajib | kode node, huruf/angka/`_`/`-` |
| `AGENT_TOKEN` | — wajib, ≥ 32 karakter | rahasia bersama dengan Rinjing utama |
| `AGENT_ALLOW_FROM` | — wajib | IP/CIDR yang boleh memanggil `/v1`, koma; `*` = semua (hanya bila ada firewall) |
| `AGENT_LISTEN` | `:9100` | IP:port yang dibuka |
| `AGENT_TLS_CERT` / `AGENT_TLS_KEY` | kosong | aktifkan HTTPS |
| `AGENT_MIN_FREE` | `5GiB` | unggahan ditolak (507) di bawah sisa disk ini |
| `AGENT_MAX_SHARD` | `6GiB` | batas ukuran satu shard |
| `AGENT_TMP_MAX_AGE` | `24h` | umur sisa unggahan gagal sebelum disapu |
| `AGENT_DATA_PATH` | `/data/rinjing-agen` | folder data di host (untuk compose) |

## API `/v1`

Semua endpoint `/v1` wajib lolos **allowlist IP** dan **tanda tangan** `Authorization: RJ1 node=…,ts=…,sig=…`
(HMAC-SHA256 atas metode, path, query, node, waktu). Selisih jam > 5 menit ditolak.

| Endpoint | Fungsi |
|---|---|
| `GET /healthz` | **tanpa autentikasi**, hanya `ok` (healthcheck Docker) |
| `GET /v1/health` | status, versi API & format, kapasitas disk |
| `PUT /v1/shards/{blob}/{idx}` | simpan shard (stream; tiap blok diverifikasi saat diterima; atomik) |
| `GET /v1/shards/{blob}/{idx}?first=&count=` | kirim blok tertentu, tiap blok dengan checksum |
| `HEAD /v1/shards/{blob}/{idx}` | meta shard (header `X-Rinjing-*`) |
| `DELETE /v1/shards/{blob}/{idx}` | hapus (idempoten) |
| `GET /v1/shards?after=&limit=` | daftar shard terurut, berhalaman |
| `POST /v1/verify` | periksa checksum shard di disk agen (scrub), maks 1000 per panggilan |

Format byte shard (di disk dan di jaringan) dijelaskan di komentar [`shard/format.go`](shard/format.go)
dan [`shard/stream.go`](shard/stream.go).

## Keamanan

- **Jangan buka port agen ke internet.** Allowlist IP + firewall + jaringan privat.
- Body request **tidak** ditandatangani. Integritasnya dijaga checksum per blok, dan
  kerahasiaannya oleh TLS (aktifkan `AGENT_TLS_*`). Request yang direkam lalu diputar ulang dalam
  5 menit hanya bisa mengulang operasi atas blob immutable. Contohnya, menyimpan ulang shard yang
  sama, yang akan disapu sebagai yatim.
- Isi shard: potongan Reed-Solomon **bukan enkripsi**. Shard data berisi byte asli. Aplikasi yang
  menyimpan data rahasia mengenkripsi sebelum unggah (DMS sudah melakukannya).

## Operasi

- **Disk penuh:** unggahan baru ditolak 507. Rinjing utama menaruh shard di agen lain.
- **Agen mati:** Rinjing utama menunggu ±15 menit (bisa saja hanya restart), lalu membangun ulang
  shard-nya di server lain.
- **Ganti disk/server rusak:** pasang agen dengan kode node **baru**, daftarkan, lalu keluarkan node
  lama di console. Healing mengisi ulang shard secara otomatis. Folder shard tidak perlu disalin.
- **Jangan menyalin atau mengedit** isi `/data/rinjing-agen/shards` secara manual.
- Log: JSON ke stdout (`docker compose logs`).

## Pengembangan

```bash
go vet ./...
go test -p 2 ./...                       # semua test tanpa jaringan luar
go test -p 2 -cover ./...                # coverage per paket; jaga ≥ 85 % total
```

Perubahan pada `shard/` atau `protocol/` = perubahan kontrak. Baca [CLAUDE.md](CLAUDE.md) §Kontrak
sebelum mengubahnya.
