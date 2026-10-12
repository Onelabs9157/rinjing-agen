# CLAUDE.md — rinjing-agen

Panduan untuk agen AI & developer. Baca README.md untuk pemasangan.

## Peran

Program kecil penyimpan **shard** untuk Rinjing cluster (rancangan: `docs/plan-cluster.md` di repo
`rinjing`, saudara folder ini). Rinjing utama melakukan encode/decode Reed-Solomon, placement,
healing, dan menyimpan semua metadata di PostgreSQL. Agen hanya menyimpan, melayani, menghapus,
dan memverifikasi shard. **Jangan menambah logika bisnis ke agen**: tidak ada akses DB, nama
bucket/objek, access key, atau kunci enkripsi sistem.

## Struktur

```
shard/       KONTRAK — format file & jaringan (header 64 B, blok, CRC32C, tail; frame per blok)
protocol/    KONTRAK — API /v1, tanda tangan RJ1 (HMAC-SHA256), tipe JSON, header meta, klien Go
store/       penyimpanan disk: tmp → fsync → rename → fsync dir; penanda folder per node
server/           handler HTTP (net/http, pola Go 1.22), allowlist IP + verifikasi tanda tangan — publik untuk test integrasi Rinjing
internal/config/  env → Config
cmd/rinjing-agen/ main: serve | token | healthcheck | version
```

`shard`, `protocol`, `store`, dan `server` publik karena diimpor Rinjing utama
(`github.com/Onelabs9157/rinjing-agen/...`).

## Kontrak (wajib dibaca sebelum mengubah `shard/` atau `protocol/`)

1. **Tata letak byte berubah** → naikkan `shard.FormatVersion`. Agen baru **tetap harus membaca
   versi lama**: tambahkan cabang parse, jangan ganti.
2. **Endpoint/semantik `/v1` berubah tidak kompatibel** → naikkan `protocol.APIVersion` (atau
   buat `/v2`). Menambah field JSON opsional tidak perlu menaikkan versi.
3. Urutan rilis: tag repo ini → update semua agen → baru Rinjing utama menaikkan versi modulnya.
4. Setiap perubahan kontrak disertai test bolak-balik + test kerusakan (lihat `shard/shard_test.go`).

## Invarian yang dijaga test

- Shard hanya muncul di `shards/` bila **semua** blok + trailer lolos checksum dan sudah di-fsync.
- Unggahan yang gagal/dibatalkan/terlalu besar tidak meninggalkan file (tmp dihapus).
- GET: blok rusak di awal → 422; rusak di tengah → koneksi diputus **tanpa** frame penutup, sehingga
  klien tidak pernah menerima byte rusak sebagai data sah.
- Folder data milik node lain / berisi shard tanpa penanda → agen menolak start.
- Semua blok berukuran `BlockSize` kecuali yang terakhir (offset → nomor blok = pembagian).

## Konvensi

- Go 1.25+, hanya stdlib + `github.com/google/uuid`. Reed-Solomon **tidak** ada di sini.
- **Jangan menyalin kode MinIO** (AGPL).
- Pesan galat & komentar berbahasa Indonesia, mengikuti repo `rinjing`.
- Test tanpa jaringan luar/DB: `go test -p 2 ./...` (batas 2 worker). Coverage total ≥ 85 %.
- Branch kerja `development`; commit gaya `feat(scope): …` / `fix(scope): …`.
