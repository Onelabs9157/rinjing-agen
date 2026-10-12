# Panduan pasang node agen baru

Checklist untuk menambah satu **rinjing-agen** ke Rinjing cluster. Ikuti dari atas ke bawah.
Pengalaman dari node pertama (`NODE_DEMO`, `NODE_DIGIX`) sudah dimasukkan di sini.

Node yang sudah terpasang (perbarui tabel ini setiap menambah node):

| Kode node | Zona (server fisik) | Alamat | Folder data | Catatan |
|---|---|---|---|---|
| `NODE_ONELABS` | `ONELABS` (sementara) | lokal (di dalam Rinjing utama) | `DATA_DIR/cluster-shards` | otomatis, tidak dipasang manual |
| `NODE_DEMO` | `VM03` | `http://10.10.10.60:9100` | `/home/demo/rinjing-agen-data` | VM juga menyimpan backup harian → `AGENT_MIN_FREE=20GiB` |
| `NODE_DIGIX` | `VM05` | `http://10.10.10.12:9101` | `/data/rinjing-agen` | port 9100 dipakai program lain → 9101 |

---

## 0. Tentukan dulu (jangan diubah setelah agen jalan)

| Hal | Aturan |
|---|---|
| **Server fisik** | Satu agen per **server fisik (node Proxmox)**. VM lain di host yang sama tidak menambah ketahanan. |
| **Kode node** | Huruf/angka/`_`/`-`, mis. `NODE_SRV6`. **Permanen**: folder data dicap dengan kode ini saat pertama jalan. |
| **Zona** | **Nama node Proxmox** tempat VM berada, bukan nama VM. Dua node satu host fisik = zona sama. |
| **Disk** | Disk VM harus di **storage lokal host itu sendiri**, bukan storage bersama lintas host (Ceph/NFS). |
| **Port** | Bawaan 9100. Kalau sudah terpakai (sering oleh `node_exporter`), pakai 9101 dst. |

Cara melihat zona: web UI Proxmox → panel kiri **Datacenter** → nama-nama di bawahnya adalah server fisik;
VM berada di bawah salah satunya.

## 1. Cek VM

```bash
timedatectl | grep synchronized          # wajib: yes (jam meleset > 5 menit = request ditolak)
ss -ltn | grep -E ':910[0-9]'            # pilih port yang TIDAK muncul di sini
ip -4 addr | grep inet                   # catat IP jaringan 10.10.10.x milik VM ini
df -h /                                  # sisa disk
docker version && docker compose version
```

Belum ada Docker (Ubuntu):

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER            # lalu logout & login lagi
```

## 2. Ambil kode

```bash
git clone https://github.com/Onelabs9157/rinjing-agen.git ~/rinjing-agen
cd ~/rinjing-agen
```

## 3. Folder data (dimiliki uid 10001)

Dengan sudo:

```bash
sudo mkdir -p /data/rinjing-agen && sudo chown 10001:10001 /data/rinjing-agen
```

Tanpa sudo (folder di home, chown lewat Docker):

```bash
mkdir -p ~/rinjing-agen-data
docker run --rm --user 0 -v ~/rinjing-agen-data:/d alpine chown 10001:10001 /d
```

## 4. Konfigurasi `.env`

```bash
cp .env.example .env && chmod 600 .env
openssl rand -hex 32                     # hasilnya untuk AGENT_TOKEN
nano .env
```

Yang wajib diubah:

```ini
AGENT_NODE_CODE=NODE_SRV6                # kode permanen dari langkah 0
AGENT_TOKEN=<hasil openssl>              # rahasia — simpan juga di password manager
AGENT_ALLOW_FROM=10.10.10.25             # hanya Rinjing utama (onelabs)
AGENT_LISTEN=<IP VM ini>:9100            # IP VM, bukan 0.0.0.0; port sesuai langkah 1
AGENT_DATA_PATH=/data/rinjing-agen       # atau /home/<user>/rinjing-agen-data
AGENT_VERSION=v0.2.0                     # supaya versi terbaca di console (bukan "dev")
```

VM yang juga dipakai untuk hal lain (mis. menyimpan backup): naikkan `AGENT_MIN_FREE` (mis. `20GiB`).

## 5. Jalankan

```bash
docker compose up -d --build
docker compose ps                        # tunggu ±30 dtk → (healthy)
docker compose logs --tail 3             # "rinjing-agen jalan" … node & listen benar
```

## 6. Firewall — port agen hanya untuk onelabs

```bash
sudo ufw allow OpenSSH                   # DULU, supaya tidak terkunci
sudo ufw allow from 10.10.10.25 to any port <port agen> proto tcp
sudo ufw show added                      # periksa sebelum mengaktifkan
sudo ufw enable
sudo ufw status verbose
```

Port aplikasi yang dipublish Docker tidak terpengaruh ufw; layanan non-Docker lain yang perlu diakses
dari luar → tambahkan aturannya sebelum `ufw enable`.

## 7. Cek dari onelabs

```bash
ssh onelabs 'curl -s http://<IP>:<port>/healthz'     # → ok
```

Dari komputer lain perintah yang sama harus **timeout** (firewall bekerja).

## 8. Daftarkan di console Rinjing

**Storage → Node Cluster → Tambah Node**

| Isian | Nilai |
|---|---|
| Kode node | **persis** `AGENT_NODE_CODE` |
| Alamat agen | `http://<IP>:<port>` |
| Zona | nama node Proxmox (langkah 0) |
| Token | **persis** `AGENT_TOKEN` — salin dengan `sed -n 's/^AGENT_TOKEN=//p' .env` |

Dalam beberapa detik status harus **Online**. Lalu perbarui tabel di atas.

---

## Kalau tidak Online

| Gejala (kolom Kesehatan) | Penyebab & perbaikan |
|---|---|
| **Ditolak agen (401/403)** | Kode node atau token tidak sama persis; `AGENT_ALLOW_FROM` bukan `10.10.10.25`; jam tidak sinkron. Cek nilai di container yang **sedang jalan**: `docker exec rinjing-agen printenv AGENT_NODE_CODE` |
| **Tidak terjangkau: connection refused** | Container mati / restart terus → `docker compose logs --tail 20` |
| **Tidak menjawab dalam 5s** | Firewall belum mengizinkan onelabs, atau IP/port salah |
| Log: `folder … milik node X, bukan Y` | Kode node diganti setelah agen pernah jalan. Bila folder **belum berisi shard**, hapus penanda: `docker run --rm --user 0 -v <folder data>:/d alpine rm -rf /d/.rinjing-agen` lalu `docker compose up -d`. Bila sudah berisi data: daftarkan sebagai node baru dengan folder data baru — jangan ganti kode |
| Log: `bind: cannot assign requested address` | IP di `AGENT_LISTEN` bukan IP VM ini (`ip -4 addr`) |
| Log: `address already in use` | Port sudah dipakai → pilih port lain (ubah juga firewall & alamat di console) |
| Status **Versi beda** | Agen lebih lama/baru dari Rinjing utama → perbarui (lihat bawah) |

**Ingat:** mengubah `.env` → jalankan `docker compose up -d` (bukan `restart`, karena `restart` tidak
membaca ulang `.env`).

## Perbarui agen

```bash
cd ~/rinjing-agen && git pull --ff-only
sed -i "s/^AGENT_VERSION=.*/AGENT_VERSION=$(git describe --tags --always)/" .env
docker compose up -d --build
```

Urutan saat ada versi baru: **semua agen dulu, baru Rinjing utama**.

## Jangan

- Mengganti `AGENT_NODE_CODE` atau memindah/menyalin isi folder data ke node lain.
- Membuka port agen ke internet atau ke seluruh jaringan (`AGENT_ALLOW_FROM=*` hanya bila ada firewall).
- Memasang dua agen di server fisik yang sama dengan zona berbeda.
- Mematikan agen lama tanpa **Kosongkan (drain)** dulu di console begitu fitur pemindahan isi (C5) aktif.
