# 🦆 Quack-Cluster

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/USERNAME/quack-cluster)

**Quack-Cluster** adalah sebuah *serverless distributed SQL query engine* yang dirancang untuk menganalisis data skala besar langsung dari *object storage*. Proyek ini menggabungkan kemudahan Python dengan kekuatan orkestrasi dari [Ray](https://www.ray.io/) dan kecepatan pemrosesan data dari [DuckDB](https://duckdb.org/).

## ✨ Fitur Utama

* **Serverless & Terdistribusi**: Jalankan kueri SQL pada klaster yang dapat diskalakan secara elastis tanpa perlu manajemen server yang rumit.
* **Cepat**: Memanfaatkan kecepatan pemrosesan *in-memory* DuckDB dan format data Apache Arrow yang efisien.
* **Python-Native**: Mudah diintegrasikan ke dalam alur kerja data science dan data engineering yang sudah ada.
* **Cloud-Native**: Dirancang untuk membaca data langsung dari *object storage* seperti S3, GCS, dll.
* **Open Source**: Dibangun dengan teknologi open source terdepan dan terbuka untuk kontribusi.

## 🏛️ Arsitektur

Sistem Quack-Cluster terdiri dari Koordinator yang cerdas dan sekumpulan Worker yang kuat, diorkestrasi oleh Ray.

```
Pengguna (via API)
      |
      | SQL Query
      v
+-----------------------+
|   Koordinator (Otak)  |
| [FastAPI + SQLGlot]   |
+-----------------------+
      |
      | Rencana Eksekusi
      v
+---------------------------------+
| Platform Eksekusi (Ray Cluster) |
|---------------------------------|
|  +-----------+   +-----------+  |
|  | Worker 1  |   | Worker 2  |  | ...
|  | [Ray Actor] |   | [Ray Actor] |  |
|  |   DuckDB  |   |   DuckDB  |  |
|  +-----------+   +-----------+  |
+---------------------------------+
      |
      | Hasil Akhir
      v
   Pengguna
```

## 🚀 Memulai

Anda hanya memerlukan Docker dan `make` untuk menjalankan klaster Quack-Cluster secara lokal.

### 1. Prasyarat
* [Docker](https://www.docker.com/products/docker-desktop/)
* `make` (biasanya sudah terinstal di Linux/macOS, untuk Windows bisa menggunakan WSL)

### 2. Instalasi
```bash
# Clone repositori ini
git clone [https://github.com/USERNAME/quack-cluster.git](https://github.com/USERNAME/quack-cluster.git)
cd quack-cluster

# Buat data sampel
make data

# (Hanya dijalankan sekali) Instal semua dependensi
make install
```

### 3. Jalankan Klaster
Jalankan perintah ini untuk memulai klaster dengan 1 head node dan 2 worker node.
```bash
make up scale=2
```
Anda dapat memeriksa status klaster melalui **Ray Dashboard** di `http://localhost:8265`.

### 4. Jalankan Kueri Pertama Anda
Gunakan `curl` untuk mengirim kueri SQL ke API.

```bash
# Kueri semua data
curl -X 'POST' \
  'http://localhost:8000/query' \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM \"data_part_*\""}'

# Kueri dengan agregasi
curl -X 'POST' \
  'http://localhost:8000/query' \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT product, COUNT(*) FROM \"data_part_*\" GROUP BY product"}'
```

## 🛠️ Alur Kerja Pengembangan

Gunakan perintah `make` berikut untuk mengelola lingkungan pengembangan Anda:

* `make up`: Memulai semua kontainer.
* `make down`: Menghentikan kontainer (aman, tidak menghapus paket).
* `make logs`: Melihat log dari semua kontainer secara real-time.
* `make build`: Membangun ulang image Docker jika ada perubahan pada `Dockerfile`.
* `make clean`: Menghentikan kontainer DAN menghapus semua volume (termasuk paket yang sudah diunduh).

## 🗺️ Roadmap

* [ ] Dukungan untuk `JOIN` antar sumber data.
* [ ] Dukungan untuk Window Functions (`OVER`, `PARTITION BY`).
* [ ] Integrasi dengan *metadata catalog* seperti Apache Iceberg.
* [ ] Klien Python (SDK) yang lebih mudah digunakan.

## 🤝 Kontribusi

Kontribusi sangat kami harapkan! Silakan buka *issue* untuk diskusi fitur atau *pull request* untuk perbaikan.

## 📄 Lisensi

Proyek ini dilisensikan di bawah [Lisensi MIT](LICENSE).