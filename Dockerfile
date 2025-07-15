# Menggunakan image resmi Python 3.9 yang stabil
FROM python:3.9-slim

# Set direktori kerja
WORKDIR /app

# Salin file dependensi terlebih dahulu untuk caching
COPY pyproject.toml ./

# SOLUSI: Tambahkan "[dev]" untuk menginstal dependensi pengembangan
# Tanda kutip penting untuk shell
RUN pip install -e ".[dev]"

# Salin sisa kode proyek
COPY . .

# Port untuk Ray Dashboard, GCS, dan FastAPI
EXPOSE 8265 6379 8000

# Perintah default akan di-override di docker-compose
CMD ["echo", "This is the base image for Quack-Cluster."]