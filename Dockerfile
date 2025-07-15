# Menggunakan image resmi Python 3.9 yang stabil
FROM python:3.9-slim

# Set direktori kerja
WORKDIR /app

# Salin file dependensi terlebih dahulu untuk caching
COPY pyproject.toml ./

# Install semua dependensi dari pyproject.toml
# Opsi -e (editable) penting agar hot-reloading tetap berfungsi
RUN pip install -e .

# Salin sisa kode proyek
COPY . .

# Port untuk Ray Dashboard, GCS, dan FastAPI
EXPOSE 8265 6379 8000

# Perintah default akan di-override di docker-compose
CMD ["echo", "This is the base image for Quack-Cluster."]