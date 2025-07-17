# Mencegah 'make' bingung jika ada file dengan nama yang sama seperti target
.PHONY: help build up down clean logs test lint data

# Menetapkan nilai default 1 untuk skala worker, bisa di-override dari command line
scale ?= 1

## =================================================================================
## 							Perintah Utama (Lifecycle)
## =================================================================================

help:
	@echo "Available commands:"
	@echo ""
	@echo "  --- Lifecycle ---"
	@echo "  build         : (RUN FIRST) Build/re-build the image and install all dependencies."
	@echo "  up scale=N    : Start the cluster with N workers (default 1)."
	@echo "  down          : Stop and remove containers (SAFE)."
	@echo "  clean         : Stop containers AND DELETE all volumes."
	@echo ""
	@echo "  --- Development ---"
	@echo "  test          : Run the test suite against the running cluster."
	@echo "  logs          : View real-time logs from all services."
	@echo "  lint          : Run linter and formatter on the code."
	@echo "  data          : Generate sample data files."
	@echo ""

build:
	@echo "🛠️ Building Docker image and installing dependencies via pip..."
	docker compose build

up:
	@echo "🚀 Starting Quack-Cluster with $(scale) worker(s)..."
	docker compose up -d --scale ray-worker=$(scale)
	@echo "✅ Cluster is up! Ray Dashboard: http://localhost:8265"

down:
	@echo "泊 Stopping Quack-Cluster containers..."
	docker compose down

## =================================================================================
## 							Perintah Pengembangan & Utilitas
## =================================================================================

test:
	@echo "🧪 Running tests..."
	docker compose exec ray-head pytest

logs:
	docker compose logs -f

lint:
	@echo "✨ Linting and formatting code..."
	# FIX: Jalankan ruff langsung, bukan via PDM
	docker compose run --rm ray-head ruff check . && ruff format .

data:
	@echo "🦆 Generating sample data..."
	# FIX: Jalankan skrip Python langsung, bukan via PDM
	docker compose run --rm ray-head python -c 'from quack_cluster.settings import generate_sample_data; generate_sample_data()'

## =================================================================================
## 							Perintah Perawatan (Maintenance)
## =================================================================================

clean:
	@echo "🔥 WARNING: Deleting all containers AND volumes..."
	docker compose down -v