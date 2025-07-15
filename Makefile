.PHONY: help build up down clean logs lint data

scale ?= 1

help:
	@echo "Available commands:"
	@echo "  build         : Build the docker images."
	@echo "  up scale=N    : Start the cluster with N workers (default 1)."
	@echo "  down          : Stop and remove containers."
	@echo "  clean         : Stop containers AND DELETE all volumes."
	@echo "  logs          : View logs from all services."

build:
	docker compose build

up:
	docker compose up -d --scale ray-worker=$(scale)

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f