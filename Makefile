.PHONY: up down logs test test-unit test-integration test-e2e lint format migrate shell-api shell-db shell-redis shell-kafka

# ─────────────────────────────────────────
# Stack local
# ─────────────────────────────────────────
up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

ps:
	docker-compose ps

# ─────────────────────────────────────────
# Testing
# ─────────────────────────────────────────
test:
	pytest tests/ -v --cov=src/

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

test-e2e:
	pytest tests/e2e/ -v

test-load:
	pytest tests/load/ -v

# ─────────────────────────────────────────
# Code quality
# ─────────────────────────────────────────
lint:
	ruff check src/ tests/

format:
	black src/ tests/

typecheck:
	mypy src/

# ─────────────────────────────────────────
# Database
# ─────────────────────────────────────────
migrate:
	alembic upgrade head

migrate-down:
	alembic downgrade -1

migrate-new:
	alembic revision --autogenerate -m "$(msg)"

# ─────────────────────────────────────────
# Shells de debug
# ─────────────────────────────────────────
shell-api:
	docker-compose exec api bash

shell-db:
	docker-compose exec postgres psql -U postgres -d analysis_db

shell-redis:
	docker-compose exec redis redis-cli

shell-kafka:
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# ─────────────────────────────────────────
# Setup inicial
# ─────────────────────────────────────────
setup:
	python3.11 -m venv venv
	venv/bin/pip install -r requirements-dev.txt
	cp .env.example .env
	@echo "Edita .env con tus valores locales y luego ejecuta: make up"
