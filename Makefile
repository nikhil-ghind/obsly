.PHONY: install dev-install bootstrap provision compose-up compose-down test lint

install:
	pip install -r requirements.txt

dev-install:
	pip install -e ".[dev]"

bootstrap:
	python -m obsly.kafka.admin

provision:
	python -m obsly.cli.provision all

compose-up:
	docker compose -f deploy/docker-compose.yml up -d --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v

test:
	pytest -q

lint:
	ruff check obsly tests
