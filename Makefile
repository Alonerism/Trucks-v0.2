DOCKER_COMPOSE=docker compose

.PHONY: up down logs rebuild test test-backend shell-backend

up:
	$(DOCKER_COMPOSE) up -d --build

down:
	$(DOCKER_COMPOSE) down -v

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

rebuild:
	$(DOCKER_COMPOSE) build --no-cache

api:
	poetry run uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000

ui-lovable:
	cd ui-lovable && npm i && npm run dev

.PHONY: api ui-lovable

# Run tests inside backend container
test:
	$(DOCKER_COMPOSE) exec -T backend poetry run pytest -q

test-backend: test

# Open a shell into the backend container
shell-backend:
	$(DOCKER_COMPOSE) exec backend bash
