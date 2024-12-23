.PHONY: up

dagster_up:
	docker compose -f dagster/compose.dagster.yaml --env-file .env up --build -d

dagster_down:
	docker compose -f dagster/compose.dagster.yaml --env-file .env down

up: dagster_up
	docker compose up --build -d

down: dagster_down
	docker compose down
