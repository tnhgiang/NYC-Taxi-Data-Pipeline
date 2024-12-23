.PHONY: up

dagster_up:
	docker compose -f dagster/compose.dagster.yaml --env-file .env up --build

dagster_down:
	docker compose -f dagster/compose.dagster.yaml --env-file .env down
