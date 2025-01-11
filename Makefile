.PHONY: pipeline_up pipeline_down up down setup_database load_csv test dbt_compile

pipeline_up:
	docker compose -f compose/compose.etl.yaml \
				-f compose/compose.dw.yaml \
				--env-file .env up -d

pipeline_down:
	docker compose -f compose/compose.etl.yaml \
				-f compose/compose.dw.yaml \
				--env-file .env down

up: pipeline_up
	docker compose -f compose/compose.serving.yaml --env-file .env up -d

down: pipeline_down
	docker compose -f compose/compose.serving.yaml --env-file .env down -d

setup_database:
	./scripts/database/setup_mysql_and_create_schema.sh

load_csv:
	./scripts/database/load_csv_to_mysql.sh ./data/csv/yellow_tripdata_2024-01.csv

test:
	pytest

dbt_compile:
	docker exec pipeline bash -c "cd dbt_nyc && dbt compile"
