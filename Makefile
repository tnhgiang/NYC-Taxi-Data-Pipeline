.PHONY: up

dagster_up:
	docker compose -f dagster/compose.dagster.yaml --env-file .env up -d

dagster_down:
	docker compose -f dagster/compose.dagster.yaml --env-file .env down

spark_up:
	docker compose -f spark/compose.spark.yaml --env-file .env up -d

spark_down:
	docker compose -f spark/compose.spark.yaml --env-file .env down

up: dagster_up spark_up
	docker compose up -d

down: dagster_down spark_down
	docker compose down

setup_database:
	./scripts/database/setup_mysql_and_create_schema.sh

load_csv:
	./scripts/database/load_csv_to_mysql.sh ./data/csv/yellow_tripdata_2024-01.csv

test:
	pytest
