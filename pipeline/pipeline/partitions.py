from datetime import datetime

from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(start_date=datetime(2024, 1, 1))
