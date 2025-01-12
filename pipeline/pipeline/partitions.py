from datetime import datetime

from dagster import MonthlyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(start_date=datetime(2009, 1, 1))
