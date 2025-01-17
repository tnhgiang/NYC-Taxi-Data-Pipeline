import pandas as pd
from ydata_profiling import ProfileReport

df = pd.read_parquet("./data/parquet/yellow_tripdata_2024-01.parquet")
profile = ProfileReport(df, title="Pandas Profiling Report", explorative=True)
profile.to_file("./scripts/data_profiling.html")
