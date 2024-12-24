from pathlib import Path

import polars as pl
from tqdm import tqdm


def parquet2csv(parquet_path, csv_path):
    """Convert parquet file to csv file"""
    parquet_path = Path(parquet_path)
    csv_path = Path(csv_path)
    # Read the Parquet file into a Polars DataFrame
    df = pl.read_parquet(str(parquet_path))

    # Write the DataFrame to the CSV file
    df.write_csv(csv_path)


def get_csv_path(parquet_path):
    """Get the corresponding csv file path for a given parquet file path"""
    parquet_path = Path(parquet_path)
    csv_path = (
        parquet_path.parent.parent / "csv" / parquet_path.with_suffix(".csv").name
    )

    return csv_path


def main():
    parquet_dir = Path("./data/parquet")
    parquet_path_list = list(parquet_dir.glob("*.parquet"))

    # Convert all parquet files to csv files
    for parquet_path in tqdm(parquet_path_list):
        parquet2csv(parquet_path, get_csv_path(parquet_path))


if __name__ == "__main__":
    main()
