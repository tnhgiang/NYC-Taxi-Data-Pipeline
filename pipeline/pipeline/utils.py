from datetime import datetime
from pathlib import Path

import polars as pl
import pytz


def get_current_datetime(tz="Asia/Ho_Chi_Minh") -> datetime:
    vietname_tz = pytz.timezone(tz)
    dt = datetime.now(vietname_tz)

    return dt


def get_current_datetime_str(only_time=False) -> str:
    """Get current time in Vietnam timezone."""
    dt = get_current_datetime()

    if only_time:
        return dt.strftime("%H_%M_%S_%f")
    return dt.strftime("%Y-%m-%d_%H:%M:%S:%f")


def get_size_in_MB(path: str) -> float:
    """Get size of a file or a directory in MB."""
    path = Path(path)

    if path.is_file():
        return path.stat().st_size * 1e-6

    return sum(f.stat().st_size for f in path.glob('*/*') if f.is_file()) * 1e-6


def to_markdown(pl_df: pl.DataFrame) -> str:
    """Convert Polars DataFrame to markdown table."""
    with pl.Config(
        tbl_formatting="MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
    ):
        return repr(pl_df)
