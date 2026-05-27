"""Parquet file storage helpers. Does not replace warehouse — lives alongside it."""

from pathlib import Path
from typing import List
import pandas as pd
import duckdb



# Natural keys for each source — the columns that uniquely identify a record.
# Used to filter out duplicates when appending new downloads.
NATURAL_KEYS = {'wiski': ['ts_id', 'Timestamp'],
                'equis': ['SAMPLE_ID','TEST_ID','CAS_RN']}



def staging_dir(data_dir: Path, source: str) -> Path:
    p = data_dir / "staging" / source
    p.mkdir(parents=True, exist_ok=True)
    return p

def derived_dir(data_dir: Path, name: str) -> Path:
    p = data_dir / "derived" / name
    p.mkdir(parents=True, exist_ok=True)
    return p

def staging_path(data_dir: Path, source: str, station_id: str) -> Path:
    return staging_dir(data_dir, source) / f"{station_id}.parquet"

def derived_path(data_dir: Path, name: str, identifier: str) -> Path:
    return derived_dir(data_dir, name) / f"{identifier}.parquet"


def read_staging_glob(data_dir: Path, source: str) -> str:
    return (staging_dir(data_dir, source) / "*.parquet").as_posix()


def read_derived_glob(data_dir: Path, name: str) -> str:
    return (derived_dir(data_dir, name) / "*.parquet").as_posix()


def save_baseflow(df: pd.DataFrame, data_dir: Path, station_id: str) -> None:
    """Save baseflow separation results to derived."""
    # Store results with same schema as staging
    savepath = derived_path(data_dir, 'baseflow', f'{station_id}').with_suffix('.parquet')
    duckdb.sql(f"COPY df TO '{savepath.as_posix()}' (FORMAT PARQUET)")


def save_staging(
    df: pd.DataFrame,
    data_dir: Path,
    source: str,
    station_id: str,
) -> tuple[Path, int]:
    """Save raw download to parquet, deduplicating against existing data.
    
    Uses NATURAL_KEYS[source] to identify which rows are new.
    Returns (path, new_row_count).
    """
    _validate_natural_keys(source, df)
    
    keys = NATURAL_KEYS[source]
    path = staging_path(data_dir, source, station_id)

    if path.exists():
        df_existing = pd.read_parquet(path)
        # Filter to only genuinely new rows
        merged = df.merge(df_existing[keys], on=keys, how='left', indicator=True)
        df_new = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

        if df_new.empty:
            return path, 0

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        new_count = len(df_new)
    else:
        df_combined = df
        new_count = len(df)


    duckdb.sql(f"COPY df_combined TO '{path.as_posix()}' (FORMAT PARQUET)")
    return path, new_count


def drop_stations(station_ids: List[str], data_dir: Path, source: str) -> None:
    """Delete staging files for the given station IDs and source."""
    for station_id in station_ids:
        path = staging_path(data_dir, source, station_id)
        if path.exists():
            path.unlink()
            print(f"Deleted {path}")
        else:
            print(f"No file to delete for {station_id} at {path}")



def _validate_natural_keys(source: str, df: pd.DataFrame) -> None:
    """Check that the expected natural key columns are present in the DataFrame."""
    if source not in NATURAL_KEYS:
        raise ValueError(f"Unknown source '{source}'. Known sources: {list(NATURAL_KEYS)}")
    
    missing_keys = [key for key in NATURAL_KEYS[source] if key not in df.columns]
    if missing_keys:
        raise ValueError(f"Missing natural key columns for source '{source}': {missing_keys}")
    
    # count of total rows vs unique rows based on natural keys
    total_rows = len(df)
    unique_rows = len(df.drop_duplicates(subset=NATURAL_KEYS[source]))
    if total_rows != unique_rows:
        raise ValueError(f"DataFrame for source '{source}' contains duplicate records based on natural keys. Total rows: {total_rows}, Unique rows: {unique_rows}")
    
    # check for nulls in natural key columns
    for key in NATURAL_KEYS[source]:
        if df[key].isnull().any():
            raise ValueError(f"Natural key column '{key}' for source '{source}' contains null values, which may lead to incorrect deduplication.")
