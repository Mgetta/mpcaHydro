import duckdb
from pathlib import Path
from typing import Union
from mpcaHydro.warehouse import queries


def export_station_to_csv(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str,
    output_path: Union[str, Path]
) -> None:
    """Export analytics observation data for a station to CSV.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.
    output_path : str or Path
        Destination CSV file path.
    """
    df = queries.get_station_data(con, station_id, station_origin)
    df.to_csv(output_path, index=False)


def export_raw_to_csv(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str,
    output_path: Union[str, Path]
) -> None:
    """Export raw staging data for a station to CSV.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.
    output_path : str or Path
        Destination CSV file path.
    """
    df = queries.get_raw_data(con, station_id, station_origin)
    df.to_csv(output_path, index=False)