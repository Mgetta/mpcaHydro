
import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Optional

AGG_DEFAULTS = {
    'cfs': 'mean',
    'mg/l': 'mean',
    'degf': 'mean',
    'lb': 'sum'
}

UNIT_DEFAULTS = {
    'Q': 'cfs',
    'QB': 'cfs',
    'TSS': 'mg/l',
    'TP': 'mg/l',
    'OP': 'mg/l',
    'TKN': 'mg/l',
    'N': 'mg/l',
    'WT': 'degf',
    'WL': 'ft'
}


def get_outlets(con: duckdb.DuckDBPyConnection, model_name: str) -> pd.DataFrame:
    """Query outlet station-reach pairs for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    model_name : str
        Repository name.

    Returns
    -------
    pandas.DataFrame
        Rows from ``outlets.station_reach_pairs`` ordered by
        ``outlet_id``.
    """
    query = '''
    SELECT *
    FROM outlets.station_reach_pairs
    WHERE repository_name = ?
    ORDER BY outlet_id'''
    return con.execute(query, [model_name]).fetch_df()


def get_station_ids(
    con: duckdb.DuckDBPyConnection,
    station_origin: Optional[str] = None
) -> List[str]:
    """Return distinct station IDs from the analytics observations.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_origin : str, optional
        Filter to a specific origin (``'wiski'`` or ``'equis'``).
        When ``None``, all stations are returned.

    Returns
    -------
    list of str
    """
    if station_origin is None:
        query = '''
        SELECT DISTINCT station_id, station_origin
        FROM analytics.observations'''
        df = con.execute(query).fetch_df()
    else:
        query = '''
        SELECT DISTINCT station_id
        FROM analytics.observations
        WHERE station_origin = ?'''
        df = con.execute(query, [station_origin]).fetch_df()
    return df['station_id'].to_list()


def get_observation_data(
    con: duckdb.DuckDBPyConnection,
    station_ids: List[str],
    constituent: str,
    agg_period: Optional[str] = None
) -> pd.DataFrame:
    """Retrieve observation data for given stations and constituent.

    Optionally resamples to a coarser time period using the default
    aggregation function for the constituent's unit (mean for
    concentrations and flow, sum for mass loads).

    **Why resample?**
    Model calibration often compares at daily or monthly resolution.
    Resampling in the query layer avoids repeating aggregation logic in
    every downstream consumer.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_ids : list of str
        Station identifiers.
    constituent : str
        Constituent abbreviation (e.g. ``'TP'``).
    agg_period : str, optional
        Pandas resample period string (e.g. ``'D'`` for daily, ``'M'``
        for monthly).  If ``None``, data is returned at its native
        hourly resolution.

    Returns
    -------
    pandas.DataFrame
        Indexed by ``datetime`` with an ``observed`` column.
        ``df.attrs`` contains ``unit`` and ``constituent`` metadata.
    """
    query = '''
    SELECT *
    FROM analytics.observations
    WHERE station_id IN ? AND constituent = ?'''
    df = con.execute(query, [station_ids, constituent]).fetch_df()

    unit = UNIT_DEFAULTS.get(constituent, 'mg/l')
    agg_func = AGG_DEFAULTS.get(unit, 'mean')

    if df['datetime'].isnull().any():
        df.set_index('date', inplace=True)
    else:
        df.set_index('datetime', inplace=True)
    df.attrs['unit'] = unit
    df.attrs['constituent'] = constituent

    if agg_period is not None:
        df = df[['value']].resample(agg_period).agg(agg_func)
        df.attrs['agg_period'] = agg_period

    df.rename(columns={'value': 'observed'}, inplace=True)
    return df.dropna(subset=['observed'])

def get_outlet_by_station(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str
) -> Optional[int]:
    """Return the outlet_id for a given station, if it exists.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    int or None
        The outlet_id if it exists, otherwise None.
    """
    query = '''
    SELECT outlet_id
    FROM outlets.outlet_stations
    WHERE station_id = ? AND station_origin = ?'''
    result = con.execute(query, [station_id, station_origin]).fetchone()
    return result[0] if result is not None else None


def get_outlet_data(
    con: duckdb.DuckDBPyConnection,
    outlet_id: int,
    constituent: str,
    agg_period: str = 'D'
) -> pd.DataFrame:
    """Retrieve outlet-level observations with paired flow and baseflow.

    Queries ``analytics.outlet_observations_with_flow``, which joins
    constituent observations with discharge (``Q``) and baseflow (``QB``)
    at matching timestamps.  This is the primary query for HSPF
    calibration: it produces a DataFrame with ``observed``,
    ``observed_flow``, and ``observed_baseflow`` columns that can be
    directly compared to model output.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    outlet_id : int
        Outlet group identifier.
    constituent : str
        Constituent abbreviation (e.g. ``'TSS'``).
    agg_period : str, default ``'D'``
        Pandas resample period string.  Use ``None`` for hourly.

    Returns
    -------
    pandas.DataFrame
        Indexed by ``datetime`` with columns ``observed``,
        ``observed_flow``, ``observed_baseflow``.
    """
    query = '''
    SELECT *
    FROM analytics.outlet_observations_with_flow
    WHERE outlet_id = ? AND constituent = ?'''
    df = con.execute(query, [outlet_id, constituent]).fetch_df()

    unit = UNIT_DEFAULTS.get(constituent, 'mg/l')
    agg_func = AGG_DEFAULTS.get(unit, 'mean')

    if df['datetime'].isnull().any():
        df.set_index('date', inplace=True)
    else:
        df.set_index('datetime', inplace=True)

    df.attrs['unit'] = unit
    df.attrs['constituent'] = constituent

    if agg_period is not None:
        df = df[['value', 'flow_value', 'baseflow_value']].resample(agg_period).agg(agg_func)
        df.attrs['agg_period'] = agg_period

    df.rename(columns={
        'value': 'observed',
        'flow_value': 'observed_flow',
        'baseflow_value': 'observed_baseflow'
    }, inplace=True)
    return df.dropna(subset=['observed'])


def get_station_data(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str,
) -> pd.DataFrame:
    """Retrieve all analytics observations for a specific station.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    pandas.DataFrame
    """
    query = '''
    SELECT *
    FROM analytics.observations
    WHERE station_id = ? AND station_origin = ?'''
    return con.execute(query, [station_id, station_origin]).fetch_df()


def get_raw_data(
    con: duckdb.DuckDBPyConnection,
    station_id: str,
    station_origin: str
) -> pd.DataFrame:
    """Retrieve raw staging data for a specific station.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
        If *station_origin* is not recognised.
    """
    if station_origin.lower() == 'equis':
        query = '''
        SELECT *
        FROM staging.equis
        WHERE SYS_LOC_CODE = ?'''
    elif station_origin.lower() == 'wiski':
        query = '''
        SELECT *
        FROM staging.wiski
        WHERE station_no = ?'''
    else:
        raise ValueError(f'Station origin {station_origin} not recognized.')
    return con.execute(query, [station_id]).fetch_df()


def get_constituent_summary(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return a summary of sample counts and date ranges by constituent.

    Aggregates ``analytics.observations`` by ``(station_id,
    station_origin, constituent)`` and reports the sample count, earliest
    year, and latest year.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
    """
    query = '''
    SELECT
      station_id,
      station_origin,
      constituent,
      COUNT(*) AS sample_count,
      year(MIN(date)) AS start_date,
      year(MAX(date)) AS end_date
    FROM
      analytics.observations
    GROUP BY
      constituent, station_id, station_origin
    ORDER BY
      sample_count'''
    return con.execute(query).fetch_df()




def get_equis_template(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return an empty DataFrame matching the ``staging.equis`` schema.

    Useful for constructing manual data frames that can be appended to
    the staging table.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
        Zero rows, columns matching ``staging.equis``.
    """
    query = '''SELECT * FROM staging.equis LIMIT 0'''
    return con.execute(query).fetch_df()


def get_wiski_template(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return an empty DataFrame matching the ``staging.wiski`` schema.

    Useful for constructing manual data frames that can be appended to
    the staging table.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
        Zero rows, columns matching ``staging.wiski``.
    """
    query = '''SELECT * FROM staging.wiski LIMIT 0'''
    return con.execute(query).fetch_df()


def outlet_summary(con: duckdb.DuckDBPyConnection):
    """Return the outlet-level constituent summary report.

    Queries ``reports.outlet_constituent_summary`` for sample counts,
    averages, min/max values, and date ranges per outlet and constituent.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
    """
    query = '''
    SELECT *,
    FROM 
        reports.outlet_constituent_summary
    ORDER BY
        outlet_id,
        constituent
    '''
    df = con.execute(query).fetch_df()
    return df
        

def wiski_qc_counts(con: duckdb.DuckDBPyConnection):
    """Return WISKI quality-code frequency counts.

    Queries ``reports.wiski_qc_count`` which tallies quality codes per
    station and parameter, joined to human-readable descriptions from
    ``mappings.wiski_quality_codes``.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
    """
    query = '''
    SELECT *,
    FROM 
        reports.wiski_qc_count
    ORDER BY
        station_no,
        parametertype_name
    '''
    df = con.execute(query).fetch_df()
    return df

def station_summary(con: duckdb.DuckDBPyConnection, constituent: str = None):
    """Return per-station constituent summary statistics.

    Queries ``reports.constituent_summary`` for sample counts, averages,
    min/max values, and date ranges.  Optionally filters to a single
    constituent.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    constituent : str, optional
        Filter to a specific constituent abbreviation.

    Returns
    -------
    pandas.DataFrame
    """
    
    query = '''
    SELECT *,
    FROM 
        reports.constituent_summary
    ORDER BY
        station_id,
        station_origin,
        constituent
    '''
    df = con.execute(query).fetch_df()
    if constituent is not None:
        df = df[df['constituent'] == constituent]
    return df

def station_reach_pairs(con: duckdb.DuckDBPyConnection):
    """Return all station-reach pair records from the outlets schema.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    pandas.DataFrame
    """
    query = '''
    SELECT *,
    FROM 
        outlets.station_reach_pairs
    ORDER BY
        outlet_id,
        station_id
    '''
    df = con.execute(query).fetch_df()
    return df

def get_wiski_data(con: duckdb.DuckDBPyConnection, constituent: str):
    """Retrieve raw WISKI staging data for a specific station.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.

    Returns
    -------
    pandas.DataFrame
    """
    query = """
        SELECT *
        FROM analytics.wiski
        WHERE constituent = ?  -- discharge
        ORDER BY station_id, date, time
    """
    df = con.execute(query, [constituent]).fetch_df()
    return df





# Helpful Queries:
def get_outlets_by_model(con: duckdb.DuckDBPyConnection, model_name: str):
    """Query the outlet database for all station-reach pairs in a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    pandas.DataFrame
        Rows from ``outlets.station_reach_pairs`` for *model_name*.
    """
    df = con.execute(
        """
        SELECT r.*
        FROM outlets.station_reach_pairs r
        WHERE r.repository_name = ?
        """,
        [model_name]
    ).fetchdf()
    return df

def get_outlets_by_reach(con: duckdb.DuckDBPyConnection, reach_id: int, model_name: str):
    """Return outlet rows containing a specific reach within a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    reach_id : int
        HSPF model reach identifier.
    model_name : str
        Repository name.

    Returns
    -------
    pandas.DataFrame
    """
    df = con.execute(
        """
        SELECT r.*
        FROM outlets.station_reach_pairs r
        WHERE r.reach_id = ? AND r.repository_name = ?
        """,
        [reach_id, model_name]).fetchdf()
    return df

def get_outlets_by_station(con: duckdb.DuckDBPyConnection, station_id: str, station_origin: str):
    """Return outlet rows for a specific station and data origin.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    pandas.DataFrame
    """

    df = con.execute(
    """
    SELECT r.*
    FROM outlets.station_reach_pairs r
    WHERE r.station_id = ? AND r.station_origin = ?
    """,
    [station_id, station_origin]).fetchdf()
    return df

def get_station_opnids(con: duckdb.DuckDBPyConnection, station_id: str, station_origin: str):
    """Return reach IDs associated with a station from the outlet database.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    list of int
        Model reach IDs (``opnids``) linked to the station.
    """
    df = con.execute(
        """
        SELECT r.reach_id
        FROM outlets.station_reach_pairs r
        WHERE r.station_id = ? AND r.station_origin = ?
        """,
        [station_id, station_origin]).fetchdf()
    return df['reach_id'].tolist()

def get_outlet_opnids(con: duckdb.DuckDBPyConnection, outlet_id: int):
    """Return the unique set of reach IDs for an outlet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    outlet_id : int
        Outlet group identifier.

    Returns
    -------
    list of int
    """
    df = con.execute(
        """
        SELECT r.reach_id
        FROM outlets.station_reach_pairs r
        WHERE r.outlet_id = ?
        """,
        [outlet_id]).fetchdf()
    return list(set(df['reach_id'].tolist()))

def get_outlet_stations(con: duckdb.DuckDBPyConnection, outlet_id: int):
    """Return station identifiers and origins for an outlet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    outlet_id : int
        Outlet group identifier.

    Returns
    -------
    list of dict
        Each dict has keys ``'station_id'`` and ``'station_origin'``.
    """
    df = con.execute(
        """
        SELECT r.station_id, r.station_origin
        FROM outlets.station_reach_pairs r
        WHERE r.outlet_id = ?
        """,
        [outlet_id]).fetchdf()
    return df[['station_id', 'station_origin']].drop_duplicates()
