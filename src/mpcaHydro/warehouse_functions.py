# -*- coding: utf-8 -*-
"""
warehouse_functions
===================

Procedural helpers and the :class:`DataManagerWrapper` convenience class
for interacting with the mpcaHydro data warehouse.

Overview
--------
This module sits between the low-level :mod:`warehouse` database
operations and the end-user workflow.  It provides:

* **Procedural functions** that combine download, ETL, and warehouse
  loading into single calls (e.g. :func:`download_wiski_data`).
* **Query functions** that retrieve observation data, outlet data, and
  summary statistics from the analytics layer.
* **Export functions** for writing query results to CSV or Parquet.
* The :class:`DataManagerWrapper` class, which bundles all of the above
  behind a single object that manages its own database connections.

Typical workflow (procedural)
-----------------------------
::

    from mpcaHydro import warehouse, warehouse_functions as wf

    # Initialise a new warehouse
    wf.init_warehouse('observations.duckdb', reset=True)

    # Open a connection and download + load WISKI data
    with warehouse.connect('observations.duckdb') as con:
        wf.download_wiski_data(con, ['E66050001'], start_year=2000)

Typical workflow (wrapper class)
---------------------------------
::

    from mpcaHydro.warehouse_functions import DataManagerWrapper

    dm = DataManagerWrapper('observations.duckdb', reset=True)
    dm.download_wiski_data(['E66050001'], start_year=2000)
    df = dm.get_observation_data(['E66050001'], 'Q', agg_period='D')

Key data structures
-------------------
``AGG_DEFAULTS``
    Maps unit strings to default aggregation functions (``'mean'`` for
    concentration and flow units, ``'sum'`` for mass units like ``'lb'``).

``UNIT_DEFAULTS``
    Maps constituent abbreviations to their expected unit strings.
"""

from pathlib import Path
from typing import List, Optional, Union

import duckdb
import pandas as pd

from mpcaHydro import equis, storage, warehouse, wiski
import baseflow


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

def update_views(con: duckdb.DuckDBPyConnection) -> None:
    """Refresh all analytics and reports views.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    from mpcaHydro import warehouse
    warehouse.update_views(con)

def download_wiski_data(
    con: duckdb.DuckDBPyConnection,
    data_dir: Union[str, Path],
    station_ids: List[str],
    start_year: int = 1996,
    end_year: int = 2030,
    overwrite: bool = True) -> None:
    """Download WISKI data, transform it, and load both raw and analytics.

    End-to-end convenience function that:

    1. Downloads raw data from the KISTERS API via :func:`wiski.download`.
    2. Transforms it via :func:`wiski.transform`.
    3. Optionally drops existing data for the stations (when *overwrite*
       is ``True``).
    4. Appends raw data to ``staging.wiski`` and transformed data to
       ``analytics.wiski``.
    5. Refreshes database views.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    station_ids : list of str
        WISKI station numbers to download.
    start_year : int, default 1996
        First calendar year.
    end_year : int, default 2030
        Last calendar year.
    filter_qc_codes : bool, default True
        Apply quality-code filtering.
    data_codes : list of int, optional
        Custom quality-code whitelist.
    baseflow_method : str, default ``'Boughton'``
        Algorithm for baseflow separation.
    overwrite : bool, default True
        Drop existing data for these stations before inserting.
    """    

    df = wiski.download(station_ids, start_year=start_year, end_year=end_year)
    if not df.empty:
        for sid in df['station_no'].unique():
            df_station = df[df['station_no'] == sid]
            storage.save_staging(df_station, data_dir, 'wiski', sid)
        warehouse._refresh_staging_views(con,data_dir)
    else:
        print('No data necessary for HSPF calibration from wiski for:', station_ids)


def download_equis_data(
    con: duckdb.DuckDBPyConnection,
    data_dir: Union[str, Path],
    station_ids: List[str],
    oracle_username: Optional[str] = None,
    oracle_password: Optional[str] = None,
    overwrite: bool = True) -> None:
    """Download EQuIS data, transform it, and load both raw and analytics.

    End-to-end convenience function that:

    1. Connects to the Oracle EQuIS database.
    2. Downloads raw data via :func:`equis.download`.
    3. Transforms it via :func:`equis.transform`.
    4. Optionally drops existing data for the stations.
    5. Appends raw data to ``staging.equis`` and transformed data to
       ``analytics.equis``.
    6. Refreshes database views.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    station_ids : list of str
        EQuIS ``SYS_LOC_CODE`` values.
    oracle_username : str
        Oracle database username.
    oracle_password : str
        Oracle database password.
    overwrite : bool, default True
        Drop existing data for these stations before inserting.
    """    
    with equis.connect(user=oracle_username, password=oracle_password) as oracle_conn:
        print('Connected to Oracle database.')
        df = equis.download(station_ids, connection=oracle_conn)
    if not df.empty:
        for sid in df['SYS_LOC_CODE'].unique():
            df_station = df[df['SYS_LOC_CODE'] == sid]
            storage.save_staging(df_station, data_dir, 'equis', sid)
        warehouse._refresh_staging_views(con,data_dir)
    else:
        print('No data necessary for HSPF calibration from equis for:', station_ids)
    


def compute_baseflow(con, data_dir, method='Boughton', min_size=30):
    """Compute baseflow from hourly Q data and store in derived."""
    
    # # Check if already computed for this station+method
    # existing = con.execute("""
    #     SELECT 1 FROM derived.baseflow
    #     WHERE station_id = ? AND method = ?
    # """, [station_id, method]).fetchone()
    # if existing:
    #     return  # already done
    
    # Pull the clean flow data (from analytics or staging — your call)
    df_q = con.execute("""
        SELECT *
        FROM analytics.wiski
        WHERE constituent = 'Q'  -- discharge
        ORDER BY station_id, date, time
    """).fetch_df()

    for station_id in df_q['station_id'].unique():
        df_q_station = df_q.loc[df_q['station_id'] == station_id]
        if not df_q_station.empty and len(df_q_station) >= min_size:    
            # Run the Python algorithm
            print(len(df_q_station))
            bf = baseflow.separation(df_q_station.set_index('datetime')[['value']], method=method)
            df_q_station.loc[:,'value'] = bf[method].values # ensure numeric for transformations
            df_q_station = df_q_station.dropna(subset=['value']) # drop rows where baseflow couldn't be computed
            df_q_station = df_q_station[['datetime','date','time','value','station_id','unit']].copy()
            df_q_station['constituent'] = 'QB'  # baseflow constituent
            df_q_station['station_origin'] = 'wiski'
            df_q_station['date'] = df_q_station['datetime'].dt.date
            # Store results with same schema as staging
            derived_path = storage.derived_path(data_dir, 'baseflow', f'{station_id}').with_suffix('.parquet')
            duckdb.sql(f"COPY df_q_station TO '{derived_path.as_posix()}' (FORMAT PARQUET)")
        else:
            print(f"Station {station_id} has insufficient data for baseflow separation (n={len(df_q_station)})")
    warehouse._refresh_derived_views(con, data_dir)

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
    station_origin: str
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
    df = get_station_data(con, station_id, station_origin)
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
    df = get_raw_data(con, station_id, station_origin)
    df.to_csv(output_path, index=False)


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

class DataManagerWrapper:
    """Convenience wrapper that manages DuckDB connections for warehouse operations.

    ``DataManagerWrapper`` provides the same functionality as the
    module-level procedural functions, but bundles them behind a single
    object that owns the database path and creates context-managed
    connections on every call.  This removes the need for callers to open
    and close connections manually.

    **When to use DataManagerWrapper:**

    * Interactive exploration in a Jupyter notebook where you want a
      persistent handle to the warehouse.
    * Application code that performs many sequential operations on the
      same database.

    **When to use the procedural functions instead:**

    * When you already have an open ``duckdb.DuckDBPyConnection`` (e.g.
      inside a ``with`` block).
    * When you need fine-grained control over transaction boundaries.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB warehouse file.
    reset : bool, default False
        If ``True``, re-initialise the database (delete and recreate).

    Attributes
    ----------
    db_path : pathlib.Path
        Resolved path to the DuckDB file.

    Examples
    --------
    >>> dm = DataManagerWrapper('observations.duckdb', reset=True)
    >>> dm.download_wiski_data(['E66050001'])
    >>> df = dm.get_observation_data(['E66050001'], 'Q', agg_period='D')
    """
    
    def __init__(self, data_dir: Union[str, Path], reset: bool = False):
        """Initialise the wrapper with a database path.

        Parameters
        ----------
        db_path : str or Path
            Path to the DuckDB warehouse file.
        reset : bool, default False
            Re-initialise the database when ``True``.
        """

        self.con = warehouse.create_session(Path(data_dir).as_posix())
        self.data_dir = Path(data_dir)
        if reset:
            raise NotImplementedError('Reset functionality not implemented yet')

    def _refresh_views(self):
        """Refresh all database views."""
        warehouse._refresh_staging_views(self.con, self.data_dir)
        warehouse._refresh_derived_views(self.con, self.data_dir)
        update_views(self.con)


    def update_views(self) -> None:
        """Refresh all analytics and reports views."""
        update_views(self.con)
    
    def wiski_qc_counts(self):
        """Return WISKI quality-code frequency counts.

        See :func:`wiski_qc_counts` for details.
        """
        return wiski_qc_counts(self.con)

        
    def station_summary(self, constituent: str = None):
        """Return per-station constituent summary statistics.

        See :func:`station_summary` for details.
        """
        return station_summary(self.con, constituent)
        
    def station_reach_pairs(self):
        """Return all station-reach pair records.

        See :func:`station_reach_pairs` for details.
        """
        return station_reach_pairs(self.con)

        
    def outlet_summary(self):
        """Return outlet-level constituent summary.

        See :func:`outlet_summary` for details.
        """
        return outlet_summary(self.con)
        
    def download_wiski_data(
        self,
        station_ids: List[str],
        start_year: int = 1996,
        end_year: int = 2030,
        replace: bool = False
    ) -> None:
        """Download WISKI data and load into the warehouse.

        See :func:`download_wiski_data` for details.
        """
        download_wiski_data(
            self.con, self.data_dir, station_ids, start_year, end_year, replace
        )
    
    def compute_baseflow(self, method='Boughton', min_size=30) -> None:
        """Compute baseflow from hourly Q data and store in derived.

        See :func:`compute_baseflow` for details.
        """
        compute_baseflow(self.con, self.data_dir, method, min_size)

    def set_active_quality_codes(self, data_codes: Optional[List[int]] = None, reset: bool = False) -> None:
        """Set the WISKI quality-code filtering options for analytics views.

        See :func:`set_active_quality_codes` for details.
        """
        warehouse.set_active_quality_codes(self.con, data_codes, reset)
    
    def set_active_sample_methods(self, sample_methods: Optional[List[str]] = None, reset: bool = False) -> None:
        """Set the EQuIS sample-method filtering options for analytics views.

        See :func:`set_active_sample_methods` for details.
        """
        warehouse.set_active_sample_methods(self.con, sample_methods, reset)

    def download_equis_data(
        self,
        station_ids: List[str],
        oracle_username: Optional[str] | None = None,
        oracle_password: Optional[str] | None = None,
        replace: bool = False
    ) -> None:
        """Download EQuIS data and load into the warehouse.

        See :func:`download_equis_data` for details.
        """
        download_equis_data(self.con, self.data_dir, station_ids, oracle_username, oracle_password, replace)
    
    def get_outlets(self, model_name: str) -> pd.DataFrame:
        """Get outlet station-reach pairs for a model.

        See :func:`get_outlets` for details.
        """
        return get_outlets(self.con, model_name)
    
    def get_station_ids(self, station_origin: Optional[str] = None) -> List[str]:
        """Get station IDs, optionally filtered by origin.

        See :func:`get_station_ids` for details.
        """
        return get_station_ids(self.con, station_origin)
    
    def get_observation_data(
        self,
        station_ids: List[str],
        constituent: str,
        agg_period: Optional[str] = None
    ) -> pd.DataFrame:
        """Get observation data for given stations and constituent.

        See :func:`get_observation_data` for details.
        """
        return get_observation_data(self.con, station_ids, constituent, agg_period)
    
    def get_outlet_data(
        self,
        outlet_id: int,
        constituent: str,
        agg_period: str = 'D'
    ) -> pd.DataFrame:
        """Get outlet observations with paired flow and baseflow.

        See :func:`get_outlet_data` for details.
        """
        return get_outlet_data(self.con, outlet_id, constituent, agg_period)
    
    def get_station_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get all analytics observations for a station.

        See :func:`get_station_data` for details.
        """
        return get_station_data(self.con, station_id, station_origin)
    
    def get_raw_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get raw staging data for a station.

        See :func:`get_raw_data` for details.
        """
        return get_raw_data(self.con, station_id, station_origin)
    
    def get_constituent_summary(self) -> pd.DataFrame:
        """Get constituent summary across all stations.

        See :func:`get_constituent_summary` for details.
        """
        return get_constituent_summary(self.con)
    
    def export_station_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path]
    ) -> None:
        """Export analytics data for a station to CSV.

        See :func:`export_station_to_csv` for details.
        """
        export_station_to_csv(self.con, station_id, station_origin, output_path)
    
    def export_raw_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path]
    ) -> None:
        """Export raw staging data for a station to CSV.

        See :func:`export_raw_to_csv` for details.
        """
        export_raw_to_csv(self.con, station_id, station_origin, output_path)
    
    def get_equis_template(self) -> pd.DataFrame:
        """Get an empty DataFrame matching the ``staging.equis`` schema.

        See :func:`get_equis_template` for details.
        """
        return get_equis_template(self.con)
    
    def get_wiski_template(self) -> pd.DataFrame:
        """Get an empty DataFrame matching the ``staging.wiski`` schema.

        See :func:`get_wiski_template` for details.
        """
        return get_wiski_template(self.con)
