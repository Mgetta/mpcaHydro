# -*- coding: utf-8 -*-
"""
outlets
=======

Manage the mapping between monitoring stations, model reaches, and
outlet groups for HSPF watershed models.

Overview
--------
An **outlet** is a logical grouping that links one or more observation
stations (from WISKI or EQuIS) to one or more HSPF model reaches
(``opnids``).  This many-to-many relationship is central to calibration:
a single outlet may combine data from a WISKI continuous sensor and an
EQuIS grab-sample station at the same physical location, and may map to
multiple upstream reaches in the model network.

This module provides:

* **In-memory station registries** loaded from bundled GeoPackage files
  (``stations_wiski.gpkg`` and ``stations_EQUIS.gpkg``).  These are
  combined into the :data:`MODL_DB` DataFrame which stores every
  station's outlet assignment, model repository name, and reach IDs.
* **A DuckDB-backed outlet database** that persists the same information
  in normalised relational tables (``outlet_groups``,
  ``outlet_stations``, ``outlet_reaches``) plus a convenience view
  (``station_reach_pairs``).
* **Pure-function accessors** for querying stations, reaches, and outlets
  by model name, station ID, or outlet ID — suitable for both scripting
  and integration with the data warehouse.
* The :class:`OutletGateway` class, which provides an object-oriented
  façade for a single model's outlet configuration.

Key concepts
------------
``station_id``
    Unique identifier for a monitoring station (WISKI station number or
    EQuIS ``SYS_LOC_CODE``).

``station_origin`` / ``source``
    Either ``'wiski'`` or ``'equis'``, indicating which data system
    provides observations for that station.

``opnid`` / ``reach_id``
    HSPF model reach identifier (operation ID).

``outlet_id``
    Integer key that groups stations and reaches into a single outlet.

``repo_name`` / ``repository_name``
    Name of the HSPF model repository (e.g. ``'Clearwater'``).

``wplmn_flag``
    ``1`` if the station belongs to the Watershed Pollutant Load
    Monitoring Network, ``0`` otherwise.
"""
#import sqlite3
from pathlib import Path
import pandas as pd
import duckdb
from mpcaHydro.warehouse import sql_loader
from mpcaHydro.warehouse.sql_loader import get_outlets_schema_sql

_DEFAULT_CSV_PATH = Path(__file__).parent / 'data' / 'modl_db.csv'


def _load_stations():
    import geopandas as gpd
    _stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
    stations_wiski = _stations_wiski.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name','wplmn_flag']]
    stations_wiski['source'] = 'wiski'
    _stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
    stations_equis = _stations_equis.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name']]
    stations_equis['source'] = 'equis'
    stations_equis['wplmn_flag'] = 0

    stations = pd.concat([stations_wiski, stations_equis], ignore_index=True)
    return stations


def _write_modl_db(filepath = None, model_name = None):
    """Write outlet station and reach data to CSV files for inspection."""
    if filepath is None:
        filepath = _DEFAULT_CSV_PATH
    stations = _load_stations()
    if model_name is not None:
        stations = stations[stations['repo_name'] == model_name]
    stations.to_csv(filepath, index=False)

def _parse_opnids(s: str) -> tuple[int, ...]:
    """Parse 'opnids' cell into a sorted tuple of ints. Sorting makes
    outlet identity insensitive to comma order in the source CSV."""
    return tuple(sorted(int(float(x.strip())) for x in str(s).split(',') if x.strip()))


def _derive_tables(modl_db: pd.DataFrame):
    """One station-row CSV → three normalised tables.

    Returns (df_groups, df_stations, df_reaches), ready for INSERT.
    """
    df = modl_db.copy()
    df['opnids'] = df['opnids'].astype(str).str.strip().replace({'': pd.NA, 'nan': pd.NA})
    df = df.dropna(subset=['opnids', 'repo_name'])
    df = df.drop_duplicates(['station_id', 'source']).reset_index(drop=True)

    # Parse the comma-separated opnids once.
    df['reach_tuple'] = df['opnids'].apply(_parse_opnids)

    # Outlet identity = (sorted reach tuple, repo_name).  Stable, derived,
    # deterministic — same CSV always produces same outlet_ids.
    outlet_keys = (
        df[['reach_tuple', 'repo_name']]
        .drop_duplicates()
        .sort_values(['repo_name', 'reach_tuple'])
        .reset_index(drop=True)
    )
    outlet_keys['outlet_id'] = outlet_keys.index
    df = df.merge(outlet_keys, on=['reach_tuple', 'repo_name'])

    df_groups = (
        outlet_keys[['outlet_id', 'repo_name']]
        .rename(columns={'repo_name': 'repository_name'})
        .assign(outlet_name=None, notes=None)
    )

    df_stations = (
        df[['outlet_id', 'station_id', 'source', 'repo_name',
            'true_opnid', 'wplmn_flag', 'comments']]
        .rename(columns={'source': 'station_origin', 'repo_name': 'repository_name'})
    )

    df_reaches = (
        outlet_keys
        .assign(reach_id=outlet_keys['reach_tuple'])
        .explode('reach_id')
        .assign(reach_id=lambda d: d['reach_id'].astype(int))
        [['outlet_id', 'reach_id', 'repo_name']]
        .rename(columns={'repo_name': 'repository_name'})
    )

    return df_groups, df_stations, df_reaches


def build_outlets(con : duckdb.DuckDBPyConnection, csv_path: Path | None = None):
    """Build the outlets schema from the source CSV.

    Called once per session by warehouse.database.create_session.
    """
    if csv_path is None:
        csv_path = _DEFAULT_CSV_PATH

    con.execute(sql_loader.get_outlets_schema_sql())

    modl_db = pd.read_csv(csv_path)
    df_groups, df_stations, df_reaches = _derive_tables(modl_db)

    #TODO: Add modl_db table to schema definition and load it as well, for easier debugging and maintenance.
    con.execute("CREATE OR REPLACE TABLE outlets.modl_db AS SELECT * FROM modl_db")
    con.execute("INSERT INTO outlets.outlet_groups SELECT * FROM df_groups")
    con.execute("INSERT INTO outlets.outlet_stations SELECT * FROM df_stations")
    con.execute("INSERT INTO outlets.outlet_reaches SELECT * FROM df_reaches")
    #%%

def get_model_db(con, model_name: str):
    """Return the subset of stations data for a specific model repository.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name (e.g. ``'Clearwater'``).

    Returns
    -------
    pandas.DataFrame
        Rows from outlets.modl_db matching *model_name*.
    """
    return con.execute(
        "SELECT * FROM outlets.modl_db WHERE repo_name = ?", 
        (model_name,)
    ).df()

def valid_models(con):
    """Return a list of all unique model repository names.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.

    Returns
    -------
    list of str
    """
    df = con.execute("SELECT DISTINCT repo_name FROM outlets.modl_db WHERE repo_name IS NOT NULL").df()
    return df['repo_name'].tolist()

def equis_stations(con, model_name: str):
    """Return EQuIS station IDs for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    df = con.execute(
        "SELECT station_id FROM outlets.modl_db WHERE source = 'equis' AND repo_name = ?", 
        (model_name,)
    ).df()
    return df['station_id'].tolist()

def wiski_stations(con, model_name: str):
    """Return WISKI station IDs for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    df = con.execute(
        "SELECT station_id FROM outlets.modl_db WHERE source = 'wiski' AND repo_name = ?", 
        (model_name,)
    ).df()
    return df['station_id'].tolist()

def wplmn_stations(con, model_name: str):
    """Return WISKI station IDs flagged as WPLMN for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    df = con.execute(
        "SELECT DISTINCT station_id FROM outlets.modl_db WHERE source = 'wiski' AND repo_name = ? AND wplmn_flag = 1", 
        (model_name,)
    ).df()
    return df['station_id'].tolist()    

def wplmn_station_opnids(con, model_name: str):
    """Return reach IDs associated with WPLMN stations for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    df = con.execute("""
        SELECT DISTINCT opnids 
        FROM outlets.stations 
        WHERE repo_name = ? AND wplmn_flag = 1 AND source = 'wiski' AND opnids IS NOT NULL
    """, (model_name,)).df()
    return df['opnids'].tolist()

def wiski_station_opnids(con, model_name: str):
    """Return reach IDs for all WISKI stations in a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    df = con.execute("""
        SELECT DISTINCT opnids 
        FROM outlets.stations 
        WHERE repo_name = ? AND source = 'wiski' AND opnids IS NOT NULL
    """, (model_name,)).df()
    return df['opnids'].tolist()

def equis_station_opnids(con, model_name: str):
    """Return reach IDs for all EQuIS stations in a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    df = con.execute("""
        SELECT DISTINCT opnids 
        FROM outlets.stations 
        WHERE repo_name = ? AND source = 'equis' AND opnids IS NOT NULL
    """, (model_name,)).df()
    return df['opnids'].tolist()

def mapped_station_opnids(con, station_id: str, station_origin: str):
    """Return reach IDs mapped to a specific station.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    list of int
    """
    df = con.execute("""
        SELECT DISTINCT opnids 
        FROM outlets.stations 
        WHERE station_id = ? AND source = ? AND opnids IS NOT NULL
    """, (station_id, station_origin)).df()
    return df['opnids'].tolist()

def mapped_stations(con, model_name: str, station_origin: str):
    """Return station IDs for a model filtered by data origin.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    list of str

    Raises
    ------
    AssertionError
        If *station_origin* is not ``'wiski'`` or ``'equis'``.
    """
    assert station_origin in ['wiski', 'equis']
    df = con.execute("""
        SELECT DISTINCT station_id 
        FROM outlets.stations 
        WHERE repo_name = ? AND source = ? AND opnids IS NOT NULL
    """, (model_name, station_origin)).df()
    return df['station_id'].tolist()
    
def mapped_equis_stations(con, model_name: str):
    """Return EQuIS station IDs that have reach mappings for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    df = con.execute("""
        SELECT DISTINCT station_id 
        FROM outlets.stations 
        WHERE repo_name = ? AND source = 'equis' AND opnids IS NOT NULL
    """, (model_name,)).df()
    return df['station_id'].tolist()

def mapped_wiski_stations(con, model_name: str):
    """Return WISKI station IDs that have reach mappings for a model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    df = con.execute("""
        SELECT DISTINCT station_id 
        FROM outlets.stations 
        WHERE repo_name = ? AND source = 'wiski' AND opnids IS NOT NULL
    """, (model_name,)).df()
    return df['station_id'].tolist()

def outlets(con, model_name: str):
    """Return outlet groups as a list of DataFrames, one per outlet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of pandas.DataFrame
        Each element is the subset of stations for one outlet group.
    """
    df = con.execute("""
        SELECT * FROM outlets.stations 
        WHERE repo_name = ? AND opnids IS NOT NULL
    """, (model_name,)).df()
    
    return [group for _, group in df.groupby(by=['opnids', 'repo_name'])]

def outlet_stations(con, model_name: str):
    """Return station ID lists grouped by outlet.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Connection to the DuckDB database.
    model_name : str
        Repository name.

    Returns
    -------
    list of list of str
        Each inner list contains station IDs belonging to one outlet.
    """
    df = con.execute("""
        SELECT opnids, repo_name, station_id 
        FROM outlets.stations 
        WHERE repo_name = ? AND opnids IS NOT NULL
    """, (model_name,)).df()
    
    return [group['station_id'].to_list() for _, group in df.groupby(by=['opnids', 'repo_name'])]

