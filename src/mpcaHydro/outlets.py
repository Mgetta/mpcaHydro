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
import geopandas as gpd
import pandas as pd
import duckdb
from mpcaHydro.warehouse.sql_loader import get_outlets_schema_sql
#from hspf_tools.calibrator import etlWISKI, etlSWD




#stations_wiski = gpd.read_file('C:/Users/mfratki/Documents/GitHub/pyhcal/src/pyhcal/data/stations_wiski.gpkg')
def _construct_MODL_DB(stations_wiski, stations_equis):
    MODL_DB = pd.concat([stations_wiski,stations_equis])
    MODL_DB['opnids'] = MODL_DB['opnids'].str.strip().replace('',pd.NA)
    MODL_DB = MODL_DB.dropna(subset='opnids')
    MODL_DB = MODL_DB.dropna(subset = 'repo_name')
    MODL_DB = MODL_DB.drop_duplicates(['station_id','source']).reset_index(drop=True)
    # Add outlet_id column to MODL_DB based on enumerate grouping
    outlet_id_map = {}
    for outlet_id, (_, group) in enumerate(MODL_DB.drop_duplicates(['station_id','source']).groupby(by=['opnids','repo_name'])):
        for idx in group.index:
            outlet_id_map[idx] = int(outlet_id)
    MODL_DB['outlet_id'] = MODL_DB.index.map(outlet_id_map)
    return MODL_DB

def _load_stations():
    _stations_wiski = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_wiski.gpkg'))
    stations_wiski = _stations_wiski.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name','wplmn_flag']]
    stations_wiski['source'] = 'wiski'
    _stations_equis = gpd.read_file(str(Path(__file__).resolve().parent/'data\\stations_EQUIS.gpkg'))
    stations_equis = _stations_equis.loc[:,['station_id','true_opnid','opnids','comments','modeled','repo_name']]
    stations_equis['source'] = 'equis'
    stations_equis['wplmn_flag'] = 0
    return _stations_wiski, stations_wiski, _stations_equis, stations_equis

_stations_wiski, stations_wiski, _stations_equis, stations_equis = _load_stations()
MODL_DB = _construct_MODL_DB(stations_wiski, stations_equis)

#TODO terrible terrible approach, need to refactor
def _reload():
    """Reload the in-memory station registries from their GeoPackage sources.

    Refreshes the module-level globals ``_stations_wiski``,
    ``stations_wiski``, ``_stations_equis``, ``stations_equis``, and
    :data:`MODL_DB`.  Call this after modifying the bundled GeoPackage
    files to pick up changes without restarting the interpreter.
    """
    global _stations_wiski, stations_wiski, _stations_equis, stations_equis, MODL_DB
    _stations_wiski, stations_wiski, _stations_equis, stations_equis = _load_stations()
    MODL_DB = _construct_MODL_DB(stations_wiski, stations_equis)


def split_opnids(opnids: list):
    """Flatten and convert a nested list of reach-ID strings to integers.

    Parameters
    ----------
    opnids : list of list of str
        Nested list, typically from ``Series.str.split(',').to_list()``.

    Returns
    -------
    list of int
        Flat list of integer reach IDs.
    """
    return [int(float(j)) for i in opnids for j in i]

def get_model_db(model_name: str):
    """Return the subset of :data:`MODL_DB` for a specific model repository.

    Parameters
    ----------
    model_name : str
        Repository name (e.g. ``'Clearwater'``).

    Returns
    -------
    pandas.DataFrame
        Rows from :data:`MODL_DB` matching *model_name*.
    """
    return MODL_DB.query('repo_name == @model_name')

def valid_models():
    """Return a list of all unique model repository names in :data:`MODL_DB`.

    Returns
    -------
    list of str
    """
    return MODL_DB['repo_name'].unique().tolist()

def equis_stations(model_name):
    """Return EQuIS station IDs for a model (from the raw GeoPackage data).

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    return _stations_equis.query('repo_name == @model_name')['station_id'].tolist()

def wiski_stations(model_name):
    """Return WISKI station IDs for a model (from the raw GeoPackage data).

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    return _stations_wiski.query('repo_name == @model_name')['station_id'].tolist()

def wplmn_stations(model_name):
    """Return WISKI station IDs flagged as WPLMN for a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    return MODL_DB.query('repo_name == @model_name and wplmn_flag == 1 and source == "wiski"')['station_id'].tolist()

def wplmn_station_opnids(model_name):
    """Return reach IDs associated with WPLMN stations for a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and wplmn_flag == 1 and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def wiski_station_opnids(model_name):
    """Return reach IDs for all WISKI stations in a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "wiski"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def equis_station_opnids(model_name):
    """Return reach IDs for all EQuIS stations in a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of int
    """
    opnids = MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "equis"')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def mapped_station_opnids(station_id, station_origin):
    """Return reach IDs mapped to a specific station.

    Parameters
    ----------
    station_id : str
        Station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.

    Returns
    -------
    list of int
    """
    opnids = MODL_DB.dropna(subset=['opnids']).query('station_id == @station_id and source == @station_origin')['opnids'].str.split(',').to_list()
    return split_opnids(opnids)

def mapped_stations(model_name, station_origin):
    """Return station IDs for a model filtered by data origin.

    Parameters
    ----------
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
    assert(station_origin in ['wiski', 'equis'])
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == @station_origin')['station_id'].tolist()
    
def mapped_equis_stations(model_name):
    """Return EQuIS station IDs that have reach mappings for a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "equis"')['station_id'].tolist()

def mapped_wiski_stations(model_name):
    """Return WISKI station IDs that have reach mappings for a model.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of str
    """
    return MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name and source == "wiski"')['station_id'].tolist()

def outlets(model_name):
    """Return outlet groups as a list of DataFrames, one per outlet.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of pandas.DataFrame
        Each element is the subset of :data:`MODL_DB` for one outlet.
    """
    return [group for _, group in MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name').groupby(by = ['opnids','repo_name'])]

def outlet_stations(model_name):
    """Return station ID lists grouped by outlet.

    Parameters
    ----------
    model_name : str
        Repository name.

    Returns
    -------
    list of list of str
        Each inner list contains station IDs belonging to one outlet.
    """
    return [group['station_id'].to_list() for _, group in MODL_DB.dropna(subset=['opnids']).query('repo_name == @model_name').groupby(by = ['opnids','repo_name'])]


def connect(db_path, read_only=True):
    """Open a DuckDB connection to the outlet database.

    Parameters
    ----------
    db_path : str
        Path to the DuckDB file.
    read_only : bool, default True
        Open in read-only mode.

    Returns
    -------
    duckdb.DuckDBPyConnection
    """
    #Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path,read_only=read_only)


def init_db(db_path: str, reset: bool = False):
    """Initialise the DuckDB outlet database with schema and tables.

    Creates the ``outlets`` schema and its tables
    (``outlet_groups``, ``outlet_stations``, ``outlet_reaches``) plus the
    ``station_reach_pairs`` view.  If *reset* is ``True``, deletes the
    existing database file first.

    Parameters
    ----------
    db_path : str
        Filesystem path for the DuckDB file.
    reset : bool, default False
        Delete the existing file before creating a fresh database.
    """
    db_path = Path(db_path)
    if reset and db_path.exists():
        db_path.unlink()
    with connect(db_path.as_posix(),False) as con:
        con.execute(get_outlets_schema_sql())



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

class OutletGateway:
    """Object-oriented gateway for querying outlet data for a single model.

    Wraps the module-level accessor functions so that callers do not need
    to pass ``model_name`` on every call.

    Parameters
    ----------
    model_name : str
        Repository name for the HSPF model.

    Attributes
    ----------
    model_name : str
        The model repository name this gateway is bound to.
    db_path : str
        Path to the bundled DuckDB outlet database.
    modl_db : pandas.DataFrame
        In-memory station data for *model_name*.

    Examples
    --------
    >>> gw = OutletGateway('Clearwater')
    >>> gw.wiski_stations()
    ['E66050001']
    """

    def __init__(self, model_name: str):
        """Initialise the gateway for a specific model."""
        self.model_name = model_name
        self.db_path = DB_PATH
        self.modl_db = get_model_db(model_name)

    # Legacy methods to access functions
    def wplmn_station_opnids(self):
        """Return reach IDs for WPLMN stations in this model."""
        return wplmn_station_opnids(self.model_name)

    def wiski_station_opnids(self):
        """Return reach IDs for all WISKI stations in this model."""
        return wiski_station_opnids(self.model_name)

    def equis_station_opnids(self):
        """Return reach IDs for all EQuIS stations in this model."""
        return equis_station_opnids(self.model_name)

    def station_opnids(self):
        """Return reach IDs for all mapped stations in this model."""
        return mapped_station_opnids(self.model_name)

    def equis_stations(self):
        """Return EQuIS station IDs for this model."""
        return equis_stations(self.model_name)

    def wiski_stations(self):
        """Return WISKI station IDs for this model."""
        return wiski_stations(self.model_name)

    def wplmn_stations(self):
        """Return WPLMN station IDs for this model."""
        return wplmn_stations(self.model_name)

    def outlets(self):
        """Return outlet groups as a list of DataFrames."""
        return outlets(self.model_name)

    def outlet_stations(self):
        """Return station ID lists grouped by outlet."""
        return outlet_stations(self.model_name)

    # Accessors for outlets
    def get_outlets(self):
        """Query the DuckDB outlet database for this model's station-reach pairs."""
        with connect(self.db_path) as con:
            return get_outlets_by_model(con, self.model_name)

    def get_outlets_by_reach(self, reach_id: int):
        """Return outlet rows containing *reach_id* in this model."""
        with connect(self.db_path) as con:
            return get_outlets_by_reach(con, reach_id, self.model_name)

    def get_outlets_by_station(self, station_id: str, station_origin: str):
        """Return outlet rows for *station_id* (must belong to this model).

        Raises
        ------
        AssertionError
            If *station_id* is not found in this model's station lists.
        """
        assert(station_id in self.wiski_stations() + self.equis_stations()), f"Station ID {station_id} not found in model {self.model_name}"
        with connect(self.db_path) as con:
            return get_outlets_by_station(con, station_id, station_origin)

    def get_outlet_opnids(self, outlet_id: int):
        """Return unique reach IDs for the given outlet."""
        with connect(self.db_path) as con:
            return get_outlet_opnids(con, outlet_id)
    
    def get_outlet_stations(self, outlet_id: int):
        """Return station IDs and origins for the given outlet."""
        with connect(self.db_path) as con:
            return get_outlet_stations(con, outlet_id)
    
# constructors:
def build_outlet_db(db_path: str = None):
    """Build (or rebuild) the complete outlet DuckDB database.

    Initialises the database schema, then populates all outlet groups,
    stations, and reaches from :data:`MODL_DB`.

    Parameters
    ----------
    db_path : str, optional
        Path for the DuckDB file.  Defaults to :data:`DB_PATH`.
    """
    if db_path is None:
        db_path = DB_PATH
    init_db(db_path,reset=True)
    with connect(db_path,False) as con:
        build_outlets(con)


def build_outlets(con, model_name: str = None):
    """Populate outlet tables from MODL_DB — bulk insert, no loops."""
    if model_name is not None:
        modl_db = get_model_db(model_name)
    else:
        modl_db = MODL_DB

    # ── outlet_groups: one row per unique outlet ──
    df_groups = (
        modl_db[['outlet_id', 'repo_name']]
        .drop_duplicates('outlet_id')
        .rename(columns={'repo_name': 'repository_name'})
        .assign(outlet_name=None, notes=None)
    )
    con.execute("""
        INSERT INTO outlets.outlet_groups (outlet_id, repository_name, outlet_name, notes)
        SELECT outlet_id, repository_name, outlet_name, notes FROM df_groups
    """)

    # ── outlet_stations: one row per unique station+source ──
    df_stations = (
        modl_db
        .drop_duplicates(subset=['station_id', 'source'])
        [['outlet_id', 'station_id', 'source', 'true_opnid', 'repo_name', 'comments']]
        .rename(columns={'source': 'station_origin', 'repo_name': 'repository_name'})
    )
    con.execute("""
        INSERT INTO outlets.outlet_stations 
            (outlet_id, station_id, station_origin, true_opnid, repository_name, comments)
        SELECT outlet_id, station_id, station_origin, true_opnid, repository_name, comments
        FROM df_stations
    """)

    # ── outlet_reaches: explode comma-separated opnids, one row per reach ──
    df_reaches = (
        modl_db[['outlet_id', 'opnids', 'repo_name']]
        .dropna(subset=['opnids'])
        .assign(reach_id=lambda d: d['opnids'].str.split(','))
        .explode('reach_id')
        .assign(reach_id=lambda d: d['reach_id'].str.strip().astype(float).astype(int))
        .drop_duplicates(subset=['outlet_id', 'reach_id'])
        [['outlet_id', 'reach_id', 'repo_name']]
        .rename(columns={'repo_name': 'repository_name'})
    )
    con.execute("""
        INSERT INTO outlets.outlet_reaches (outlet_id, reach_id, repository_name)
        SELECT outlet_id, reach_id, repository_name FROM df_reaches
    """)


def build_outlets_legacy(con, model_name: str = None):
    """Populate the outlet tables from :data:`MODL_DB`.

    For each outlet group, inserts rows into ``outlet_groups``,
    ``outlet_reaches``, and ``outlet_stations``.  If *model_name* is
    provided, only that model's outlets are created.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    model_name : str, optional
        Restrict to a single model.  If ``None``, all models are
        populated.
    """
    if model_name is not None:
        modl_db = get_model_db(model_name)
    else:
        modl_db = MODL_DB

    for outlet_id in modl_db['outlet_id'].unique():
        group = modl_db.query('outlet_id == @outlet_id')
        repo_name = group['repo_name'].iloc[0]
        add_outlet(con, outlet_id = int(outlet_id), outlet_name = None, repository_name = repo_name, notes = None)
        opnids = set(split_opnids(group['opnids'].str.split(',').to_list()))
        for opnid in opnids:
            add_reach(con, outlet_id = int(outlet_id), reach_id = int(opnid), repository_name = repo_name)
        for _, row in group.drop_duplicates(subset=['station_id', 'source']).iterrows():
            add_station(con, outlet_id = int(outlet_id), station_id = row['station_id'], station_origin = row['source'], true_opnid = row['true_opnid'], repository_name= repo_name, comments = row['comments'])


def add_outlet(con,
               outlet_id: int,
               repository_name: str,
               outlet_name=None,
               notes=None):
    """Insert a new outlet group into the database.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    outlet_id : int
        Unique outlet identifier.
    repository_name : str
        Model repository name.
    outlet_name : str, optional
        Human-readable outlet label.
    notes : str, optional
        Free-text notes.
    """
    con.execute(
        "INSERT INTO outlets.outlet_groups (outlet_id, repository_name, outlet_name, notes) VALUES (?, ?, ?, ?)",
        [outlet_id, repository_name, outlet_name, notes]
    )

def add_station(con,
                outlet_id: int,
                station_id: int,
                station_origin: str,
                true_opnid: int,
                repository_name: str,
                comments=None):
    """Insert a station membership record for an outlet.

    The ``(station_id, station_origin)`` pair must be unique across all
    outlets (enforced by a database constraint).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    outlet_id : int
        Parent outlet group identifier.
    station_id : int
        Monitoring station identifier.
    station_origin : str
        ``'wiski'`` or ``'equis'``.
    true_opnid : int
        The primary reach ID that best represents this station's
        location in the model network.
    repository_name : str
        Model repository name.
    comments : str, optional
        Free-text comments.
    """
    con.execute(
        """INSERT INTO outlets.outlet_stations
           (outlet_id, station_id, station_origin, true_opnid, repository_name, comments)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [outlet_id, station_id, station_origin, true_opnid, repository_name, comments]
    )

def add_reach(con,
              outlet_id: int,
              reach_id: int,
              repository_name: str):
    """Insert a reach membership record for an outlet.

    A reach may appear in multiple outlets, enabling many-to-many
    relationships between stations and reaches across the model.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    outlet_id : int
        Parent outlet group identifier.
    reach_id : int
        HSPF model reach identifier.
    repository_name : str
        Model repository name.
    """
    con.execute(
        """INSERT INTO outlets.outlet_reaches (outlet_id, reach_id, repository_name)
           VALUES (?, ?, ?)""",
        [outlet_id, reach_id, repository_name]
    )


    
#row = modl_db.MODL_DB.iloc[0]

#info = etlWISKI.info(row['station_id'])

#modl_db.MODL_DB.query('source == "equis"')

# outlet_dict = {'stations': {'wiski': ['E66050001'],
#                'equis': ['S002-118']},
#                'reaches': {'Clearwater': [650]}
                      



# station_ids = ['S002-118']
# #station_ids = ['E66050001']
# reach_ids = [650]
# flow_station_ids =  ['E66050001']
