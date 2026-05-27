"""
database
=========

DuckDB data database for storing, transforming, and querying hydrological
observation data used in HSPF model calibration.

Overview
--------
The warehouse module manages a local DuckDB database that consolidates
data from multiple sources (WISKI, EQuIS) into a unified, query-ready
analytical store.  It handles database lifecycle operations — schema
creation, table creation, data loading, view management — so that
upstream ETL code (:mod:`wiski`, :mod:`equis`) and downstream query code
(:class:`~mpcaHydro.warehouse_functions.DataManagerWrapper`) do not need
to know the underlying SQL.

Database schema
---------------
The database is organised into **five schemas**, each serving a distinct
role in the data pipeline:

**staging**
    Raw data as received from external systems, with minimal
    transformation.  Tables mirror the source column layout.

    ``staging.equis``
        Raw EQuIS result rows (70+ columns) exactly as returned by the
        Oracle query in :func:`equis.download`.  Key columns include
        ``SYS_LOC_CODE``, ``CAS_RN``, ``RESULT_NUMERIC``,
        ``RESULT_UNIT``, ``SAMPLE_DATE_TIME``, and
        ``SAMPLE_DATE_TIMEZONE``.

    ``staging.wiski``
        Raw WISKI time-series rows including ``Timestamp``, ``Value``,
        ``Quality Code``, ``station_no``, ``parametertype_id``,
        ``stationparameter_no``, ``ts_unitsymbol``, and the
        ``wplmn_flag``.

**analytics**
    Cleaned, standardised, and aggregated data ready for analysis.
    All tables share a common schema:
    ``(datetime, value, station_id, station_origin, constituent, unit)``.

    ``analytics.equis``
        Transformed EQuIS data (hourly averages, standard units, mapped
        constituents) produced by :func:`equis.transform`.

    ``analytics.wiski``
        Transformed WISKI data (quality-filtered, hourly averages,
        standard units, baseflow) produced by :func:`wiski.transform`.

    ``analytics.observations`` *(view)*
        Union of ``analytics.equis`` and ``analytics.wiski``, providing
        a single virtual table of all observations regardless of source.

    ``analytics.outlet_observations`` *(view)*
        Observations aggregated by outlet, joining observations to
        ``outlets.outlet_stations`` so that data is grouped by outlet
        rather than by individual station.

    ``analytics.outlet_observations_with_flow`` *(view)*
        Extends ``outlet_observations`` by left-joining discharge (``Q``)
        and baseflow (``QB``) data alongside each constituent
        observation.  This is the primary view used for HSPF calibration
        comparisons.

**outlets**
    Defines the mapping between monitoring stations and model reaches.
    See :mod:`outlets` for full documentation.

    ``outlets.outlet_groups``
        One row per outlet: ``(outlet_id, repository_name, outlet_name,
        notes)``.

    ``outlets.outlet_stations``
        Station membership: ``(outlet_id, station_id, station_origin,
        repository_name, true_opnid, comments)``.  The
        ``(station_id, station_origin)`` pair is unique across all
        outlets.

    ``outlets.outlet_reaches``
        Reach membership: ``(outlet_id, reach_id, repository_name)``.
        A reach may appear in multiple outlets.

    ``outlets.station_reach_pairs`` *(view)*
        Convenience view that joins ``outlet_stations`` to
        ``outlet_reaches`` on ``outlet_id``, deriving the implicit
        many-to-many station ↔ reach relationship.

**mappings**
    Lookup tables used during ETL transformations.

    ``mappings.wiski_parametertype``
        Maps WISKI ``parametertype_id`` to constituent abbreviations.

    ``mappings.equis_casrn``
        Maps EQuIS CAS registry numbers to constituent abbreviations.

    ``mappings.station_xref``
        Cross-reference between WISKI and EQuIS station identifiers
        (loaded from ``data/WISKI_EQUIS_XREF.csv``).

    ``mappings.wiski_quality_codes``
        WISKI quality-code definitions (loaded from
        ``data/WISKI_QUALITY_CODES.csv``).

**reports**
    Pre-built summary views for quick reporting.

    ``reports.wiski_qc_count`` *(view)*
        Quality-code frequency counts per WISKI station and parameter,
        joined to quality-code descriptions.

    ``reports.constituent_summary`` *(view)*
        Per-station, per-constituent summary statistics (count, mean,
        min, max, date range) across all analytics observations.

    ``reports.outlet_constituent_summary`` *(view)*
        Same statistics aggregated by outlet instead of station.

Typical workflow
----------------
::

    from mpcaHydro import warehouse

    # 1. Create a fresh database
    warehouse.init_db('observations.duckdb', reset=True)

    # 2. Open a connection and load data
    with warehouse.connect('observations.duckdb') as con:
        warehouse.add_df_to_table(con, df_wiski, 'staging', 'wiski')
        warehouse.add_df_to_table(con, df_equis, 'staging', 'equis')
        warehouse.update_views(con)

SQL files
---------
All DDL statements live in the ``sql/`` folder and are loaded at runtime
by :mod:`sql_loader`:

* ``schemas.sql`` — ``CREATE SCHEMA`` for all five schemas.
* ``staging_tables.sql`` — ``CREATE TABLE`` for staging tables.
* ``analytics_tables.sql`` — ``CREATE TABLE`` for analytics tables.
* ``outlets_schema.sql`` — outlets schema, tables, and the
  ``station_reach_pairs`` view.
* ``derived_tables.sql`` — derived tables (baseflow).
* ``views_analytics.sql`` — analytics views (observations,
  outlet_observations, outlet_observations_with_flow).
* ``views_reports.sql`` — report views.
* ``views_outlets.sql`` — outlets views.
"""

from typing import List

import duckdb
import pandas as pd
from pathlib import Path
from mpcaHydro.warehouse import outlets
from mpcaHydro.warehouse import sql_loader


def create_session(data_dir: str | Path = 'data') -> duckdb.DuckDBPyConnection:
    
    data_dir = Path(data_dir)


    # Create in-memory DuckDB connection and build schemas
    con = duckdb.connect()
    con.execute(sql_loader.get_schemas_sql())
    
    # Create outlets tables and views first
    con.execute(sql_loader.get_outlets_schema_sql())
    con.execute(sql_loader.get_views_outlets_sql())
    outlets.build_outlets(con, model_name=None)

    # Create mapping tables (e.g. WISKI parametertype_id → constituent)
    create_mapping_tables(con)


    # Create empty staging tables first — guarantees the names exist
    con.execute(sql_loader.get_staging_tables_sql())
    con.execute(sql_loader.get_derived_tables_sql())
    _refresh_staging_views(con, data_dir)
    _refresh_derived_views(con, data_dir)

    # These all resolve — either against parquet views or empty tables
    update_views(con)


    return con

def _refresh_staging_views(con: duckdb.DuckDBPyConnection, data_dir: str):
    
    equis_path = Path(f"{data_dir}/staging/equis")
    wiski_path = Path(f"{data_dir}/staging/wiski")

    # If parquet files exist, replace the empty tables with views over them
    if list(equis_path.rglob("*.parquet")):
        con.execute(f"""
            CREATE OR REPLACE VIEW staging.equis AS 
            SELECT * FROM read_parquet('{data_dir}/staging/equis/*.parquet', union_by_name=true);
        """)
    if list(wiski_path.rglob("*.parquet")):
        con.execute(f"""    
            CREATE OR REPLACE VIEW staging.wiski AS 
            SELECT * FROM read_parquet('{data_dir}/staging/wiski/*.parquet', union_by_name=true);
        """)

def _refresh_derived_views(con: duckdb.DuckDBPyConnection, data_dir: str):
    
    baseflow_path = Path(f"{data_dir}/derived/baseflow")

    # If parquet files exist, replace the empty tables with views over them
    if list(baseflow_path.rglob("*.parquet")):
        con.execute(f"""
            CREATE OR REPLACE VIEW derived.baseflow AS 
            SELECT * FROM read_parquet('{data_dir}/derived/baseflow/*.parquet', union_by_name=true);
        """)

def _validate_schemas(con: duckdb.DuckDBPyConnection):
    """Validate that the database contains all expected schemas.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Raises
    ------
    ValueError
        If any of the expected schemas (staging, analytics, mappings,
        outlets, reports) are missing.
    """
    expected_schemas = {'staging', 'analytics', 'mappings', 'outlets', 'reports'}
    result = con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    existing_schemas = {row[0] for row in result}
    missing_schemas = expected_schemas - existing_schemas
    if missing_schemas:
        raise ValueError(f"Missing schemas: {missing_schemas}")

def _validate_tables(con: duckdb.DuckDBPyConnection, schema: str, expected_tables: set):
    """Validate that a schema contains the expected tables.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.
    schema : str
        Schema name to inspect.
    expected_tables : set of str
        Table names that must exist.

    Raises
    ------
    ValueError
        If any expected tables are missing.
    """
    result = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]).fetchall()
    existing_tables = {row[0] for row in result}
    missing_tables = expected_tables - existing_tables
    if missing_tables:
        raise ValueError(f"Missing tables in {schema} schema: {missing_tables}")


def create_mapping_tables(con: duckdb.DuckDBPyConnection):
    """Create and populate lookup tables in the ``mappings`` schema.

    Populates the following from Python dictionaries and CSV files:

    * ``mappings.wiski_parametertype`` — WISKI parameter-type ID →
      constituent mapping.
    * ``mappings.equis_casrn`` — CAS registry number → constituent
      mapping.
    * ``mappings.station_xref`` — WISKI/EQuIS station cross-reference
      (from ``data/WISKI_EQUIS_XREF.csv``).
    * ``mappings.wiski_quality_codes`` — quality-code descriptions
      (from ``data/WISKI_QUALITY_CODES.csv``).

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    #TODO: these mappings could be managed in a more robust way — e.g. stored as tables in the repo and edited via pull requests, rather than hardcoded in Python.  But this is good enough for now.
    # WISKI parametertype_id -> constituent
    wiski_parametertype_map = {
        '11522': 'TP', 
        '11531': 'TP', 
        '11532': 'TSS', 
        '11523': 'TSS',
        '11526': 'N', 
        '11519': 'N', 
        '11520': 'OP', 
        '11528': 'OP',
        '11530': 'TKN', 
        '11521': 'TKN', 
        '11500': 'Q', 
        '11504': 'WT',
        '11533': 'DO', 
        '11507': 'WL'
    }
    df_wiski_params = pd.DataFrame(wiski_parametertype_map.items(), columns=['parametertype_id', 'constituent'])
    con.execute("CREATE TABLE IF NOT EXISTS mappings.wiski_parametertype AS SELECT * FROM df_wiski_params")

    # EQuIS cas_rn -> constituent
    equis_casrn_map = {
        '479-61-8': 'CHLA', 
        'CHLA-CORR': 'CHLA', 
        'BOD': 'BOD', 
        'NO2NO3': 'N',
        '14797-55-8': 'NO3', 
        '14797-65-0': 'NO2', 
        '14265-44-2': 'OP',
        'N-KJEL': 'TKN', 
        'PHOSPHATE-P': 'TP', 
        '7723-14-0': 'TP',
        'SOLIDS-TSS': 'TSS', 
        'TEMP-W': 'WT', 
        '7664-41-7': 'NH3'
    }
    df_equis_cas = pd.DataFrame(equis_casrn_map.items(), columns=['cas_rn', 'constituent'])
    con.execute("CREATE TABLE IF NOT EXISTS mappings.equis_casrn AS SELECT * FROM df_equis_cas")
    # Load wiski_quality_codes from CSV
    create_wiski_quality_codes_table(con)
    create_equis_sample_method_table(con)


#TODO move these to sql code
def create_wiski_quality_codes_table(con: duckdb.DuckDBPyConnection):
    """Create the mappings.wiski_quality_codes table from the CSV file."""
    wiski_qc_csv_path = Path(__file__).parent.parent / 'data/WISKI_QUALITY_CODES.csv'
    if wiski_qc_csv_path.exists():
        con.execute(f"CREATE OR REPLACE TABLE mappings.wiski_quality_codes AS SELECT * FROM read_csv_auto('{wiski_qc_csv_path.as_posix()}')")
    else:
        print(f"Warning: WISKI_QUALITY_CODES.csv not found at {wiski_qc_csv_path}")

def create_equis_sample_method_table(con: duckdb.DuckDBPyConnection):
    """Create a table of EQuIS sample methods to filter on."""
    #TODO: This filter should be more robustly managed — e.g. stored as a table in the repo and edited via pull requests, rather than hardcoded in Python.  But this is good enough for now.
    sample_methods = ['G-EVT', 'G', 'FIELDMSROBS', 'LKSURF1M', 'LKSURF2M', 'LKSURFOTH','Unknown']
    df_sample_methods = pd.DataFrame(sample_methods, columns=['sample_method'])
    df_sample_methods['include'] = 1  # Add a column to indicate which methods to include
    # add a row for 'Unknown' sample methods, which we will exclude by default
    #df_sample_methods = pd.concat([df_sample_methods, pd.DataFrame([['Unknown', 0]], columns=['sample_method', 'include'])], ignore_index=True)
    con.execute("CREATE OR REPLACE TABLE mappings.equis_sample_methods AS SELECT * FROM df_sample_methods")

def set_active_sample_methods(con, sample_methods: list = None, reset: bool = False):
    """Update which EQuIS sample methods are active in the current session."""
    if reset:
        create_equis_sample_method_table(con)  # reload from Python list to reset to default
    elif sample_methods is None:
        con.execute("UPDATE mappings.equis_sample_methods SET include = 1")
    else:
        # check that provided sample_methods are valid
        result = con.execute("SELECT sample_method FROM mappings.equis_sample_methods").fetchall()
        valid_methods = {row[0] for row in result}
        assert (set(sample_methods) <= valid_methods), f"Invalid sample methods: {set(sample_methods) - valid_methods}"
        con.execute("UPDATE mappings.equis_sample_methods SET include = 0")
        placeholders = ', '.join(['?'] * len(sample_methods))
        con.execute(
            f"UPDATE mappings.equis_sample_methods SET include = 1 WHERE sample_method IN ({placeholders})",
            sample_methods
        )

def set_active_quality_codes(con, quality_codes: list = None, reset: bool = False):
    """Update which quality codes are active in the current session.
    
    Since analytics.wiski is a VIEW, the next query against it
    will automatically reflect the change. No reprocessing needed.
    """
    if reset:
        create_wiski_quality_codes_table(con)  # reload from CSV to reset to default
    elif quality_codes is None:
        con.execute("UPDATE mappings.wiski_quality_codes SET active = 1")
    else:
        # check that provided quality_codes are valid
        result = con.execute("SELECT quality_code FROM mappings.wiski_quality_codes").fetchall()
        valid_codes = {row[0] for row in result}
        assert (set(quality_codes) <= valid_codes), f"Invalid quality codes: {set(quality_codes) - valid_codes}"
        con.execute("UPDATE mappings.wiski_quality_codes SET active = 0")
        placeholders = ', '.join(['?'] * len(quality_codes))
        con.execute(
            f"UPDATE mappings.wiski_quality_codes SET active = 1 WHERE quality_code IN ({placeholders})",
            quality_codes
        )

def update_views(con: duckdb.DuckDBPyConnection):
    """Refresh all analytics and reports views from their SQL definitions.

    Call this after loading or modifying data in the staging or analytics
    tables to ensure that dependent views reflect the latest data.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Writable DuckDB connection.
    """
    con.execute(sql_loader.get_transforms_wiski_sql())
    con.execute(sql_loader.get_transforms_equis_sql())
    #con.execute(sql_loader.get_transforms_baseflow_sql())
    con.execute(sql_loader.get_views_analytics_sql())
    con.execute(sql_loader.get_views_reports_sql())
