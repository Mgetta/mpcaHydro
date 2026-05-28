"""
equis
=====

Interface for downloading and transforming discrete water-quality sample
data from MPCA's EQuIS (Environmental Quality Information System) Oracle
database.

Overview
--------
EQuIS is the laboratory information management system (LIMS) used by MPCA
to store grab-sample analytical results — parameters such as total
phosphorus, nitrogen species, chlorophyll-a, and temperature measured at
river/stream monitoring stations.  Unlike the continuous sensor data
stored in WISKI (see :mod:`wiski`), EQuIS records are discrete events
that typically occur on scheduled sampling visits.

This module provides functions to:

* **Connect** to the Oracle EQuIS database (``DELTAW`` instance).
* **Download** raw result rows for a list of station IDs, filtered to
  HSPF-relevant constituents and river/stream locations.
* **Normalize** the raw Oracle output — map CAS registry numbers to
  short constituent names, convert units, and standardise timestamps to a
  fixed UTC-6 offset.
* **Transform** normalised data through non-detect replacement, year
  filtering, and hourly averaging — producing analysis-ready DataFrames
  for loading into the data warehouse.

When to use this module
-----------------------
Use ``equis`` when you need laboratory grab-sample data for water-quality
constituents.  If you need continuous sensor time-series (flow, temperature),
use :mod:`wiski` instead.

Typical workflow::

    import mpcaHydro.equis as equis

    # 1. Connect
    conn = equis.connect('myuser', 'mypassword')

    # 2. Download raw data
    df_raw = equis.download(['S002-118', 'S004-880'], connection=conn)

    # 3. Produce analysis-ready data
    df = equis.transform(df_raw)

    # 4. Close
    equis.close_connection(conn)

Key data structures
-------------------
``CAS_RN_MAP``
    Maps CAS registry numbers (or EQuIS analyte codes) to the package's
    short constituent names (e.g. ``'7723-14-0'`` → ``'TP'``).
"""


from datetime import datetime, timezone, timedelta
from getpass import getpass
import os
import pandas as pd
from typing import Union, Optional
import oracledb

CONNECTION = None

CAS_RN_MAP = {'479-61-8':'CHLA',
            'CHLA-CORR':'CHLA',
            'BOD':'BOD',
            'NO2NO3':'N', #TODO change to 'NO2NO3'
            '14797-55-8': 'NO3',
            '14797-65-0':'NO2',
            '14265-44-2': 'OP',
            'N-KJEL' : 'TKN',
            'PHOSPHATE-P': 'TP',
            '7723-14-0' : 'TP',
            'SOLIDS-TSS': 'TSS',
            'TEMP-W' : 'WT',
            '7664-41-7' : 'NH3'}

_TZ_OFFSET_HOURS = {
    'CST': -6,
    'CDT': -5,
    'UTC':  0,
    'EST': -5,
    'EDT': -4,
}

_TARGET_TZ = 'Etc/GMT+6'  # UTC-6 (note: POSIX convention flips the sign)

def connect(
    user: str = None,
    password: str = None,
    host: str = "DELTAW",
    port: int = 1521,
    sid: str = None
):
    """Create and return an Oracle database connection to the EQuIS instance.

    The connection is also stored in the module-level :data:`CONNECTION`
    global so that subsequent calls to :func:`download`, :func:`info`, etc.
    can omit the *connection* parameter.
    
    Credentials can be passed directly or read from environment
    variables. Password lookup is host-specific and must follow this naming convention:

        ORACLE_USER          → username (shared across hosts)
        ORACLE_PASSWORD_DELTAT  → password for DELTAT
        ORACLE_PASSWORD_DELTAW  → password for DELTAW

    To set environment variables (no admin required)::

        setx ORACLE_USER myusername
        setx ORACLE_PASSWORD_DELTAT mypassword

    Then restart your terminal/Jupyter for them to take effect.
    Parameters

    Credential resolution order:
    1. Explicit parameters (``user=``, ``password=``)
    2. User environment variables
    3. Interactive prompt

    
    ----------
    user : str
        Oracle username.
    password : str
        Oracle password.
    host : str, default ``'DELTAW'``
        Oracle host name or IP address.
    port : int, default 1521
        Oracle listener port.
    sid : str, default ``'DELTAW'``
        Oracle System Identifier.

    Returns
    -------
    oracledb.Connection
        An open Oracle connection.
    """
    # 1. Try explicit args
    # 2. Try environment variables
    user = user or os.environ.get('ORACLE_USER')

    if password is None:
        host_key = host.upper().replace('-', '_')
        password = (
            os.environ.get(f'ORACLE_PASSWORD_{host_key}')
            or os.environ.get('ORACLE_PASSWORD')
        )

    # 3. Fall back to interactive prompt
    if not user:
        user = input("Oracle username: ")
    if not password:
        password = getpass(f"Oracle password for {user}@{host}: ")

    if sid is None:
        sid = host

    # Legacy 
    global CONNECTION
    CONNECTION = oracledb.connect(
        user=user, password=password,
        host=host, port=port, sid=sid
    )
    return CONNECTION


def close_connection(connection: Optional[oracledb.Connection] = None):
    """Close an Oracle database connection.

    If *connection* is provided it is closed directly; otherwise the
    module-level :data:`CONNECTION` global is closed and reset to ``None``.

    Parameters
    ----------
    connection : oracledb.Connection, optional
        Specific connection to close.  Falls back to the global
        :data:`CONNECTION` when omitted.
    """
    global CONNECTION
    if connection is not None:
        connection.close()
        if connection is CONNECTION:
            CONNECTION = None
    elif CONNECTION is not None:
        CONNECTION.close()
        CONNECTION = None



def test_connection():
    """Placeholder for testing the Oracle EQuIS connection.

    .. warning::

       This function is not yet implemented and will raise
       ``NotImplementedError``.

    Raises
    ------
    NotImplementedError
        Always raised.
    """
    raise NotImplementedError("This function is a placeholder for testing Oracle DB connection.")
    # try:
    #     # or for SID:
    #     # connection = oracledb.connect(user="your_username", 
    #     #                             password="your_password", 
    #     #                             host="your_host", 
    #     #                             port=1521, 
    #     #                             sid="your_sid")

    #     print("Successfully connected to Oracle Database")

    #     # Perform database operations here
    #     # ...
    #     if connection:
    #         connection.close()
    #         print("Connection closed")
    # except oracledb.Error as e:
    #     print(f"Error connecting to Oracle Database: {e}")



def make_placeholders(items):
    """Create Oracle bind-variable placeholders for an ``IN`` clause.

    Parameters
    ----------
    items : list
        Values to bind (e.g. station IDs).

    Returns
    -------
    tuple of (str, dict)
        A tuple ``(placeholders, binds)`` where *placeholders* is a
        comma-separated string like ``':id0, :id1'`` and *binds* is the
        corresponding ``{name: value}`` dictionary.
    """
    # Create placeholders like :id0, :id1, :id2
    placeholders = ', '.join(f':id{i}' for i in range(len(items)))
    # Create dictionary of bind values
    binds = {f'id{i}': val for i, val in enumerate(items)}
    return placeholders, binds

def to_dataframe(odb_cursor):
    """Convert an ``oracledb`` cursor's result set to a :class:`pandas.DataFrame`.

    Parameters
    ----------
    odb_cursor : oracledb.Cursor
        An executed cursor with results ready to fetch.

    Returns
    -------
    pandas.DataFrame
        All rows from the cursor, with column names taken from
        ``cursor.description``.
    """
    column_names = [description[0] for description in odb_cursor.description]
    rows = odb_cursor.fetchall()
    df = pd.DataFrame(rows,columns = column_names)
    return df

#%% Query for station locations with HSPF related constituents

def info(station_ids, connection: Optional[oracledb.Connection] = None):
    """Retrieve a de-duplicated summary of available constituents per station.

    Internally calls :func:`download` and :func:`normalize`, then drops
    duplicate ``(station_id, constituent)`` pairs.  Use this to inspect
    what data exists before committing to a full download-and-transform.

    Parameters
    ----------
    station_ids : list of str
        EQuIS ``SYS_LOC_CODE`` values.
    connection : oracledb.Connection, optional
        Oracle connection.  Falls back to the global :data:`CONNECTION`.

    Returns
    -------
    pandas.DataFrame
        One row per unique station / constituent combination.

    Raises
    ------
    ValueError
        If no connection is available.
    """
    
    conn = connection if connection is not None else CONNECTION
    if conn is None:
        raise ValueError("No connection provided and global CONNECTION is not set. Call connect() first or pass a connection.")
    
    df = download(station_ids, connection=conn).drop_duplicates(subset=['SYS_LOC_CODE','CAS_RN'])
    return df

    


def download(station_ids, connection: Optional[oracledb.Connection] = None):
    """Download raw EQuIS result data for the given station IDs.

    Executes a SQL query against ``mpca_dal.mv_eq_result`` (the EQuIS
    Data Access Layer materialised view) filtered to:

    * HSPF-relevant CAS registry numbers (nutrients, solids, temperature,
      dissolved oxygen, chlorophyll-a, BOD).
    * River/Stream location types only.
    * Approved (``Final``) and reportable results.
    * Standard grab-sample methods (``G-EVT``, ``G``, etc.).

    **Why these filters?**
    The EQuIS database contains millions of results across many location
    types and analytes.  Pre-filtering at the SQL level avoids
    transferring irrelevant data and ensures consistency with the HSPF
    model calibration workflow.

    Parameters
    ----------
    station_ids : list of str
        EQuIS ``SYS_LOC_CODE`` values.
    connection : oracledb.Connection, optional
        Oracle connection.  Falls back to the global :data:`CONNECTION`.

    Returns
    -------
    pandas.DataFrame
        Raw EQuIS result rows with station coordinates, sample method,
        and all columns from ``mv_eq_result``.

    Raises
    ------
    ValueError
        If no connection is available.
    """
    placeholders, binds = make_placeholders(station_ids)
    query = f"""
SELECT
    mpca_dal.eq_fac_station.latitude,
    mpca_dal.eq_fac_station.longitude,
    mpca_dal.eq_fac_station.wid_list,
    mpca_dal.eq_sample.sample_method,
    mpca_dal.eq_sample.sample_remark,
    mpca_dal.mv_eq_result.*
    FROM
    	mpca_dal.mv_eq_result
		LEFT JOIN mpca_dal.eq_fac_station 
		   ON mpca_dal.mv_eq_result.sys_loc_code = mpca_dal.eq_fac_station.sys_loc_code
		   AND mpca_dal.mv_eq_result.facility_id = mpca_dal.eq_fac_station.facility_id        
		LEFT JOIN mpca_dal.eq_sample ON mpca_dal.mv_eq_result.sample_id = mpca_dal.eq_sample.sample_id
    WHERE
        mpca_dal.mv_eq_result.cas_rn IN ('479-61-8',
                            'CHLA-CORR',
                            'BOD',
                            'NO2NO3',
                            '14797-55-8',
                            '14797-65-0',
                            '14265-44-2',
                            'N-KJEL',
                            'PHOSPHATE-P',
                            '7723-14-0',
                            'SOLIDS-TSS',
                            'TEMP-W',
                            '7664-41-7',
                            'FLOW')
        AND mpca_dal.eq_fac_station.loc_type = 'River/Stream'
        --AND mpca_dal.mv_eq_result.approval_code = 'Final'
        AND mpca_dal.mv_eq_result.reportable_result = 'Y'
        AND mpca_dal.mv_eq_result.facility_id IN ( 1, 33836701 )
        AND mpca_dal.mv_eq_result.sys_loc_code IN ({placeholders})
    """
    conn = connect()
    with conn.cursor() as cursor:
        cursor.execute(query,binds)
        df = to_dataframe(cursor)
        df['grain'] = 'discrete'
        df['statistic'] = 'INST'
        df['interval_minutes'] = pd.NA # not applicable for discrete samples
    close_connection(conn)
    return df
    


# def as_utc_offset(naive_dt: Union[datetime, str], tz_label: str, target_offset: timezone) -> datetime:
#     """Convert a naïve datetime to a fixed UTC offset using a timezone label.

#     EQuIS stores timestamps as naïve datetime values alongside a separate
#     timezone label column (``SAMPLE_DATE_TIMEZONE``).  This function
#     re-interprets the naïve value in the declared source timezone and then
#     converts it to the *target_offset* (typically UTC-6 for Central
#     Standard Time).

#     The conversion rule:

#     * ``'CST'`` → interpret as UTC-6
#     * ``'CDT'`` → interpret as UTC-5
#     * ``'UTC'`` → interpret as UTC

#     .. warning::

#        Uses ``datetime.replace(tzinfo=...)`` which **assumes** the input is
#        truly naïve.  If *naive_dt* already carries ``tzinfo`` the result
#        will be incorrect.

#     Parameters
#     ----------
#     naive_dt : datetime or str
#         Naïve timestamp (no timezone info) or an ISO-format string.
#     tz_label : str
#         Timezone abbreviation from EQuIS (``'CST'``, ``'CDT'``, or
#         ``'UTC'``).
#     target_offset : datetime.timezone
#         Fixed offset to convert into (e.g. ``timezone(timedelta(hours=-6))``).

#     Returns
#     -------
#     datetime
#         Timezone-aware datetime expressed in *target_offset*, then
#         localised (tz stripped).

#     Raises
#     ------
#     TypeError
#         If *naive_dt* is neither a ``datetime`` nor a ``str``.
#     ValueError
#         If *tz_label* is not one of ``CST``, ``CDT``, ``UTC``.
#     """
#     if isinstance(naive_dt, str):
#         naive = pd.to_datetime(naive_dt).to_pydatetime()
#     elif isinstance(naive_dt, datetime):
#         naive = naive_dt
#     else:
#         raise TypeError("naive_dt must be datetime or str")

#     label = (tz_label or "").strip().upper()

#     if label == "CST":
#         src_tz = timezone(timedelta(hours=-6))
#     elif label == "CDT":
#         src_tz = timezone(timedelta(hours=-5))
#     elif label == 'UTC':
#         src_tz = timezone.utc
#     else:
#         raise ValueError(f"Unexpected timezone label: {tz_label}")
#     # attach the source tz (interpret naive as local time in src_tz)
#     aware_src = naive.replace(tzinfo=src_tz)

#     # convert the instant to fixed UTC-6
#     return aware_src.astimezone(target_offset).tz_localize(None)



# def normalize_timezone(df):
#     """Convert naive datetimes + timezone labels to a single target timezone.

#     Unknown timezone labels produce NaT and are logged as warnings
#     so they can be investigated without crashing the pipeline.
#     """
#     dt = pd.to_datetime(df['SAMPLE_DATE_TIME'])

#     tz_label = df['SAMPLE_DATE_TIMEZONE'].str.strip().str.upper()
#     offset_hours = tz_label.map(_TZ_OFFSET_HOURS)

#     # # Log unknown labels — don't crash, just warn
#     unmapped = offset_hours.isna() & tz_label.notna()
#     if unmapped.any():
#         print(f"Warning: {unmapped.sum()} rows have unknown timezone labels: {tz_label[unmapped].unique().tolist()}. These will be set to NaT.")
#     #     bad_labels = tz_label[unmapped].unique().tolist()
#     #     n_bad = unmapped.sum()
#     #     logger.warning(
#     #         f"{n_bad} rows have unknown timezone labels: {bad_labels}. "
#     #         f"These will be set to NaT."
#     #     )

#     # Convert to UTC, then to target — unmapped rows stay NaT naturally
#     utc_dt = dt - pd.to_timedelta(offset_hours, unit='h')

#     df = df.copy()
#     df['datetime'] = (
#         utc_dt
#         .dt.tz_localize('UTC')
#         .dt.tz_convert(_TARGET_TZ)
#         .dt.tz_localize(None)
#     )

#     return df


# def normalize_columns(df):
#     """Select and rename relevant columns from raw EQuIS data.

#     Retains only the columns needed for downstream analysis and renames
#     them to the package-standard schema:

#     * ``SYS_LOC_CODE`` → ``station_id``
#     * ``RESULT_NUMERIC`` → ``value``
#     * ``RESULT_UNIT`` → ``unit``
#     * ``CAS_RN`` → ``cas_rn``

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         EQuIS data with constituent mapping already applied.

#     Returns
#     -------
#     pandas.DataFrame
#         Slimmed DataFrame with standardised column names.
#     """
#     return df[['SYS_LOC_CODE',
#                'constituent',
#                'CAS_RN',
#                'datetime',
#                'RESULT_NUMERIC',
#                'RESULT_UNIT',
#                ]].rename(columns={
#                    'SYS_LOC_CODE':'station_id',
#                    'RESULT_NUMERIC':'value',
#                    'RESULT_UNIT':'unit',
#                    'CAS_RN':'cas_rn'
#                })



# def normalize_timezone_legacy(df):
#     """Convert EQuIS sample timestamps to a fixed UTC-6 offset.

#     Iterates over every row, reading ``SAMPLE_DATE_TIME`` and
#     ``SAMPLE_DATE_TIMEZONE``, and applies :func:`as_utc_offset`.  Rows
#     whose timezone cannot be determined are set to ``NaT``.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Raw or partially-normalised EQuIS data.

#     Returns
#     -------
#     pandas.DataFrame
#         DataFrame with a new ``datetime`` column in UTC-6.
#     """
#     target_offset = timezone(timedelta(hours=-6))
#     def _conv(row):
#         try:
#             return as_utc_offset(row['SAMPLE_DATE_TIME'], row['SAMPLE_DATE_TIMEZONE'],target_offset)
#         except Exception:
#             return pd.NaT

#     df.loc[:,'datetime'] = df.apply(_conv, axis=1)
#     return df

# def convert_units(df):
#     """Convert EQuIS measurement units to package-standard units.

#     The following conversions are applied *in-place*:

#     * Micrograms per litre (``ug/l``) → milligrams per litre (``mg/l``)
#     * Milligrams per gram (``mg/g``) → milligrams per litre (``mg/l``)
#       (assumes density of 1 g/mL)
#     * Degrees Celsius (``deg c``, ``degc``) → Fahrenheit (``degf``)

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         DataFrame with ``unit`` and ``value`` columns.

#     Returns
#     -------
#     pandas.DataFrame
#         The same DataFrame with converted values and normalised unit
#         strings.
#     """
#     # Convert ug/L to mg/L
#     df['unit'] = df['unit'].str.lower()

#     mask_ugL = df['unit'] == 'ug/l'
#     df.loc[mask_ugL, 'value'] = df.loc[mask_ugL, 'value'] / 1000
#     df.loc[mask_ugL, 'unit'] = 'mg/l'

#     # Convert mg/g to mg/L (assuming density of 1 g/mL)
#     mask_mgg = df['unit'] == 'mg/g'
#     df.loc[mask_mgg, 'value'] = df.loc[mask_mgg, 'value'] * 1000
#     df.loc[mask_mgg, 'unit'] = 'mg/l'

#     # Convert deg C to degF
#     mask_degC = df['unit'].isin(['deg c', 'degc'])
#     df.loc[mask_degC, 'value'] = (df.loc[mask_degC, 'value'] * 9/5) + 32
#     df.loc[mask_degC, 'unit'] = 'degf'

#     return df

# def map_constituents(df):
#     """Map CAS registry numbers to standard constituent abbreviations.

#     Uses :data:`CAS_RN_MAP` to translate the ``CAS_RN`` column into a
#     new ``constituent`` column (e.g. ``'7723-14-0'`` → ``'TP'``).

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Raw EQuIS data containing a ``CAS_RN`` column.

#     Returns
#     -------
#     pandas.DataFrame
#         DataFrame with an added ``constituent`` column.
#     """
#     df['constituent'] = df['CAS_RN'].map(CAS_RN_MAP)
#     return df


# def average_results(df):
#     """Aggregate EQuIS samples to hourly means per station and constituent.

#     Timestamps are rounded to the nearest hour.  Results are grouped by
#     ``(station_id, datetime, constituent, unit, station_origin)`` and the
#     mean value is computed.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Normalised EQuIS observations.

#     Returns
#     -------
#     pandas.DataFrame
#         Hourly-averaged observations tagged with
#         ``station_origin='equis'``.
#     """
#     df['datetime'] = df['datetime'].dt.round('h')
#     df['station_origin'] = 'equis'
#     return df.groupby(['station_id', 'datetime', 'constituent', 'unit','station_origin']).agg(
#         value=('value', 'mean')
#     ).reset_index()

# def replace_nondetects(df):
#     """Replace non-detect (``NaN``) result values with zero.

#     In EQuIS, a ``NaN`` in ``RESULT_NUMERIC`` typically indicates that
#     the analyte was below the detection limit.  For HSPF calibration
#     purposes these are treated as zero.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Normalised EQuIS data.

#     Returns
#     -------
#     pandas.DataFrame
#         DataFrame with ``NaN`` values in the ``value`` column replaced
#         by ``0``.
#     """
#     df.loc[df['value'].isna(), 'value'] = 0
#     return df

# def filter_years(df, start_year=1996, end_year=None):
#     """Filter EQuIS data to a specified year range.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Must contain a ``datetime`` column of type ``datetime64``.
#     start_year : int, default 1996
#         Earliest year to keep (inclusive).
#     end_year : int, optional
#         Latest year to keep (inclusive).  ``None`` means no upper bound.

#     Returns
#     -------
#     pandas.DataFrame
#         Filtered subset.
#     """
#     df = df[df['datetime'].dt.year >= start_year]
#     if end_year is not None:
#         df = df[df['datetime'].dt.year <= end_year]
#     return df

# def normalize(df):
#     """Apply normalisation steps to raw EQuIS data (no analytical transforms).

#     Runs, in order:

#     1. :func:`map_constituents` — CAS_RN → constituent
#     2. :func:`normalize_timezone` — timestamp to UTC-6
#     3. :func:`normalize_columns` — select and rename columns
#     4. :func:`convert_units` — standardise units

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Raw EQuIS data as returned by :func:`download`.

#     Returns
#     -------
#     pandas.DataFrame
#         Normalised EQuIS data.
#     """
#     df = map_constituents(df)
#     df = normalize_timezone(df)
#     df = normalize_columns(df)
#     df = convert_units(df)
#     return df

# def transform(df):
#     """Full ETL pipeline: normalise → replace non-detects → filter → average.

#     This is the recommended entry point for preparing EQuIS data for
#     loading into the analytics layer of the data warehouse.

#     Steps performed in order:

#     1. :func:`normalize` — map constituents, fix timezones, rename
#        columns, convert units.
#     2. :func:`replace_nondetects` — set ``NaN`` values to ``0``.
#     3. :func:`filter_years` — remove records before 1996.
#     4. :func:`average_results` — round to hourly timestamps and average.

#     Parameters
#     ----------
#     df : pandas.DataFrame
#         Raw EQuIS data as returned by :func:`download`.

#     Returns
#     -------
#     pandas.DataFrame
#         Analysis-ready hourly observations tagged with
#         ``station_origin='equis'``.
#     """
    
#     df = normalize(df)
#     df = replace_nondetects(df)
#     df = filter_years(df)
#     if not df.empty:
#         df = average_results(df)
#     return df


def fetch_station_locations(connection: Optional[oracledb.Connection] = None):
    """Fetch geographic coordinates for all river/stream stations with HSPF-relevant data.

    Queries ``mpca_dal.MV_EQ_RESULT`` joined to ``EQ_FAC_STATION_NP``
    for distinct station locations.  Useful for mapping or spatial
    analysis of the monitoring network.

    Parameters
    ----------
    connection : oracledb.Connection, optional
        Oracle connection.  Falls back to the global :data:`CONNECTION`.

    Returns
    -------
    pandas.DataFrame
        Columns include ``SYS_LOC_CODE``, ``LONGITUDE``, ``LATITUDE``,
        ``LOC_MAJOR_BASIN``, and ``NON_PUBLIC_LOCATION_FLAG``.

    Raises
    ------
    ValueError
        If no connection is available.
    """
    conn = connection if connection is not None else CONNECTION
    if conn is None:
        raise ValueError("No connection provided and global CONNECTION is not set. Call connect() first or pass a connection.")
    
    query ="""SELECT DISTINCT
    m.SYS_LOC_CODE,
    stn.LONGITUDE,
    stn.LATITUDE,
    stn.LOC_MAJOR_BASIN,
    stn.NON_PUBLIC_LOCATION_FLAG
    FROM MPCA_DAL.MV_EQ_RESULT m
    LEFT JOIN MPCA_DAL.EQ_FAC_STATION_NP stn
    ON m.SYS_LOC_CODE = stn.SYS_LOC_CODE
    WHERE m.LOC_TYPE = 'River/Stream'
    AND m.CAS_RN IN ('479-61-8',
                        'CHLA-CORR',
                        'BOD',
                        'NO2NO3',
                        '14797-55-8',
                        '14797-65-0',
                        '14265-44-2',
                        'N-KJEL',
                        'PHOSPHATE-P',
                        '7723-14-0',
                        'SOLIDS-TSS',
                        'TEMP-W',
                        '7664-41-7')
        """
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = to_dataframe(cursor)
        return df
