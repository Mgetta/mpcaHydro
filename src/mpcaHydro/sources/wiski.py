"""
wiski
=====

Interface for downloading and transforming hydrological time-series data from
the MPCA WISKI database (powered by KISTERS).

Overview
--------
WISKI (Water Information System by KISTERS) is the primary continuous
monitoring data system used by MPCA.  This module wraps the low-level
``pywisk`` client to provide a high-level, constituent-oriented API for:

* **Downloading** raw time-series data for one or more monitoring stations
  and water-quality constituents (discharge, temperature, nutrients, etc.).
* **Normalizing** column names, units, and constituent codes so that outputs
  from WISKI can be merged with EQuIS lab-sample data downstream.
* **Transforming** the normalised data through quality-code filtering,
  hourly averaging, year filtering, and baseflow separation — producing
  analysis-ready DataFrames suitable for loading into the data warehouse.

When to use this module
-----------------------
Use ``wiski`` whenever you need continuous sensor data (e.g. 15-minute flow,
daily water temperature) from MPCA-managed stations.  If you instead need
discrete grab-sample laboratory results, see the :mod:`equis` module.

Typical workflow::

    import mpcaHydro.wiski as wiski

    # 1. Quick connectivity check
    wiski.test_connection()

    # 2. Download raw data for two stations
    df_raw = wiski.download(['E66050001', '02HA001'])

    # 3. Produce analysis-ready data
    df = wiski.transform(df_raw)

Key data structures
-------------------
``PARAMETERTYPE_MAP``
    Maps WISKI ``parametertype_id`` codes to short constituent names
    (e.g. ``'11500'`` → ``'Q'`` for discharge).

``TS_NAME_SELECTOR``
    Nested dict that selects the correct time-series name for each
    constituent / station type (Internal vs External) / time resolution
    (unit vs daily).  External stations (IDs beginning with ``'E'``) use
    provisionally-edited series, while internal stations use archive series.

``VALID_CONSTITUENTS``
    The set of constituent abbreviations supported by the download pipeline:
    ``Q``, ``WT``, ``OP``, ``DO``, ``TP``, ``TSS``, ``N``, ``TKN``, ``TRB``.
"""

import pandas as pd
from mpcaHydro.sources import pywisk
import baseflow as bf
import time
import concurrent

#%% Define Selectors and Maps
PARAMETERTYPE_MAP ={'11522': 'TP',
                    '11531': 'TP',
                    '11532': 'TSS',
                    '11523': 'TSS',
                    '11526': 'N',
                    '11519': 'N',
                    '11520': 'OP',
                    '11528': 'OP',
                    '11530': 'TKN',
                    '11521': 'TKN',
                    '11500' : 'Q',
                    '11504': 'WT',
                    '11533': 'DO',
                    '11507':'WL'}

DATA_CODES = [1,3,10,12,15,20,29,30,31,32,34,45,46,47,48,49]

TS_NAME_SELECTOR = {'Q':{'Internal':{'daily':'20.Day.Mean.Archive',
                                     'unit': '15.Rated'},
                         'External': {'daily': '20.Day.Mean',
                                      'unit': '08.Provisional.Edited'}},
                    'WT':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},
                    'TSS':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                   
                    'N':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'TKN':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'TP':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'OP':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},                    
                    'DO':{'Internal':{'daily':'20.Day.Mean',
                                      'unit': '09.Archive'},
                          'External': {'daily': '20.Day.Mean',
                                       'unit': '08.Provisional.Edited'}},
                    'TRB':{'Internal':{'daily':'20.Day.Mean',
                                    'unit': '09.Archive'},
                        'External': {'daily': '20.Day.Mean',
                                    'unit': '08.Provisional.Edited'}}}

#STATIONPARAMETER_NOS = ['262*','450*','451*','863*','866*','5034' ,'5035','5005', '5004','5014' ,'5015','5024'  ,'5025','5044' ,'5045']
STATIONPARAMETER_NOS = ['262*','450*','451*','863*','866*']

CONSTITUENT_NAME_NO = {'Q'  :['262*'],#,'263'],
                       'WT' :['450*', '451*'], # '450.42','451.42'],
                       'OP' :['863*'],
                       'DO' :['866*'],
                       'TRB':['811*'],
                       'TP' :None,
                       'TSS':None,
                       'N'  :None,
                       'TKN':None}

STATIONPARAMETER_NOS_MAP = {'262*':'Q',
                            '450*':'WT',
                            '451*':'WT',
                            '863*':'OP',
                            '866*':'DO',
                            '811*':'TRB'}

CONSTITUENT_NAME_NO_WPLMN = {'Q'  :['262*'],#,'263'],
                       'WT' :['450*', '451*'], # '450.42','451.42'],
                       'OP' :['863*','5034' ,'5035'],
                       'DO' :['866*'],
                       'TP' :['5005'  ,'5004'],
                       'TSS':['5014' ,'5015'],
                       'N'  :['5024'  ,'5025'],
                       'TKN':['5044' ,'5045']}

VALID_CONSTITUENTS = ['Q','WT','OP','DO','TP','TSS','N','TKN','TRB']

def test_connection():
    """Test connectivity to the WISKI KISTERS API.

    Delegates to :func:`pywisk.test_connection` and returns its result.

    Returns
    -------
    bool
        ``True`` if the connection succeeds.

    Raises
    ------
    ConnectionError
        If the KISTERS API is unreachable.
    """
    return pywisk.test_connection()

def info(station_ids: list, constituent=None):
    """Retrieve time-series metadata for one or more WISKI stations.

    Queries the KISTERS API for time-series identifiers that match the
    requested station(s) and, optionally, a single constituent.  The
    returned DataFrame is normalised via :func:`normalize_columns` so
    that column names and constituent codes are consistent with the rest
    of the package.

    Use this method to discover what data is available *before*
    committing to a full download.

    Parameters
    ----------
    station_ids : list of str
        WISKI station numbers (e.g. ``['E66050001']``).
    constituent : str, optional
        If provided, must be one of :data:`VALID_CONSTITUENTS`.  Limits
        the query to the ``stationparameter_no`` codes mapped to that
        constituent.  When ``None`` the default set
        :data:`STATIONPARAMETER_NOS` is used.

    Returns
    -------
    pandas.DataFrame
        Normalised metadata with columns such as ``station_id``,
        ``constituent``, ``ts_name``, etc.
    """
    if constituent is not None:
        stationparameter_nos = CONSTITUENT_NAME_NO[constituent]
    else:
        stationparameter_nos = STATIONPARAMETER_NOS
    
    df = pywisk.get_ts_ids(station_nos = station_ids,
                            stationparameter_no = stationparameter_nos,
                            ts_name = ['15.Rated','09.Archive','08.Provisional.Edited'])

    df = normalize_columns(df)

    # rows = []
    # for station_id in df['station_id'].unique():            
    #     for constituent in df.loc[df['station_id'] == station_id,'constituent'].unique():
    #         df_station_constituent = df.loc[(df['station_id'] == station_id) & (df['constituent'] == constituent) & df['ts_name'].isin(['15.Rated','09.Archive','08.Provisional.Edited'])]
    #         if not df_station_constituent.empty:
    #             if station_id.lower().startswith('e'):
    #                 ts_names = TS_NAME_SELECTOR[constituent]['External']['unit']
    #             else:
    #                 ts_names = TS_NAME_SELECTOR[constituent]['Internal']['unit']
    #             rows.append(df_station_constituent.loc[df_station_constituent['ts_name'] == ts_names,:])
 
    return df





def download(station_ids: list, constituent=None, start_year: int = 1996, end_year: int = 2030, wplmn: bool = False):
    """Download raw time-series data from WISKI for one or more stations.

    This is the primary entry point for fetching continuous monitoring data.
    For every station the method iterates over each requested constituent,
    calls the KISTERS API in year-chunked requests, and concatenates the
    results.  A ``wplmn_flag`` column is appended to indicate whether the
    station belongs to the Watershed Pollutant Load Monitoring Network.

    **Why chunked downloads?**
    The KISTERS API can time-out or return partial data for very long date
    ranges.  Splitting the request into two-year windows (see
    :func:`download_chunk`) improves reliability.

    Parameters
    ----------
    station_ids : list of str
        WISKI station numbers.
    constituent : str, optional
        Restrict to a single constituent (e.g. ``'Q'``).  Must be in
        :data:`VALID_CONSTITUENTS`.  If ``None``, all valid constituents
        are downloaded.
    start_year : int, default 1996
        First calendar year to request.
    end_year : int, default 2030
        Last calendar year to request (inclusive).
    wplmn : bool, default False
        When ``True``, use the extended WPLMN station-parameter number
        mapping (:data:`CONSTITUENT_NAME_NO_WPLMN`) which includes
        additional nutrient parameter codes.

    Returns
    -------
    pandas.DataFrame
        Raw WISKI data with a ``wplmn_flag`` column (``1`` if the station
        belongs to WPLMN, ``0`` otherwise).

    Raises
    ------
    ValueError
        If *constituent* is not in :data:`VALID_CONSTITUENTS`, or if a
        station ID is not a string.
    """
    if constituent is None:
        constituents = VALID_CONSTITUENTS
    else:
        if constituent not in VALID_CONSTITUENTS:
            raise ValueError(f'Invalid constituent: {constituent}. Valid constituents are: {VALID_CONSTITUENTS}')
        constituents = [constituent]

    dfs = [pd.DataFrame()]
    for station_id in station_ids:
        if not isinstance(station_id,str):
            raise ValueError(f'Station ID {station_id} is not a string')
        print('Downloading Timeseries Data')
        df = pd.concat([_download(constituent,station_id,start_year,end_year,wplmn) for constituent in constituents])

        if not df.empty:
            dfs.append(df)
    df = pd.concat(dfs)

    station_metadata = pywisk.get_stations(station_no = station_ids,returnfields = ['stationgroup_id'])
    if any(station_metadata['stationgroup_id'].isin(['1319204'])):
        df['wplmn_flag'] = 1
    else:
        df['wplmn_flag'] = 0
    print('Done!')
    
    return df

def _get_ts_ids(station_nos: list, constituent: str):
    """Fetch time-series IDs for a station/constituent pair.

    Tries the unit-resolution time-series name first; if none is found it
    falls back to the daily-resolution name.

    Parameters
    ----------
    station_nos : list of str
        WISKI station numbers.
    constituent : str
        One of :data:`VALID_CONSTITUENTS`.

    Returns
    -------
    pandas.DataFrame
        Rows from the KISTERS ``getTimeseriesList`` endpoint.

    Raises
    ------
    ValueError
        If *constituent* is not valid.
    """
    if constituent not in VALID_CONSTITUENTS:
        raise ValueError(f'Invalid constituent: {constituent}. Valid constituents are: {VALID_CONSTITUENTS}')
    
    if station_nos[0] == 'E':
        ts_names = TS_NAME_SELECTOR[constituent]['External']
    else:
        ts_names =TS_NAME_SELECTOR[constituent]['Internal']
    
    constituent_nos = CONSTITUENT_NAME_NO[constituent]
    
    ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                            stationparameter_no = constituent_nos,
                            ts_name = ts_names['unit'])
    
    if ts_ids.empty:
        ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                                stationparameter_no = constituent_nos,
                                ts_name = ts_names['daily'])
    
    return ts_ids

def _download(constituent, station_nos, start_year=1996, end_year=2030, wplmn=False):
    """Download raw data for a single constituent and station.

    Internal helper called by :func:`download`.  Resolves the correct
    time-series IDs via :func:`_get_ts_ids` logic and delegates the
    actual data retrieval to :func:`convert_to_df`.

    Parameters
    ----------
    constituent : str
        Constituent abbreviation (e.g. ``'Q'``).
    station_nos : str
        WISKI station number.
    start_year : int, default 1996
        First calendar year.
    end_year : int, default 2030
        Last calendar year.
    wplmn : bool, default False
        Use WPLMN parameter mapping when ``True``.

    Returns
    -------
    pandas.DataFrame
        Raw timeseries data, or an empty DataFrame if no data is found.
    """

    if station_nos[0] == 'E':
        ts_names = TS_NAME_SELECTOR[constituent]['External']
    else:
        ts_names =TS_NAME_SELECTOR[constituent]['Internal']
    
    if wplmn:
        constituent_nos = CONSTITUENT_NAME_NO_WPLMN[constituent]
    else:
        constituent_nos = CONSTITUENT_NAME_NO[constituent]
    
    if constituent_nos is not None:
        ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                            stationparameter_no = constituent_nos,
                            ts_name = ts_names['unit'])
        if ts_ids.empty:
            ts_ids = pywisk.get_ts_ids(station_nos = station_nos,
                                stationparameter_no = constituent_nos,
                                ts_name = ts_names['daily'])
            statistic = 'MEAN'
            if ts_ids.empty:
                return pd.DataFrame()    
        else:
            statistic = 'INST'
        
        
        df = convert_to_df(ts_ids['ts_id'],start_year,end_year)

        
        if df.empty:
            print(f'No data found for station {station_nos} and constituent {constituent}')
            return pd.DataFrame()    
        
        df['Date'] = df['Timestamp'].dt.date
        df['Time'] = pd.NA
        df['Timezone'] = 'CST'
        if statistic == 'INST':
            df['Time'] = df['Timestamp'].dt.time
        df['statistic'] = statistic
        df['grain'] = 'continuous'

    else:
        df = pd.DataFrame()
    return df



def _nominal_interval(df, groupby_columns):
    """Compute time differences between consecutive records within each group."""
    diffs = (
        df.index.to_series()
        .groupby([df[c] for c in groupby_columns])
        .diff()
        .dt.total_seconds()
        .div(60)
    )
    df['interval_minutes'] = diffs

    #calculate mode of interval_minutes for each groupby subset
    modes = df.groupby(groupby_columns)['interval_minutes'].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA)
    

    return modes

def _interval(df, groupby_columns=['station_id', 'constituent']):
    """For each groupby subset, find continuous segments with more than 10
    records and return the mode of the interval (in minutes) per group.

    Assumes the DataFrame index is a datetime.
    """
    # compute diffs between consecutive datetime-index values within each group
    diffs = (
        df.index.to_series()
        .groupby([df[c] for c in groupby_columns])
        .diff()
        .dt.total_seconds()
        .div(60)
    )
    df['interval_minutes'] = diffs

    def _mode_of_long_segments(s, min_len=2):
        # a new segment starts whenever the interval changes
        seg_id = (s != s.shift()).cumsum()
        seg_sizes = seg_id.map(seg_id.value_counts())
        long_vals = s[seg_sizes > min_len].dropna()
        if long_vals.empty:
            return pd.NA
        return long_vals.mode().iloc[0]

    modes = (
        df.groupby(groupby_columns)['interval_minutes']
        .apply(_mode_of_long_segments)
        .rename('interval_minutes_mode')
    )

    modes = modes.dropna().reset_index()



    modes_grouper = modes.groupby(groupby_columns)

    modes_count = modes_grouper.count()
    assert (modes_count['interval_minutes_mode'] == 1).all(), f'Multiple modes found for some station/constituent groups: {modes_count[modes_count["interval_minutes_mode"] > 1]}'        

    intervals = modes_grouper.first()['interval_minutes_mode']

    return intervals

# For each groupby pair flag groups that have varying interval minutes, which indicates a mix of different time resolutions (e.g. 15-minute and daily) that should be separated before loading to the warehouse.


def download_chunk_legacy(ts_id, start_year=1996, end_year=2030, interval=2, as_json=False):
    """Download a single time-series in year-chunked windows.

    Splits the ``[start_year, end_year]`` range into sub-ranges of
    *interval* years and issues one API call per sub-range.  A short
    sleep is inserted between calls to respect API rate limits.

    Parameters
    ----------
    ts_id : str
        KISTERS time-series identifier.
    start_year : int, default 1996
        First calendar year.
    end_year : int, default 2030
        Last calendar year.
    interval : int, default 2
        Number of years per request window.
    as_json : bool, default False
        Return raw JSON instead of a DataFrame.

    Returns
    -------
    pandas.DataFrame
        Concatenated time-series values across all windows.
    """
    frames = []
    for start in range(start_year, end_year + 1, interval):  # +1 to include end_year
        end = min(start + interval - 1, end_year)
        df = pywisk.get_ts(
            ts_id,
            start_date=f'{start}-01-01',
            end_date=f'{end}-12-31',
            as_json=as_json,
        )
        if not df.empty:
            df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
            frames.append(df)
        time.sleep(0.1)

    if not frames:
        df = pd.DataFrame()
    else:
        df = pd.concat(frames, ignore_index=True)

    return df


def _generate_date_ranges(start_year, end_year, interval):
    """Sub-procedure 1: Calculate all the 2-year window boundaries."""
    for start in range(start_year, end_year + 1, interval):
        end = min(start + interval - 1, end_year)
        yield f'{start}-01-01', f'{end}-12-31'

def _fetch_and_clean_chunk(ts_id, start_date, end_date, as_json):
    """Sub-procedure 2: Execute a single network request and clean its output."""
    try:
        df = pywisk.get_ts(ts_id, start_date=start_date, end_date=end_date, as_json=as_json)
        
        if not df.empty:
            df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.tz_localize(None)
        
        return df
    
    except Exception as exc:
        print(f"FAILED chunk {start_date} to {end_date} for ts_id {ts_id}. Error: {exc}")
        return pd.DataFrame()  # Always return a DataFrame, even if empty, for consistency

def download_chunk(ts_id, start_year=1996, end_year=2030, interval=2, as_json=False):
    """
    Main Procedure: Coordinates generating dates, fetching data concurrently, 
    and compiling the final dataset.
    """
    # 1. Plan the work
    date_ranges = list(_generate_date_ranges(start_year, end_year, interval))
    frames = []

    # 2. Execute the work concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all chunk requests to the pool
        futures = [
            executor.submit(_fetch_and_clean_chunk, ts_id, start, end, as_json)
            for start, end in date_ranges
        ]

        # Gather completed dataframes
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if not df.empty:
                frames.append(df)

    # 3. Assemble and sort the final product
    if not frames:
        return pd.DataFrame()
        
    final_df = pd.concat(frames, ignore_index=True)
    final_df = final_df.sort_values('Timestamp').reset_index(drop=True)

    return final_df

def convert_to_df(ts_ids, start_year=1996, end_year=2030):
    """Download and concatenate data for multiple time-series IDs.

    For each *ts_id* the available date range is queried first, and
    ``start_year`` / ``end_year`` are narrowed to the range that actually
    contains data before calling :func:`download_chunk`.

    Parameters
    ----------
    ts_ids : iterable of str
        One or more KISTERS time-series identifiers.
    start_year : int, default 1996
        Earliest year to download.
    end_year : int, default 2030
        Latest year to download.

    Returns
    -------
    pandas.DataFrame
        Concatenated time-series data.
    """
    dfs = []
    for ts_id in ts_ids:
        ts_info = pywisk.get_ts_ids(ts_ids=ts_id)[['from', 'to']]

        # Scope date narrowing to this TS ID only
        ts_start = start_year
        ts_end = end_year
        if ts_info['from'].iloc[0] != '':
            ts_start = int(ts_info['from'].iloc[0][:4])
        if ts_info['to'].iloc[0] != '':
            ts_end = int(ts_info['to'].iloc[0][:4])

        dfs.append(download_chunk(ts_id, ts_start, ts_end))
        time.sleep(0.1)
    return pd.concat(dfs)





def convert_units(df):
    """Convert raw WISKI measurement units to package-standard units.

    The following conversions are applied *in-place*:

    * Celsius (``°c``) → Fahrenheit (``degf``)
    * Kilograms (``kg``) → Pounds (``lb``)
    * Cubic-feet-per-second symbol (``ft³/s``) → ``cfs``

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing a ``ts_unitsymbol`` column and a ``Value``
        column.

    Returns
    -------
    pandas.DataFrame
        The same DataFrame with updated ``ts_unitsymbol`` and ``Value``
        columns.
    """
    # Convert units
    #Water temperature``
    df.loc[:,'ts_unitsymbol'] = df['ts_unitsymbol'].str.lower()
    df.replace({'ts_unitsymbol':'°c'},'degf',inplace = True)
    df.loc[df['ts_unitsymbol'] == 'degf','Value'] = df.loc[df['ts_unitsymbol'] == 'degf','Value'].apply(lambda x: (x*9/5)+32)

    # Convert kg to lb
    df.loc[df['ts_unitsymbol'] == 'kg','Value'] = df.loc[df['ts_unitsymbol'] == 'kg','Value'].apply(lambda x: (x*2.20462))
    df.replace({'ts_unitsymbol':'kg'},'lb',inplace=True)

    # rename ft3/s to cfs
    df.replace({'ts_unitsymbol':'ft³/s'},'cfs',inplace=True)
    return df


def map_constituents(df):
    """Map ``stationparameter_no`` codes to human-readable constituent names.

    Uses :data:`STATIONPARAMETER_NOS_MAP` to convert prefix-matched
    parameter numbers (e.g. ``'262*'`` → ``'Q'``).

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain a ``stationparameter_no`` column.

    Returns
    -------
    pandas.DataFrame
        DataFrame with an added ``constituent`` column.
    """
    def map_values(value):
        for key, replacement in STATIONPARAMETER_NOS_MAP.items():
            if value.startswith(key.rstrip('*')):  # Match prefix without the wildcard '*'
                return replacement
        return value  # If no match, return the original value

    df['constituent'] = df['stationparameter_no'].apply(map_values)
    return df

def normalize_columns(df):
    """Rename raw WISKI columns to the package-standard schema.

    After constituent mapping, the following renames are applied:

    * ``station_no`` → ``station_id``
    * ``Timestamp`` → ``datetime``
    * ``Value`` → ``value``
    * ``ts_unitsymbol`` → ``unit``
    * ``Quality Code`` → ``quality_code``
    * ``Quality Code Name`` → ``quality_code_name``

    Parameters
    ----------
    df : pandas.DataFrame
        Raw WISKI DataFrame.

    Returns
    -------
    pandas.DataFrame
        DataFrame with standardised column names.
    """
    # Map parameter numbers to constituent names
    #df['constituent'] = df['stationparameter_no'].map(STATIONPARAMETER_NOS_MAP,regex=True)
    
    df = map_constituents(df)

    df.rename(columns={
        'station_no':'station_id',
        'Timestamp':'datetime',
        'Value':'value',
        'ts_unitsymbol':'unit',
        'Quality Code':'quality_code',
        'Quality Code Name':'quality_code_name'}, inplace=True)
    return df
    


def filter_quality_codes(df, data_codes):
    """Keep only rows whose ``quality_code`` is in the accepted set.

    WISKI assigns numeric quality codes to every measurement.  Only codes
    listed in :data:`DATA_CODES` (or the caller-supplied list) represent
    validated observations suitable for analysis.

    Parameters
    ----------
    df : pandas.DataFrame
        Normalised WISKI data containing a ``quality_code`` column.
    data_codes : list of int
        Acceptable quality-code values.

    Returns
    -------
    pandas.DataFrame
        Filtered subset.
    """
    return df.loc[df['quality_code'].isin(data_codes)]

def filter_years(df, start_year=1996, end_year=None):
    """Filter data to include only observations within a year range.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain a ``datetime`` column of type ``datetime64``.
    start_year : int, default 1996
        Earliest year to keep (inclusive).
    end_year : int, optional
        Latest year to keep (inclusive).  If ``None``, no upper bound.

    Returns
    -------
    pandas.DataFrame
        Filtered subset.
    """
    df = df[df['datetime'].dt.year >= start_year]
    if end_year is not None:
        df = df[df['datetime'].dt.year <= end_year]
    return df


def average_results(df):
    """Aggregate observations to hourly mean values.

    Timestamps are rounded to the nearest hour, and values are averaged
    within each ``(station_id, datetime, constituent, unit)`` group.

    Parameters
    ----------
    df : pandas.DataFrame
        Normalised WISKI observations.

    Returns
    -------
    pandas.DataFrame
        Hourly-averaged observations.
    """
    df.loc[:,'datetime'] = df.loc[:,'datetime'].dt.round('h')
    return df.groupby(['station_id', 'datetime', 'constituent', 'unit']).agg(value=('value', 'mean')).reset_index()
    # Convert units


def calculate_baseflow(df, method='Boughton'):
    """Estimate baseflow from discharge data using a digital-filter method.

    For every unique ``station_id`` with discharge (``Q``) records, this
    function runs a single-pass baseflow separation (via the ``baseflow``
    package) and appends the results as a new constituent ``QB`` (baseflow).

    **Why baseflow?**
    HSPF calibration requires separate baseflow and stormflow targets.
    Separating baseflow from total discharge allows downstream model
    comparisons to evaluate both components independently.

    Parameters
    ----------
    df : pandas.DataFrame
        Must include rows where ``constituent == 'Q'``.
    method : str, default ``'Boughton'``
        Baseflow separation algorithm name passed to
        :func:`baseflow.single`.

    Returns
    -------
    pandas.DataFrame
        Original *df* with baseflow rows (``constituent='QB'``)
        appended.
    """
    dfs = [df]
    for station_id in df['station_id'].unique():
        df_station = df.query(f'constituent == "Q" & station_id == "{station_id}"')[['datetime', 'value']].copy().set_index('datetime')
        if df_station.empty | len(df_station) < 10:  # baseflow separation requires a minimum number of observations
            continue
        else:
            df_baseflow = bf.single(df_station['value'], area = None, method = method,return_kge = False)[0][method]
            
            df_baseflow = pd.DataFrame(
                {
                    "station_id": station_id,
                    "station_origin": 'wiski',
                    "datetime": df_baseflow.index,
                    "value": df_baseflow.values,
                    "constituent": 'QB',
                    "unit": 'cfs',
                }
            )
            dfs.append(df_baseflow)
    
    return pd.concat(dfs)


def normalize(df):
    """Standardise raw WISKI data without analytical transformations.

    Applies unit conversion (:func:`convert_units`) and column renaming
    (:func:`normalize_columns`).  No quality-code filtering, averaging,
    or baseflow separation is performed — use :func:`transform` for the
    full pipeline.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw WISKI data as returned by :func:`download`.

    Returns
    -------
    pandas.DataFrame
        Normalised WISKI data with standardised column names and units.
    """

    df = convert_units(df)
    df = normalize_columns(df)
    return df

def transform(df, filter_qc_codes=True, data_codes=None, baseflow_method='Boughton', include_baseflow = True):
    """Full ETL pipeline: normalise → filter → average → baseflow.

    This is the recommended entry point for preparing WISKI data for
    loading into the analytics layer of the data warehouse.

    Steps performed in order:

    1. :func:`normalize` — unit conversion and column renaming.
    2. :func:`filter_quality_codes` — keep only validated observations
       (optional, controlled by *filter_qc_codes*).
    3. :func:`average_results` — round to hourly timestamps and average.
    4. :func:`filter_years` — remove records before 1996.
    5. :func:`calculate_baseflow` — derive baseflow (``QB``) from
       discharge.
    6. Tag ``station_origin`` as ``'wiski'``.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw WISKI data as returned by :func:`download`.
    filter_qc_codes : bool, default True
        Whether to apply quality-code filtering.
    data_codes : list of int, optional
        Custom quality-code whitelist.  Defaults to :data:`DATA_CODES`.
    baseflow_method : str, default ``'Boughton'``
        Algorithm name for baseflow separation.
    include_baseflow : bool, default True
        Whether to include baseflow (``QB``) in the output.

    Returns
    -------
    pandas.DataFrame
        Analysis-ready hourly observations including baseflow, tagged
        with ``station_origin='wiski'``.
    """
    df = normalize(df)
    if filter_qc_codes:
        if data_codes is None:
            data_codes = DATA_CODES
        df = filter_quality_codes(df, data_codes)
    df = average_results(df)
    df = filter_years(df, start_year=1996)
    if include_baseflow:
        df = calculate_baseflow(df, method = baseflow_method)
    df['station_origin'] = 'wiski'
    #df.set_index('datetime',inplace=True)
    return df



