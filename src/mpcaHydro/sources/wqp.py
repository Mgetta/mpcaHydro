


from dataretrieval import wqp
import pandas as pd
from pathlib import Path

EQUIS_PARAMETER_XREF = pd.read_csv(Path(__file__).parent.parent/'data/EQUIS_PARAMETER_XREF.csv')

CONSTITUENT_MAP = {i[0]:i[1] for i in EQUIS_PARAMETER_XREF[['PARAMETER','constituent']].values}

_STANDARD_CHARACTERISTICS = {
    "TP":    "Phosphorus",
    "TSS":   "Total suspended solids",
    "TKN":   "Kjeldahl nitrogen",
    "NO23":  "Nitrate + Nitrite",
    "OP":    "Orthophosphate",
    "NH3":   "Ammonia",
    "DO":    "Dissolved oxygen (DO)",
    "ChlA":  "Chlorophyll a",
    "WT":    "Temperature, water",
}

CONSTITUENT_MAP = {v:i for i, v in _STANDARD_CHARACTERISTICS.items()}

_TZ_OFFSET_HOURS = {
    'CST': -6,
    'CDT': -5,
    'UTC':  0,
    'EST': -5,
    'EDT': -4,
}

_TARGET_TZ = 'Etc/GMT+6'  # UTC-6 (note: POSIX convention flips the sign)


def _all_stations():
    df, _ = wqp.what_sites(siteType="Stream", 
                           characteristicName=";".join(_STANDARD_CHARACTERISTICS.values()),
                           startDateLo="01-01-1996",
                           statecode="US:27")
    return _filter_mpca_sites(df)


def find_stations(huc):
    query_params = {"huc": huc if isinstance(huc, str) else ";".join(huc)}
    query_params["siteType"] = 'Stream'
    df, _ = wqp.what_sites(**query_params)
    
    return _filter_mpca_sites(df)


def info(station_ids, constituent=None, as_gdf=False):
    query_params = {"siteid": ";".join(station_ids)}
    if constituent is not None:
        query_params["characteristicName"] = _STANDARD_CHARACTERISTICS[constituent]
    df, _ = wqp.what_sites(**query_params)

    return df


def download(huc, characteristic_names=None, start_year=None, end_year=None):
    if start_year is None:
        start_year = 1996
    if end_year is None:
        end_year = pd.Timestamp.now().year
    

    start_date = f"01-01-{start_year}"
    end_date = f"12-31-{end_year}"

    if characteristic_names is None:
        characteristic_names = list(_STANDARD_CHARACTERISTICS.values())

    characteristic_names = ";".join(characteristic_names)

    df, md = wqp.get_results(
        huc=huc,
        siteType="Stream",
        characteristicName=characteristic_names,
        startDateLo=start_date,
        endDateHi=end_date,
    )
    
    #Remove MNPCA sites (Will grab from Oracle instead)
    #df = _filter_mpca_stations(df)
    return df


def map_constituents(df):
    df['constituent'] = df['CharacteristicName'].map(CONSTITUENT_MAP)
    return df

def _filter_mpca_stations(df):
    return df.loc[~df['OrganizationIdentifier'].str.startswith('MNPCA')]


def _filter_missing_datetimes(df):
    missing_dt = df['ActivityStartDate'].isna() | df['ActivityStartTime/Time'].isna() | df['ActivityStartTime/TimeZoneCode'].isna()
    if missing_dt.any():
        print(f"Warning: {missing_dt.sum()} rows are missing date or time information and will be dropped.")
    return df.loc[~missing_dt]

def normalize_timezone(df):
    """Convert naive datetimes + timezone labels to a single target timezone.

    Unknown timezone labels produce NaT and are logged as warnings
    so they can be investigated without crashing the pipeline.
    """

    dt = pd.to_datetime(df['ActivityStartDate'] + ' ' + df['ActivityStartTime/Time'], errors='coerce')
    
    tz_label = df['ActivityStartTime/TimeZoneCode'].str.strip().str.upper()
    offset_hours = tz_label.map(_TZ_OFFSET_HOURS)

    # # Log unknown labels — don't crash, just warn
    unmapped = offset_hours.isna() & tz_label.notna()
    if unmapped.any():
        print(f"Warning: {unmapped.sum()} rows have unknown timezone labels: {tz_label[unmapped].unique().tolist()}. These will be set to NaT.")
    #     bad_labels = tz_label[unmapped].unique().tolist()
    #     n_bad = unmapped.sum()
    #     logger.warning(
    #         f"{n_bad} rows have unknown timezone labels: {bad_labels}. "
    #         f"These will be set to NaT."
    #     )

    # Convert to UTC, then to target — unmapped rows stay NaT naturally
    utc_dt = dt - pd.to_timedelta(offset_hours, unit='h')

    df = df.copy()
    df['datetime'] = (
        utc_dt
        .dt.tz_localize('UTC')
        .dt.tz_convert(_TARGET_TZ)
        .dt.tz_localize(None)
    )

    return df
