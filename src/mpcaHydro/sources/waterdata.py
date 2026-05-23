
from dataretrieval import waterdata
import pandas as pd

_STANDARD_PARAMS = {'Q':'00060', 
                    'WT': '00010',
                     'DO': '00300'}
_STANDARD_STATISTIC_IDS = {'00011' : 'instantaneous', 
                             '00003' : 'mean'}

def _get_parameter_codes():
    params, _ = waterdata.get_reference_table(collection="parameter-codes",)
    return params


def info(station_ids = None, huc = None, parameter_codes = None, skip_geometry = False):
     
    if huc is not None:
          station_ids = find_stations(huc)['monitoring_location_id'].to_list()
    elif station_ids is None:
        raise ValueError("Must provide either station_ids or huc")

    if parameter_codes is None:
        parameter_codes = list(_STANDARD_PARAMS.values())

    gdf, _ = waterdata.get_time_series_metadata(monitoring_location_id=station_ids,
                                                parameter_code=list(_STANDARD_PARAMS.values()),
                                                statistic_id=list(_STANDARD_STATISTIC_IDS.keys()),
                                                skip_geometry = skip_geometry)
    return gdf

    
def find_stations(huc,skip_geometry = False):
    gdf, _ = waterdata.get_monitoring_locations(hydrologic_unit_code=huc, site_type_code=['ST'], skip_geometry=skip_geometry  )
    return gdf


def download(station_ids, constituents = None, start_year=None, end_year=None):
    
    if constituents is None:
        params = list(_STANDARD_PARAMS.values())
    else:
        params = [ _STANDARD_PARAMS[c] for c in constituents]


    dfs = []
    df = info(station_ids=station_ids, skip_geometry=True,parameter_codes = params)
    for _, row in df.iterrows():
        print(f"Downloading {row['monitoring_location_id']} {row['parameter_code']} {row['statistic_id']}")
        if row['computation_period_identifier'] == 'Daily' and (row['computation_identifier'] == 'Mean'):
            data, _ = waterdata.get_daily(row['time_series_id'], skip_geometry=True)
        elif row['computation_period_identifier'] == 'Points':
            data,_ = _download_continuous(row['time_series_id'], row['begin'].year, row['end'].year, start_year, end_year)
        else:
            raise ValueError(f"Unsupported computation period: {row['computation_period_identifier']} for time series {row['time_series_id']}")
        dfs.append(data)
    df = pd.concat(dfs, ignore_index=True)
  
    return df

 
def _construct_time_intervals(start_year, end_year, chunk_size=2):
    return [
        f"{year}-01-01/{min(year + chunk_size - 1, end_year)}-12-31"
        for year in range(start_year, end_year + 1, chunk_size)
    ]

    
def _download_continuous(timeseries_id,start_year=None,end_year=None,ts_start_year=None,ts_end_year=None):
    
    if ts_start_year is None or ts_end_year is None:
        metadata,_ = waterdata.get_time_series_metadata(time_series_id=timeseries_id, skip_geometry=True)
        ts_start_year = metadata['begin'].iloc[0].year
        ts_end_year = metadata['end'].iloc[0].year

    #two year chunks to avoid timeouts on long time series between 1996 and present
    if start_year is None:
        start_year = ts_start_year
    if end_year is None:
        end_year = ts_end_year
    
    # split into two year chunks to avoid timeouts on long time series
    time_intervals = _construct_time_intervals(start_year, end_year, chunk_size=2)

    dfs = []
    for time in time_intervals:
        print(time)
        df, _ = waterdata.get_continuous(time_series_id = timeseries_id, time=time)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)  
    return df


def _filter_non_detects(df):
    raise NotImplementedError("Non-detect filtering not implemented yet. Need to understand how WQP encodes non-detects in the results.")   

def _aggregate(df):
    raise NotImplementedError("Daily aggregation not implemented yet. Need to understand how to handle multiple samples per day, especially if there are non-detects.")

def _normalize_columns(df):
    raise NotImplementedError("Column normalization not implemented yet. Need to decide on a standard set of columns and how to handle missing or extra columns from WQP.") 

def _convert_units(df):
    raise NotImplementedError("Unit conversion not implemented yet. Need to understand the units used in WQP and how to convert them to a standard set of units for our database.") 




