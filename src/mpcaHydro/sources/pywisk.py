# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 16:18:03 2023

@author: mfratki
"""
from pathlib import Path
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError, RequestException
import pandas as pd
import time



from pathlib import Path
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError, RequestException
import pandas as pd
import time

CERT_PATH = str(Path(__file__).resolve().parent / 'data' / 'wiskiweb01.pca.state.mn.us.crt')

VALID_AGGREGATION_TYPES = ['min', 'max', 'mean', 'average', 'total', 'counts']
VALID_INTERVALS = ['decadal', 'yearly', 'year', 'monthly', 'month', 'daily', 'day', 'hourly', 'hour']
BASE_URL = 'http://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS'
BASE_PARAMS = {
    'datasource': '0',
    'service': 'kisters',
    'type': 'queryServices',
    'format': 'json',
}
_last_url = None  # For debugging purposes, to see the last URL that was requested



def _format_params(args_dict):
    """Merge base params with request args, converting lists to comma-separated strings
    and dropping None values."""
    merged = {**BASE_PARAMS, **args_dict}
    return {
        k: ','.join(str(item) for item in v) if isinstance(v, list) else v
        for k, v in merged.items()
        if v is not None
    }


def construct_url(args_dict):
    """Return the full URL that would be sent, without making a request."""
    from requests import Request
    prepared = _format_params(args_dict)
    return Request('GET', BASE_URL, params=prepared).prepare().url



def _get(params):
    """Issue a GET request and return the requests.Response object."""
    global _last_url
    prepared = _format_params(params)
    response = requests.get(BASE_URL, params=prepared)
    response.raise_for_status()
    _last_url = response.url  # Store the last URL for debugging
    return response


# ── Connection test ──────────────────────────────────────────────────
def test_connection():
    try:
        response = requests.head('http://wiskiweb01.pca.state.mn.us', timeout=5)
        response.raise_for_status()
        return True, f"Website is UP (Status Code: {response.status_code})"
    except ConnectionError as e:
        return False, f"Website is DOWN (Connection Error): {e}"
    except Timeout as e:
        return False, f"Website is DOWN (Timeout Error): {e}"
    except HTTPError as e:
        return False, f"Website is experiencing issues (HTTP Error): {e}"
    except RequestException as e:
        return False, f"An unexpected error occurred: {e}"


# ── Introspection helpers ────────────────────────────────────────────
def _request_types():
    return _get({'request': 'getrequestinfo'}).json()[0]

def getRequests():
    return list(_request_types()['Requests'].keys())

def queryfields(request_type):
    return list(_request_types()['Requests'][request_type]['QueryFields']['Content'].keys())

def returnfields(request_type):
    return list(_request_types()['Requests'][request_type]['Returnfields']['Content'].keys())

def optionalfields(request_type):
    return list(_request_types()['Requests'][request_type]['Optionalfields']['Content'].keys())

# ── Data retrieval ───────────────────────────────────────────────────


def _parse_timeseries(records: list[dict]) -> pd.DataFrame:
    """Parse a getTimeseriesValues response into a DataFrame."""
    dfs = []
    for record in records:
        df = pd.DataFrame(record['data'], columns=record['columns'].split(','))
        # Attach metadata columns without mutating the original response
        meta = {k: v for k, v in record.items() if k not in ('data', 'rows', 'columns')}
        df = df.assign(**meta)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def _parse_table(records: list) -> pd.DataFrame:
    """Parse a header-row + data-rows response (getStationList, getTimeseriesList, etc.)."""
    return pd.DataFrame(records[1:], columns=records[0])

def get(args_dict):
    """Full request with field auto-fill from API introspection (mirrors old Service.get)."""
    request_type = args_dict['request']
    defaults = {f: None for f in queryfields(request_type) + optionalfields(request_type)}
    return _get({**defaults, **args_dict})

def get_df(args_dict):
    """Fetch data and convert to a DataFrame."""
    records = get(args_dict).json()
    parser = _PARSERS.get(args_dict['request'], _parse_table)
    return parser(records)
    
_PARSERS = {
    'getTimeseriesValues': _parse_timeseries,
    # Everything else uses the table format — add exceptions here as needed
}


def construct_aggregation(interval, aggregation_type):
    validate_interval(interval)
    validate_aggregation_type(aggregation_type) 
    return f'aggregate({interval}~{aggregation_type})'

def validate_aggregation_type(aggregation_type):
    if aggregation_type.startswith('perc-'):
        validate_percentile(aggregation_type)
    else: 
        assert(aggregation_type in VALID_AGGREGATION_TYPES)
    return True
   
def validate_percentile(aggregation_type):
    assert(aggregation_type.startswith('perc-'))
    perc_value = aggregation_type.split('-')[1]
    assert(perc_value.isdigit())
    perc_value = int(perc_value)
    assert(0 < perc_value < 100)
    return True


def validate_interval(interval):
    assert(interval in VALID_INTERVALS or validate_custom_interval(interval))
    return True

def validate_custom_interval(interval:str):
    # Custom interval in HHMMSS format
    assert(len(interval) == 6)
    assert(all(char.isdigit() for char in interval))
    assert(0 <= int(interval[0:2]) < 24)  # hours
    assert(0 <= int(interval[2:4]) < 60)  # minutes
    assert(0 <= int(interval[4:6]) < 60)  # seconds
    return True

def get_ts(
            ts_id,
            aggregation_interval = None,
            aggregation_type = None,
            start_date = '1996-01-01',
            end_date = '2050-12-31',
            stationgroup_id = None,
            timezone = 'GMT-6',
            as_json = False):
    
    if (aggregation_interval is not None) and (aggregation_type is not None):
        transformation = construct_aggregation(aggregation_interval, aggregation_type)
        ts_id = f'{ts_id};{transformation}'

    #print('Downloading Timeseries Data')
    args = {'request':'getTimeseriesValues',
            'ts_id' : ts_id,
            'from': start_date,
            'to': end_date,
            'returnfields': ['Timestamp', 'Value', 'Quality Code','Quality Code Name'],
            'metadata': 'true',
            'md_returnfields': ['ts_unitsymbol',
                                'ts_name',
                                'ts_id',
                                'station_no',
                                'station_name',
                                'station_latitude',
                                'station_longitude',
                                'parametertype_id',
                                'parametertype_name',
                                'stationparameter_no',
                                'stationparameter_name'],
            'timezone':timezone,
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID']}
    
    if as_json:
        output = get(args).json()
    else: 
        output = get_df(args)
    #print('Done!')
    return output
    
def get_stations(
                    huc_id = None, 
                    parametertype_id = None,
                    stationgroup_id = None,
                    stationparameter_no = None,
                    station_no = None,
                    returnfields = []):
    
    args = {'request':'getStationList'}
    
    returnfields = list(set(['ca_sta','station_no','station_name'] + returnfields))
        
    args ={'request': 'getStationList',
            'stationparameter_no': stationparameter_no,
            'stationgroup_id': stationgroup_id,
            'parametertype_id': parametertype_id,
            'station_no': station_no,
            #'object_type': object_type,
            'returnfields': returnfields,
            #                  'parametertype_id','parametertype_name',
            #                  'station_latitude','station_longitude',
            #                  'stationparameter_no','stationparameter_name'],
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID','stn_AUID','hydrounit_title','hydrounit_no','NearestTown']
            }
    
    
    df = get_df(args)
    if huc_id is not None: df = df.loc[df['stn_HUC12'].str.startswith(huc_id)]
    return df

def get_ts_ids(
                station_nos = None,
                ts_ids = None,
                parametertype_id = None,
                stationparameter_no = None,
                stationgroup_id = None,
                ts_name = None,
                returnfields = None):
    

    if returnfields is None:
        returnfields = ['ts_id','ts_name','ca_sta','station_no',
                            'ts_unitsymbol',
                            'parametertype_id','parametertype_name',
                            'station_latitude','station_longitude',
                            'stationparameter_no','stationparameter_name',
                            'station_no','station_name',
                            'coverage','ts_density']
    

    args ={'request': 'getTimeseriesList',
            'station_no': station_nos,
            'ts_id': ts_ids,
            'parametertype_id': parametertype_id,
            'stationparameter_no': stationparameter_no,
            'ts_name' : ts_name,
            'returnfields': returnfields,
            'ca_sta_returnfields': ['stn_HUC12','stn_EQuIS_ID','stn_AUID']}
    
    df = get_df(args)
    return df
    


def get_wplmn(station_nos):
    
    PARAMETERS_MAP={'5004':'TP Load',
                    '5005':'TP Conc',
                    '5014':'TSS Load',
                    '5015':'TSS Conc',
                    '5024':'N Load',
                    '5025':'N Conc',
                    '5034':'OP Load',
                    '5035':'OP Conc',
                    '5044':'TKN Load',
                    '5045':'TKN Conc',
                    '262' :'Flow'}
        
    ts_ids = get_ts_ids(station_nos = station_nos,
                        stationgroup_id = '1319204',
                        stationparameter_no = list(PARAMETERS_MAP.keys()),
                        ts_name = ['20.Day.Mean'])
    
    if len(ts_ids) == 0:
        print('No WPLMN Sites Available')
        return pd.DataFrame() 
    
    dfs = []
    for ts_id in ts_ids['ts_id']:
        dfs.append(get_ts(ts_id))
        time.sleep(1)
    
    return pd.concat(dfs)


        
# nutrient
#     -N03N02
#     -OP
#     -NH3
#     -TP
#     -DO
#     -CHla
# temperature
# flow

# test = pyWISK()

# df = test.get_ts(ts_ids = 424663010)

# df = test.get_ts(station_nos = 'W25060001')

# df = test.get_wplmn(huc8_id = '07020005')

# df = test.get_ts(huc_id = '07010205',stationgroup_id = '1319204',parametertype_id = 11500)
