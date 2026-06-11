# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:13:23 2023

@author: mfratki
"""

import pandas as pd
from pathlib import Path
#from hspf_tools.orm.models import Station
# import geopandas as gpd

EQUIS_PARAMETER_XREF = pd.read_csv(Path(__file__).parent.parent/'data/EQUIS_PARAMETER_XREF.csv')


CONSTITUENT_MAP = {i[0]:i[1] for i in EQUIS_PARAMETER_XREF[['PARAMETER','constituent']].values}
  

# station_no  = 	'S010-822'
# data = download(station_no)
# data = transform(data) 


# def download(station_nos):
#     df = pd.concat([_download(station_no) for station_no in station_nos])
#     return df
import requests

def _download(station_id):
    # Replace {station_no} in the URL with the actual station number
    #url = f"https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=json"
    url = 'https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results'

    try:
        # Send a GET request to the URL
        params = {
            'stationId': station_id,
            'format': 'json'
        }
        response = requests.get(url,params = params)
        response.raise_for_status()  # Raise exception for HTTP errors
        # Parse the JSON data
        return pd.DataFrame(response.json()['data'])
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None



def download(station_ids):
    #df = pd.read_csv(f'https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=csv')
    dfs = []
    for station_id in station_ids:
        df = _download(station_id)
        if not df.empty:
            df['station_id'] = station_id
            dfs.append(df)

    return pd.concat(dfs, ignore_index=True)



def info(station_no):
    #df = pd.read_csv(f'https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?stationId={station_no}&format=csv')
    df = _download(station_no)
    df['station_id'] = station_no
    df.loc[:,'resultUnit'] = df['resultUnit'].str.lower()
    df.replace({'resultUnit':'kg'},'lb',inplace=True)
    df.replace({'resultUnit':'ug/l'},'mg/l',inplace=True)
    df.replace({'resultUnit':'deg c'},'degF',inplace=True)
    df.replace({'resultUnit':'deg c'},'degF',inplace=True)
    
    return df.drop_duplicates(subset = 'station_id')
  

# def _info(station_nos):
#     station_info = info(station_nos)
#     if station_info.empty:
#         return Station(station_nos,
#                        'equis',
#                        station_type = 'River')
#     else:             
#         return Station(station_info.iloc[0]['stationId'],
#                        'equis',
#                        station_name = station_info.iloc[0]['stationName'],
#                        station_type = 'River')
            
    

def transform(df):
    df = df.loc[df['parameter'].isin(CONSTITUENT_MAP.keys()),:]
    df['datetime'] = pd.to_datetime(list(df.loc[:,'sampleDate'] +' ' + df.loc[:,'sampleTime']))
    df = df.loc[(df['datetime'] > '1996') & (df['result'] != '(null)')]
    
    if df.empty:
        return df
    
    df['result'] = pd.to_numeric(df['result'], errors='coerce')
    df.rename(columns = {'result': 'value',
                           'parameter': 'variable',
                           'stationName': 'station_name',
                           'stationID': 'station_id',
                           'resultUnit':'unit'},inplace=True)
    
    df['constituent'] = df['variable'].map(CONSTITUENT_MAP)
    df['station_origin'] = 'swd'
    df['quality_id'] = pd.NA
    station_name = df.iloc[0]['station_name']
    df = df.loc[:,['datetime','value','variable','unit','station_id','station_name','constituent','station_origin']]
    
    df = df.astype({'value':float,
               'unit':str,
               'station_id':str,
               'station_name':str,
               'constituent':str})
    
    # convert ug to mg/l
    df.loc[:,'unit'] = df['unit'].str.lower()
    df.loc[df['unit'] == 'ug/l','value'] = df.loc[df['unit'] == 'ug/l','value']*.001
    df.loc[df['unit'] == 'kg','value'] = df.loc[df['unit'] == 'kg','value']*2.20462
    df.loc[df['unit'] == 'deg c','value'] = df.loc[df['unit'] == 'deg c','value']*9/5 + 32 # Convert celsius to faren

    df.replace({'unit':'kg'},'lb',inplace=True)
    df.replace({'unit':'ug/l'},'mg/l',inplace=True)
    df.replace({'unit':'deg c'},'degF',inplace=True)

    # df['unit'].replace('kg','lb',inplace=True)
    # df['unit'].replace('ug/l','mg/l',inplace=True)
    # df['unit'].replace('deg c','degF',inplace=True)
    df['data_type'] = 'discrete'
    df['data_format'] = 'instantaneous'
    df.set_index('datetime',drop=True,inplace=True)
    df.index = df.index.tz_localize('UTC-06:00')
    
    df.index = df.index.round('h').round('h')
    df = df.reset_index()
    df = df.groupby(['datetime','variable','unit','station_id','station_name','constituent','data_format','data_type','station_origin']).mean()
    df = df.reset_index()
    df = df.set_index('datetime')
    df['quality_id'] = pd.NA
    df['station_name'] = station_name
    return df

def load(df,file_path):
    '''
    date, time, value, variable, unit, station_id, station_name, constituent, station_origin, data_format, data_type, quality_code, 
    
    
    
    '''


    df.to_csv(file_path)      
    
    


# base_url = 'https://webapp.pca.state.mn.us/surface-water/search?'


# https://services.pca.state.mn.us/api/v1/surfacewater/monitoring-stations/results?


# dataType
# geographicType
# specificGeoAreaCode
# wuType
# stationType
# stationId


        
# CONSTITUENT_MAP = {'TSS': ['Total suspended solids'],
#                 'TKN': ['Kjeldahl nitrogen as N','Nitrogen, Total Kjeldahl (TKN) as N'],
#                 'N'  :  ['Nitrate + Nitrite Nitrogen, Total as N','Nitrate/Nitrite as N (N+N) as N'],
#                 'TP' :  ['Phosphorus, Total as P as P'],
#                 'BOD': ['Carbonaceous biochemical oxygen demand, standard conditions',
#                                 'Chemical oxygen demand'],
#                 'CHLA': ['Chlorophyll a, corrected for pheophytin',
#                               'Chlorophyll-A',
#                               'Chlorophyll-a, Pheophytin Corrected'],
#                 'Q': ['Flow']}
