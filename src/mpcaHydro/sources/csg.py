# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:13:23 2023

@author: mfratki
"""

import pandas as pd
import requests
import zipfile
import io
# import geopandas as gpd


CONSITUENT_MAP = {'Water Temp. (C)': 'WT',
                 'Discharge (cfs)': 'Q',
                 'DO (mg/L)': 'DO'
    }

# def download(station_no):
#     # save_path = Path(save_path)
#     # file_path = save_path.joinpath('csg.csv')

#     station = station_no[1:]
#     df = pd.read_csv(f'https://maps2.dnr.state.mn.us/cgi-bin/csg.cgi?mode=dump_hydro_data_as_csv&site={station}&startdate=1996-1-1&enddate=2050-1-1')
#     df = pd.read_csv(f'https://apps.dnr.state.mn.us/csg/api/v1/download?callback=json&ids=66050001&vars=262')
#     df['station_id'] = station_no

#     return df

def download(station_no):
    station = station_no[1:]
    url = f'https://apps.dnr.state.mn.us/csg/api/v1/download?ids={station}&vars=262'
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        df = pd.read_csv(zip_ref.open(zip_ref.namelist()[0]))
        df['station_id'] = station_no
      
    return df
      

    # def process(df):
    #     
    #     df.set_index('Timestamp',inplace=True)
    #     value_variables = [column for column in df.columns if (column not in ['Site','Timestamp','station_no']) & ~(column.endswith('Quality'))]
        
    #     test = df[value_variables].resample(rule='1H', kind='interval').mean().dropna()
    #     df = df['Value'].resample(rule='1H', kind='interval').mean().to_frame()
        
def transform(data):
    data.rename(columns = {'tstamp': 'datetime',
                                'var_name': 'variable',
                                'station_no': 'station_id'}, inplace = True)

    data['unit'] = data['variable'].map({'Water Temp. (C)' : 'C',
                                                   'Discharge (cfs)' : 'cfs',
                                                   'DO (mg/L)' : 'mg/L'})   
    
    data['constituent'] = data['variable'].map({'Water Temp. (C)' : 'WT',
                                                          'Discharge (cfs)' : 'Q',
                                                          'DO (mg/L)' : 'DO'})
  
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.set_index('datetime',drop=True,inplace=True)
    data.index = data.index.tz_localize('UTC-06:00')
    data.dropna(subset = 'value',inplace=True)
    data['source'] = 'csg'
    return data





def load(data,file_path):
    
    data.to_csv(file_path)



        