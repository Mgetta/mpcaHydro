from mpcaHydro.sources import wiski, equis
from pathlib import Path
from typing import List
from mpcaHydro.warehouse import storage, database
import baseflow


def download_wiski_data(
    station_ids: List[str],
    data_dir: Path,
    start_year: int = 1996,
    end_year: int = 2030,
    wplmn: bool = False,
    replace: bool = False
) -> None:
    """Download WISKI data for the given stations and save to staging, deduplicating against existing data."""
    
    for station_id in station_ids:
        df_new = wiski.download([station_id], start_year=start_year, end_year=end_year, wplmn=wplmn)
        if not df_new.empty:
            if replace:
                storage.drop_stations([station_id], data_dir, source='wiski')
            storage.save_staging(df_new, data_dir, 'wiski', station_id) 

        
def download_equis_data(
    station_ids: List[str],
    data_dir: Path,
    replace: bool = False,
    oracle_user: str = None,
    oracle_password: str = None,
    oracle_host: str = 'DELTAT'
) -> None:
    """Download EQUIS data for the given stations and save to staging, deduplicating against existing data."""
    df_equis = equis.download(station_ids, oracle_user=oracle_user, oracle_password=oracle_password, oracle_host=oracle_host)
    if df_equis.empty:
        print("No data downloaded")
    else:
        for station_id in df_equis['SYS_LOC_CODE'].unique(): 
            df_new = df_equis[df_equis['SYS_LOC_CODE'] == station_id]
            if not df_new.empty:
                if replace:
                    storage.drop_stations([station_id], data_dir, source='equis')     
                storage.save_staging(df_new, data_dir, 'equis', station_id)



def compute_baseflow(df, data_dir: Path, method='Boughton', min_size=30):
    """Compute baseflow from hourly Q data and store in derived."""
    for station_id in df['station_id'].unique():
        df_q_station = df.loc[df['station_id'] == station_id]
        if not df_q_station.empty and len(df_q_station) >= min_size:    
            # Run the Python algorithm
            print(len(df_q_station))
            bf = baseflow.separation(df_q_station.set_index('datetime')[['value']], method=method)
            df_q_station.loc[:,'value'] = bf[method].values # ensure numeric for transformations
            df_q_station = df_q_station.dropna(subset=['value']) # drop rows where baseflow couldn't be computed
            df_q_station = df_q_station[['datetime','date','time','value','station_id','unit','grain','statistic','interval_minutes']].copy()
            df_q_station['constituent'] = 'QB'  # baseflow constituent
            df_q_station['station_origin'] = 'wiski'
            df_q_station['date'] = df_q_station['datetime'].dt.date
            # Store results with same schema as staging
            storage.save_baseflow(df_q_station, data_dir, station_id)
        else:
            print(f"Station {station_id} has insufficient data for baseflow separation (n={len(df_q_station)})")
