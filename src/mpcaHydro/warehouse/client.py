from mpcaHydro import outlets
from mpcaHydro.warehouse import database, queries, export, pipeline
from pathlib import Path
from typing import List, Optional, Union
import pandas as pd


class DataManagerWrapper:
    """Convenience wrapper that manages DuckDB connections for warehouse operations.

    ``DataManagerWrapper`` provides the same functionality as the
    module-level procedural functions, but bundles them behind a single
    object that owns the database path and creates context-managed
    connections on every call.  This removes the need for callers to open
    and close connections manually.

    **When to use DataManagerWrapper:**

    * Interactive exploration in a Jupyter notebook where you want a
      persistent handle to the warehouse.
    * Application code that performs many sequential operations on the
      same database.

    **When to use the procedural functions instead:**

    * When you already have an open ``duckdb.DuckDBPyConnection`` (e.g.
      inside a ``with`` block).
    * When you need fine-grained control over transaction boundaries.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB warehouse file.
    reset : bool, default False
        If ``True``, re-initialise the database (delete and recreate).

    Attributes
    ----------
    db_path : pathlib.Path
        Resolved path to the DuckDB file.

    Examples
    --------
    >>> dm = DataManagerWrapper('observations.duckdb', reset=True)
    >>> dm.download_wiski_data(['E66050001'])
    >>> df = dm.get_observation_data(['E66050001'], 'Q', agg_period='D')
    """
    
    def __init__(self, data_dir: Union[str, Path], reset: bool = False):
        """Initialise the wrapper with a database path.

        Parameters
        ----------
        db_path : str or Path
            Path to the DuckDB warehouse file.
        reset : bool, default False
            Re-initialise the database when ``True``.
        """

        self.con = database.create_session(Path(data_dir).as_posix())
        self.data_dir = Path(data_dir)
        if reset:
            raise NotImplementedError('Reset functionality not implemented yet')

    def _refresh_views(self):
        """Refresh all database views."""
        database._refresh_staging_views(self.con, self.data_dir)
        database._refresh_derived_views(self.con, self.data_dir)
        database.update_views(self.con)


    def update_views(self) -> None:
        """Refresh all analytics and reports views."""
        database.update_views(self.con)
    
    def wiski_qc_counts(self):
        """Return WISKI quality-code frequency counts.

        See :func:`wiski_qc_counts` for details.
        """
        return queries.wiski_qc_counts(self.con)

        
    def station_summary(self, constituent: str = None):
        """Return per-station constituent summary statistics.

        See :func:`station_summary` for details.
        """
        return queries.station_summary(self.con, constituent)
        
    def station_reach_pairs(self):
        """Return all station-reach pair records.

        See :func:`station_reach_pairs` for details.
        """
        return queries.station_reach_pairs(self.con)

        
    def outlet_summary(self):
        """Return outlet-level constituent summary.

        See :func:`outlet_summary` for details.
        """
        return queries.outlet_summary(self.con)
        
    def download_wiski_data(
        self,
        station_ids: List[str],
        start_year: int = 1996,
        end_year: int = 2030,
        replace: bool = False
    ) -> None:
        """Download WISKI data and load into the warehouse.

        See :func:`download_wiski_data` for details.
        """
        pipeline.download_wiski_data(station_ids, self.data_dir, start_year, end_year, replace = replace)
        self._refresh_views()
    
    def compute_baseflow(self, method='Boughton', min_size=30) -> None:
        """Compute baseflow from hourly Q data and store in derived.

        See :func:`compute_baseflow` for details.
        """
        df = self.con.execute('''SELECT * FROM analytics.observations WHERE constituent = 'Q' AND grain = 'continuous'
                              ''').fetchdf()
        pipeline.compute_baseflow(df, self.data_dir, method, min_size)
        self._refresh_views()

    def set_active_quality_codes(self, data_codes: Optional[List[int]] = None, reset: bool = False) -> None:
        """Set the WISKI quality-code filtering options for analytics views.

        See :func:`set_active_quality_codes` for details.
        """
        database.set_active_quality_codes(self.con, data_codes, reset)
    
    def set_active_sample_methods(self, sample_methods: Optional[List[str]] = None, reset: bool = False) -> None:
        """Set the EQuIS sample-method filtering options for analytics views.

        See :func:`set_active_sample_methods` for details.
        """
        database.set_active_sample_methods(self.con, sample_methods, reset)

    def download_equis_data(
        self,
        station_ids: List[str],
        replace: bool = False,
        oracle_username: str = None,
        oracle_password: str = None,
        oracle_host: str = 'DELTAT'
    ) -> None:
        """Download EQuIS data and load into the warehouse.

        See :func:`download_equis_data` for details.
        """
        pipeline.download_equis_data(station_ids, self.data_dir, replace, oracle_username, oracle_password, oracle_host)
        self._refresh_views()
        
    def get_outlets(self, model_name: str) -> pd.DataFrame:
        """Get outlet station-reach pairs for a model.

        See :func:`get_outlets` for details.
        """
        return queries.get_outlets(self.con, model_name)
    
    def get_outlets_by_reach(self, reach_id: int,model_name: str) -> pd.DataFrame:
        """Return outlet rows containing *reach_id* in this model."""
        return queries.get_outlets_by_reach(self.con, reach_id, model_name)

    def get_outlets_by_station(self, station_id: str, station_origin: str):
        """Return outlet rows for *station_id* (must belong to this model).

        Raises
        ------
        AssertionError
            If *station_id* is not found in this model's station lists.
        """
        return queries.get_outlets_by_station(self.con, station_id, station_origin)

    def get_outlet_opnids(self, outlet_id: int):
        """Return unique reach IDs for the given outlet."""
        return queries.get_outlet_opnids(self.con, outlet_id)
    
    def get_outlet_stations(self, outlet_id: int):
        """Return station IDs and origins for the given outlet."""
        return queries.get_outlet_stations(self.con, outlet_id)


    def get_station_ids(self, station_origin: Optional[str] = None) -> List[str]:
        """Get station IDs, optionally filtered by origin.

        See :func:`get_station_ids` for details.
        """
        return queries.get_station_ids(self.con, station_origin)

    def get_wplmn_stations(self, model_name: str) -> List[str]:
        """Get WPLMN station IDs for a model.

        See :func:`get_wplmn_stations` for details.
        """
        return outlets.wplmn_stations(self.con, model_name)

    def get_all_stations(self,station_origin,model_name):
        """Get all station ID within the spatial bounds of a model."""
        if station_origin == 'wiski':
            stations = outlets.wiski_stations(self.con, model_name)
        elif station_origin == 'equis':
            stations = outlets.equis_stations(self.con, model_name)
        else:
            raise ValueError("station_origin must be 'wiski' or 'equis'")
        return stations
    
    def get_mapped_stations(self, station_origin: str, model_name : str) -> List[str]:
        """Get station IDs that are mapped to reaches for a specific model and station origin.
        """
        if station_origin == 'wiski':
            stations = outlets.mapped_wiski_stations(self.con,model_name)
        elif station_origin == 'equis':
            stations = outlets.mapped_equis_stations(self.con,model_name)
        else:
            raise ValueError("station_origin must be 'wiski' or 'equis'")
        return stations
    
    def get_observation_data(
        self,
        station_ids: List[str],
        constituent: str,
        agg_period: Optional[str] = None
    ) -> pd.DataFrame:
        """Get observation data for given stations and constituent.

        See :func:`get_observation_data` for details.
        """
        return queries.get_observation_data(self.con, station_ids, constituent, agg_period)
    
    def get_outlet_data(
        self,
        outlet_id: int,
        constituent: str,
        agg_period: str = 'D'
    ) -> pd.DataFrame:
        """Get outlet observations with paired flow and baseflow.

        See :func:`get_outlet_data` for details.
        """
        return queries.get_outlet_data(self.con, outlet_id, constituent, agg_period)
    
    def get_station_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get all analytics observations for a station.

        See :func:`get_station_data` for details.
        """
        return queries.get_station_data(self.con, station_id, station_origin)
    
    def get_raw_data(self, station_id: str, station_origin: str) -> pd.DataFrame:
        """Get raw staging data for a station.

        See :func:`get_raw_data` for details.
        """
        return queries.get_raw_data(self.con, station_id, station_origin)
    
    def get_constituent_summary(self) -> pd.DataFrame:
        """Get constituent summary across all stations.

        See :func:`get_constituent_summary` for details.
        """
        return queries.get_constituent_summary(self.con)
    
    def export_station_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path] = None
    ) -> None:
        """Export analytics data for a station to CSV.

        See :func:`export_station_to_csv` for details.
        """
        if output_path is None:
            output_path = dm.data_dir
        export.export_station_to_csv(self.con, station_id, station_origin, output_path)
    
    def export_raw_to_csv(
        self,
        station_id: str,
        station_origin: str,
        output_path: Union[str, Path] = None
    ) -> None:
        """Export raw staging data for a station to CSV.

        See :func:`export_raw_to_csv` for details.
        """
        if output_path is None:
            output_path = dm.data_dir
        export.export_raw_to_csv(self.con, station_id, station_origin, output_path)
    
    def get_equis_template(self) -> pd.DataFrame:
        """Get an empty DataFrame matching the ``staging.equis`` schema.

        See :func:`get_equis_template` for details.
        """
        return queries.get_equis_template(self.con)
    
    def get_wiski_template(self) -> pd.DataFrame:
        """Get an empty DataFrame matching the ``staging.wiski`` schema.

        See :func:`get_wiski_template` for details.
        """
        return queries.get_wiski_template(self.con)