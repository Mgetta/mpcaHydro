# -*- coding: utf-8 -*-
"""SQL file loader utilities for loading SQL from separate files."""

from pathlib import Path
from typing import Optional

# Path to the SQL folder
SQL_DIR = Path(__file__).parent / 'sql'


def load_sql(filename: str) -> str:
    """Load SQL content from a file in the sql folder."""
    filepath = SQL_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"SQL file not found: {filepath}")
    return filepath.read_text(encoding='utf-8')


def get_schemas_sql() -> str:
    """Load SQL for creating database schemas."""
    return load_sql('schemas.sql')


def get_staging_tables_sql() -> str:
    """Load SQL for creating staging tables."""
    return load_sql('staging_tables.sql')


def get_analytics_tables_sql() -> str:
    """Load SQL for creating analytics tables."""
    return load_sql('analytics_tables.sql')


def get_outlets_schema_sql() -> str:
    """Load SQL for creating outlets schema and tables."""
    return load_sql('outlets_schema.sql')


def get_views_analytics_sql() -> str:
    """Load SQL for creating analytics views."""
    return load_sql('views_analytics.sql')


def get_views_reports_sql() -> str:
    """Load SQL for creating reports views."""
    return load_sql('views_reports.sql')


def get_views_outlets_sql() -> str:
    """Load SQL for creating outlets views."""
    return load_sql('views_outlets.sql')


def get_calibration_schema_sql() -> str:
    """Load SQL for creating calibration schema (SQLite)."""
    return load_sql('calibration_schema.sql')

def get_transforms_wiski_sql() -> str:
    """Load SQL for transforming WISKI data."""
    return load_sql('transforms_wiski.sql')

def get_transforms_equis_sql() -> str:
    """Load SQL for transforming EQUIS data."""
    return load_sql('transforms_equis.sql')

def get_transforms_baseflow_sql() -> str:
    """Load SQL for transforming baseflow data."""
    return load_sql('transforms_baseflow.sql')

def get_derived_tables_sql() -> str:
    """Load SQL for creating derived tables (baseflow)."""
    return load_sql('derived_tables.sql')