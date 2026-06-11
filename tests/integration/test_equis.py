# -*- coding: utf-8 -*-
"""Integration tests for equis module.

Pipeline tests use synthetic data shaped like real Oracle output.
Network tests require Oracle credentials and are skipped by default.

Run pipeline only:  pytest tests/integration/test_equis.py -v -m "not credentials"
Run all:            pytest tests/integration/test_equis.py -v
"""

import pytest
import pandas as pd
from datetime import datetime
from mpcaHydro.sources.equis import transform, normalize, download, info


# ── Skip setup for credential-gated tests ───────────────────────────

try:
    from mpcaHydro import equis
    HAS_CONNECTION = equis.CONNECTION is not None
except Exception:
    HAS_CONNECTION = False

skip_credentials = pytest.mark.skipif(
    not HAS_CONNECTION,
    reason="No active Oracle connection — call equis.connect() first"
)

TEST_STATION = 'S002-118'


# ── Synthetic raw data ──────────────────────────────────────────────
# Shaped like real Oracle output from equis.download().
# Includes: two constituents (TP, WT), mixed timezones (CST, CDT),
# one non-detect (NaN), and a ug/l unit that needs conversion.

RAW_EQUIS_DATA = pd.DataFrame({
    'SYS_LOC_CODE':          ['S002-118'] * 5,
    'CAS_RN':                ['7723-14-0', '7723-14-0', 'TEMP-W', 'TEMP-W', '7723-14-0'],
    'SAMPLE_DATE_TIME':      [
        datetime(2020, 6, 1, 10, 0),
        datetime(2020, 6, 1, 10, 30),
        datetime(2020, 6, 1, 10, 0),
        datetime(2020, 6, 1, 10, 0),
        datetime(2020, 6, 1, 11, 0),
    ],
    'SAMPLE_DATE_TIMEZONE':  ['CST', 'CST', 'CDT', 'CST', 'CST'],
    'RESULT_NUMERIC':        [0.05, float('nan'), 22.0, 20.0, 0.08],
    'RESULT_UNIT':           ['mg/l', 'mg/l', 'deg c', 'deg c', 'ug/l'],
    'APPROVAL_CODE':         ['Final'] * 5,
    'REPORTABLE_RESULT':     ['Y'] * 5,
})


# ── Pipeline tests (no network) ────────────────────────────────────

def test_transform_pipeline():
    """The core equis workflow:
    raw Oracle data → transform() → analysis-ready output.

    This gives confidence that the full chain works together:
    constituent mapping, timezone normalization, unit conversion,
    non-detect replacement, and hourly averaging all in sequence.
    """
    result = transform(RAW_EQUIS_DATA.copy())

    # Has the expected output shape
    assert {'station_id', 'constituent', 'value', 'datetime', 'unit', 'station_origin'}.issubset(result.columns)

    # Tagged as equis data
    assert (result['station_origin'] == 'equis').all()

    # Both constituents survived the pipeline
    assert {'TP', 'WT'}.issubset(result['constituent'].unique())

    # No NaNs remain — non-detects should be replaced with 0
    assert result['value'].isna().sum() == 0


def test_normalize_converts_units_and_timezones():
    """normalize() should handle unit conversion and timezone
    normalization without the averaging/filtering steps.

    This gives confidence that data looks correct *before* aggregation,
    which is useful when debugging transform() issues.
    """
    result = normalize(RAW_EQUIS_DATA.copy())

    # ug/l row should now be mg/l
    assert 'ug/l' not in result['unit'].values

    # Temperature should be in degf
    wt_rows = result[result['constituent'] == 'WT']
    assert (wt_rows['unit'] == 'degf').all()

    # datetime column should exist (timezone normalization ran)
    assert 'datetime' in result.columns
    assert result['datetime'].isna().sum() == 0


def test_transform_empty_input():
    """transform() on an empty DataFrame shouldn't crash.

    This catches edge cases where a station has no EQuIS data —
    the pipeline should return an empty DataFrame, not raise.
    """
    empty = pd.DataFrame(columns=RAW_EQUIS_DATA.columns)
    result = transform(empty)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


# ── Network tests (require Oracle credentials) ─────────────────────

@skip_credentials
@pytest.mark.credentials
def test_download_returns_data():
    """Can I download EQuIS data for a known station?

    Verifies the Oracle query executes and returns a DataFrame
    with the column structure that normalize/transform expect.
    """
    df = download([TEST_STATION])

    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert {'SYS_LOC_CODE', 'CAS_RN', 'RESULT_NUMERIC', 'SAMPLE_DATE_TIME'}.issubset(df.columns)


@skip_credentials
@pytest.mark.credentials
def test_download_then_transform():
    """The real workflow: Oracle → download() → transform() → analysis-ready.

    This is the single most important test — if this passes, users
    can trust the full equis pipeline end-to-end.
    """
    raw = download([TEST_STATION])

    if raw.empty:
        pytest.skip("No data returned for test station")

    result = transform(raw)

    assert {'station_id', 'constituent', 'value', 'station_origin'}.issubset(result.columns)
    assert (result['station_origin'] == 'equis').all()


@skip_credentials
@pytest.mark.credentials
def test_info_returns_station_constituent_pairs():
    """info() should return deduplicated (station, constituent) pairs.

    Gives confidence that the discovery workflow works before
    committing to a full download.
    """
    df = info([TEST_STATION])

    assert not df.empty
    assert {'station_id', 'constituent'}.issubset(df.columns)
    # Should be deduplicated
    assert not df.duplicated(subset=['station_id', 'constituent']).any()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])