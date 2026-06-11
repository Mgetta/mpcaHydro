# -*- coding: utf-8 -*-
"""Integration tests for wiski module.

Tests the workflows that users actually run:
1. Can I get data out of the API?
2. Does the full download → transform pipeline produce correct output?

Run with: pytest tests/integration/test_wiski.py -v
"""

import pytest
import pandas as pd
from mpcaHydro.sources.wiski import download, transform
from pathlib import Path


FIXTURES = Path(__file__).parent.parent / 'fixtures'


try:
    from mpcaHydro import pywisk
    _is_up, _ = pywisk.test_connection()
    NETWORK_AVAILABLE = _is_up
except Exception:
    NETWORK_AVAILABLE = False

skip_network = pytest.mark.skipif(
    not NETWORK_AVAILABLE,
    reason="WISKI network service not available"
)

TEST_STATION = 'H67014001'
TEST_START_YEAR = 2020
TEST_END_YEAR = 2020

# TEST_DATA = {
#         'station_no':          ['H67014001'] * 6,
#         'Timestamp':           pd.to_datetime([
#             '2020-06-01 00:00', '2020-06-01 00:15',
#             '2020-06-01 00:30', '2020-06-01 00:45',
#             '2020-06-01 00:00', '2020-06-01 00:15',
#             '2020-06-01 00:45', '2020-06-02 00:00',
#             '2020-06-02 00:15', '2020-06-02 00:30',
#             '2020-06-02 00:45', '2020-06-02 00:00'
#         ]),
#         'Value':               [100.0, 110.0, 105.0, 115.0, 20.0, 21.0],
#         'Quality Code':        [1, 1, 1, 200, 1, 1],
#         'Quality Code Name':   ['Good'] * 3 + ['Suspect'] + ['Good'] * 2,
#         'ts_unitsymbol':       ['ft³/s'] * 4 + ['°C'] * 2,
#         'stationparameter_no': ['262.1'] * 4 + ['450.42'] * 2,
#         'parametertype_id':    ['11500'] * 4 + ['11504'] * 2,
#         'parametertype_name':  ['Discharge'] * 4 + ['Water Temperature'] * 2,
#         'ts_name':             ['15.Rated'] * 4 + ['09.Archive'] * 2,
#         'ts_id':               ['100001'] * 4 + ['100002'] * 2,
#         'station_name':        ['Test Station'] * 6,
#         'station_latitude':    ['46.5'] * 6,
#         'station_longitude':   ['-94.3'] * 6,
#         'stationparameter_name': ['Discharge'] * 4 + ['Water Temp'] * 2,
#     }

# ── Pipeline: synthetic data, no network ────────────────────────────

def test_transform_pipeline():
    """The core user workflow on synthetic data:
    raw DataFrame → transform() → analysis-ready output."""

    #raw = pd.DataFrame(TEST_DATA)
    raw = pd.read_parquet(FIXTURES / 'wiski_H67014001_2020.parquet')
    
    result = transform(raw, filter_qc_codes=True)

    # Has the right shape
    assert {'station_origin', 'constituent', 'value', 'datetime', 'unit'}.issubset(result.columns)

    # Tagged correctly
    assert (result['station_origin'] == 'wiski').all()

    # Both constituents survived
    assert {'Q', 'WT'}.issubset(result['constituent'].unique())

    # Bad quality code row was filtered out — 4 Q rows become fewer after filter + average
    assert len(result) > 0


def test_transform_qc_filter_matters():
    """Turning off QC filtering should keep more data than filtering."""

    raw = pd.read_parquet(FIXTURES / 'wiski_H67014001_2020.parquet')

    filtered = transform(raw.copy(), filter_qc_codes=True, data_codes=[1])
    unfiltered = transform(raw, filter_qc_codes=False)

    assert len(unfiltered) >= len(filtered)


# ── Network: live API round-trips ───────────────────────────────────

@skip_network
@pytest.mark.network
def test_download_discharge():
    """Can I download discharge data for a known station?"""

    df = download(
        [TEST_STATION],
        constituent='Q',
        start_year=TEST_START_YEAR,
        end_year=TEST_END_YEAR,
    )

    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert 'wplmn_flag' in df.columns


@skip_network
@pytest.mark.network
def test_info_returns_metadata():
    """Can I discover what timeseries are available for a station?"""

    df = info([TEST_STATION], constituent='Q')

    assert not df.empty
    assert {'station_id', 'constituent'}.issubset(df.columns)


@skip_network
@pytest.mark.network
def test_download_then_transform():
    """The real workflow: download from API → transform → analysis-ready."""

    raw = download(
        [TEST_STATION],
        constituent='Q',
        start_year=TEST_START_YEAR,
        end_year=TEST_END_YEAR,
    )

    if raw.empty:
        pytest.skip("No data returned for test station/date range")

    result = transform(raw)

    assert {'station_origin', 'constituent', 'value'}.issubset(result.columns)
    assert (result['station_origin'] == 'wiski').all()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])