# -*- coding: utf-8 -*-
"""Unit tests for equis — logic that can silently produce wrong results.

Three areas justify unit tests here:
1. Unit conversions (ug/l→mg/l, °C→°F) — wrong math is invisible downstream
2. Timezone conversion (CST/CDT/UTC→UTC-6) — wrong timestamps corrupt time-series joins
3. CAS_RN mapping — wrong mapping silently mislabels constituents
"""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from mpcaHydro.sources.equis import (
    convert_units,
    as_utc_offset,
    map_constituents,
    make_placeholders,
    normalize_timezone,
    replace_nondetects,
    CAS_RN_MAP,
)


# ── convert_units ───────────────────────────────────────────────────

class TestConvertUnits:

    def test_ug_l_to_mg_l(self):
        df = pd.DataFrame({'unit': ['ug/l'], 'value': [1000.0]})
        result = convert_units(df)

        assert result['unit'].iloc[0] == 'mg/l'
        assert result['value'].iloc[0] == pytest.approx(1.0)

    def test_mg_g_to_mg_l(self):
        df = pd.DataFrame({'unit': ['mg/g'], 'value': [1.0]})
        result = convert_units(df)

        assert result['unit'].iloc[0] == 'mg/l'
        assert result['value'].iloc[0] == pytest.approx(1000.0)

    def test_celsius_to_fahrenheit(self):
        df = pd.DataFrame({'unit': ['deg c'], 'value': [0.0]})
        result = convert_units(df)

        assert result['unit'].iloc[0] == 'degf'
        assert result['value'].iloc[0] == pytest.approx(32.0)

    def test_degc_variant(self):
        """EQuIS uses both 'deg c' and 'degc' — both should convert."""
        df = pd.DataFrame({'unit': ['degc'], 'value': [100.0]})
        result = convert_units(df)

        assert result['unit'].iloc[0] == 'degf'
        assert result['value'].iloc[0] == pytest.approx(212.0)

    def test_mg_l_unchanged(self):
        df = pd.DataFrame({'unit': ['mg/l'], 'value': [5.0]})
        result = convert_units(df)

        assert result['unit'].iloc[0] == 'mg/l'
        assert result['value'].iloc[0] == 5.0


# ── (timezone conversion) ─────────────────────────────

class TestTimezoneConversion:

    df = pd.DataFrame({
        'SAMPLE_DATE_TIME': [datetime(2020, 7, 1, 14, 0),
                            datetime(2020, 7, 1, 14, 0),
                            datetime(2020, 7, 1, 14, 0)],
        'SAMPLE_DATE_TIMEZONE': ['CST', 'CDT', 'UTC']
    })

    result = normalize_timezone(df)

    def test_cst_unchanged(self):
        """CST is already UTC-6, so the time should not shift."""
        assert self.result['datetime'].iloc[0].hour == 14

    def test_cdt_shifts_minus_one(self):
        """CDT is UTC-5. Converting to UTC-6 should subtract one hour."""
        assert self.result['datetime'].iloc[1].hour == 13

    def test_utc_shifts_minus_six(self):
        """UTC to UTC-6 should subtract six hours."""
        assert self.result['datetime'].iloc[2].hour == 8

# ── map_constituents (CAS_RN mapping) ───────────────────────────────

def test_cas_rn_maps_key_constituents():
    """Spot-check that the critical CAS codes map to the right names."""
    df = pd.DataFrame({'CAS_RN': ['7723-14-0', 'SOLIDS-TSS', 'NO2NO3', 'TEMP-W']})
    result = map_constituents(df)
    assert list(result['constituent']) == ['TP', 'TSS', 'N', 'WT']


def test_unmapped_cas_rn_becomes_nan():
    """An unknown CAS code should map to NaN, not silently pass through."""
    df = pd.DataFrame({'CAS_RN': ['UNKNOWN-CODE']})
    result = map_constituents(df)
    assert pd.isna(result['constituent'].iloc[0])


# ── replace_nondetects ──────────────────────────────────────────────

def test_nondetects_replaced_with_zero():
    df = pd.DataFrame({'value': [1.0, float('nan'), 3.0, float('nan')]})
    result = replace_nondetects(df)

    assert result['value'].isna().sum() == 0
    assert result['value'].iloc[1] == 0.0
    assert result['value'].iloc[0] == 1.0  # real values untouched


# ── make_placeholders ──────────────────────────────────────────────

def test_make_placeholders():
    placeholders, binds = make_placeholders(['S002-118', 'S004-880'])

    assert placeholders == ':id0, :id1'
    assert binds == {'id0': 'S002-118', 'id1': 'S004-880'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])