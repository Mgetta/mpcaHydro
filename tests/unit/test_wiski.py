# -*- coding: utf-8 -*-
"""Unit tests for wiski — only logic that can silently produce wrong results."""

import pytest
import pandas as pd
from mpcaHydro.sources.wiski import convert_units, download


class TestConvertUnits:
    """Unit conversions are invisible when wrong — worth testing explicitly."""

    def test_celsius_to_fahrenheit(self):
        df = pd.DataFrame({'ts_unitsymbol': ['°C', '°C'], 'Value': [0.0, 100.0]})
        result = convert_units(df)

        assert result['ts_unitsymbol'].iloc[0] == 'degf'
        assert result['Value'].iloc[0] == pytest.approx(32.0)
        assert result['Value'].iloc[1] == pytest.approx(212.0)

    def test_kg_to_lb(self):
        df = pd.DataFrame({'ts_unitsymbol': ['kg'], 'Value': [1.0]})
        result = convert_units(df)

        assert result['ts_unitsymbol'].iloc[0] == 'lb'
        assert result['Value'].iloc[0] == pytest.approx(2.20462)

    def test_cfs_rename(self):
        df = pd.DataFrame({'ts_unitsymbol': ['ft³/s'], 'Value': [42.0]})
        result = convert_units(df)

        assert result['ts_unitsymbol'].iloc[0] == 'cfs'
        assert result['Value'].iloc[0] == 42.0

    def test_unknown_units_unchanged(self):
        df = pd.DataFrame({'ts_unitsymbol': ['mg/l'], 'Value': [5.0]})
        result = convert_units(df)

        assert result['ts_unitsymbol'].iloc[0] == 'mg/l'
        assert result['Value'].iloc[0] == 5.0


def test_invalid_constituent_raises():
    with pytest.raises(ValueError, match='Invalid constituent'):
        download(['H67014001'], constituent='INVALID')


def test_non_string_station_raises():
    with pytest.raises(ValueError, match='not a string'):
        download([12345], constituent='Q')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])