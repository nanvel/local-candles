import datetime
from pathlib import Path

import pytest
from pandas import DataFrame

from local_candles.data import Data
from local_candles.sources import Source
from local_candles.models import Time

from . import DATA_PATH


class FakeOHLCSource(Source):
    def __init__(self, raise_error=False):
        self.raise_error = raise_error

    @property
    def slug(self):
        return "fake_ohlc/btcusdt_1h"

    def load(self, ts: Time, path: Path) -> (Time, Time, bool):
        if self.raise_error:
            raise AssertionError("Should not be called!")

        return (
            Time(1578600000),
            Time(1580396400),
            True,
        )


def test_use_cached():
    cache_path = DATA_PATH / "sources_cache"
    service = Data(cache_root=cache_path)
    source = FakeOHLCSource(raise_error=True)

    result = service.load_df(
        source=source,
        start_ts=Time.from_datetime(
            datetime.datetime(2020, 1, 1),
        ),
        stop_ts=Time.from_datetime(
            datetime.datetime(2020, 1, 2),
        ),
        columns=["open", "high", "low", "close"],
    )

    assert isinstance(result, DataFrame)
    assert result.shape == (1, 4)


def test_load():
    cache_path = DATA_PATH / "sources_cache"
    service = Data(cache_root=cache_path)
    source = FakeOHLCSource()

    with pytest.raises(FileNotFoundError) as e:
        service.load_df(
            source=source,
            start_ts=Time.from_datetime(
                datetime.datetime(2020, 1, 1),
            ),
            stop_ts=Time.from_datetime(
                datetime.datetime(2020, 1, 15),
            ),
            columns=["open", "high", "low", "close"],
        )

    assert "fake_ohlc/btcusdt_1h/1579494600.csv" in str(e.value)
    assert "fake_ohlc/btcusdt_1h/1578600000_1580396400.csv" in str(e.value)
