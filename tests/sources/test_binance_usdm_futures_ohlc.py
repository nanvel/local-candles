import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from local_candles.models import Interval, Time
from local_candles.sources import BinanceUSDMFuturesOHLCSource


@pytest.fixture(scope="module")
def source():
    source = BinanceUSDMFuturesOHLCSource(
        symbol="BTCUSDT", interval=Interval.from_binance_slug("1h")
    )
    _request = Mock(
        return_value=[
            [
                1639800000000,
                "46267.45",
                "46489.78",
                "46046.01",
                "46380.01",
                "7049.653",
                1639803599999,
                "326182417.82580",
                "107821",
                "3518.489",
                "162802257.00324",
            ],
            [
                1641596400000,
                "41522.50",
                "41586.38",
                "41300.00",
                "41553.86",
                "8090.483",
                1641599999999,
                "335627496.03252",
                "78648",
                "4122.840",
                "171062238.49093",
            ],
        ]
    )
    _write = Mock()

    with patch.object(source, "_request", _request):
        with patch.object(source, "_write", _write):
            yield source


def test_slug(source):
    assert source.slug == "binance_usdm_futures_ohlc/btcusdt_1h"


def test_request(source):
    ts = Time.from_datetime(datetime.datetime(2022, 1, 1))
    first_ts, last_ts, completed = source.load(ts=ts, path=Path("/tmp/"))

    assert first_ts == 1639800000
    assert last_ts == 1641596400
    assert completed is False

    source._request.assert_called_once()
    source._write.assert_called_once()
