from unittest import mock

import pytest

from local_candles.sources import YahooFinanceOHLCSource


@pytest.fixture(scope="module")
def source():
    with mock.patch(
        "local_candles.sources.yahoo_finance_ohlc.YFINANCE_INSTALLED", True
    ):
        return YahooFinanceOHLCSource(symbol="SPY", interval="1d")


def test_slug(source):
    assert source.slug == "yahoo_finance_ohlc/spy_1d"
