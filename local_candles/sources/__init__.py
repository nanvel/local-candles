from .base import Source
from .binance_usdm_futures_ohlc import BinanceUSDMFuturesOHLCSource
from .binance_spot_ohlc import BinanceSpotOHLCSource
from .csv_file import CsvFileSource
from .gateio_spot_ohlc import GateioSpotOHLCSource
from .yahoo_finance_ohlc import YahooFinanceOHLCSource


SOURCES = {
    "binance_usdm_futures_ohlc": BinanceUSDMFuturesOHLCSource,
    "binance_spot_ohlc": BinanceSpotOHLCSource,
    "csv_file": CsvFileSource,
    "gateio_spot_ohlc": GateioSpotOHLCSource,
    "yahoo_finance_ohlc": YahooFinanceOHLCSource,
}


__all__ = ("Source", "SOURCES")
