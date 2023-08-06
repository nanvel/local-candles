# Local candles

[![PyPI version](https://badge.fury.io/py/local-candles.svg)](https://badge.fury.io/py/local-candles)
[![Python Versions](https://img.shields.io/pypi/pyversions/local-candles.svg)](https://pypi.python.org/pypi/local-candles/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Usage example:
```python
from local_candles import load_candles


def main():
    df = load_candles(
        source="binance_usdm_futures_ohlc",
        start_ts="2021-01-01",
        stop_ts="2021-02-01",
        interval="1d",
        symbol="BTCUSDT",
    )

    print(df)


if __name__ == "__main__":
    main()
```
