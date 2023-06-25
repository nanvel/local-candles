# Local candles

Usage example:
```python
from local_candles import load_candles


def main():
    df = load_candles(
        source="binance_futures_ohlc",
        start_ts="2021-01-01",
        stop_ts="2021-02-01",
        interval="1d",
        symbol="BTCUSDT",
    )

    print(df)


if __name__ == "__main__":
    main()
```
