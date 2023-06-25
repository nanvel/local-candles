from typing import List, Optional, Union
from pathlib import Path

from .data import Data
from .models import Time
from .sources import Source, SOURCES


__all__ = ("load_candles",)


def load_candles(
    source: Union[Source, str],
    start_ts: Union[Time, str],
    stop_ts: Union[Time, str],
    columns: Optional[List[str]] = None,
    **kwargs
):
    start_ts = Time.from_string(start_ts)
    stop_ts = Time.from_string(stop_ts)

    if not isinstance(source, Source):
        source = SOURCES[source](**kwargs)

    return Data(cache_root=Path()).load_df(
        source=source,
        start_ts=start_ts,
        stop_ts=stop_ts,
        columns=columns or ["open", "high", "low", "close", "volume"],
    )
