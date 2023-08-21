import datetime
from typing import List, Optional, Union
from pathlib import Path

from .data import Data
from .models import Time
from .sources import Source, SOURCES


__all__ = ("load_candles",)


def load_candles(
    source: Union[Source, str],
    start_ts: Union[Time, str, datetime.datetime],
    stop_ts: Union[Time, str, datetime.datetime],
    columns: Optional[List[str]] = None,
    cache_root: Optional[Union[Path, str]] = None,
    reload_latest: bool = True,
    **source_kwargs
):
    if not isinstance(start_ts, Time):
        start_ts = Time.from_string(start_ts)
    if not isinstance(stop_ts, Time):
        stop_ts = Time.from_string(stop_ts)

    if not isinstance(source, Source):
        source = SOURCES[source](**source_kwargs)

    if not cache_root:
        cache_root = Path(".cache")
    if not isinstance(cache_root, Path):
        cache_root = Path(cache_root)

    return Data(cache_root=cache_root).load_df(
        source=source,
        start_ts=start_ts,
        stop_ts=stop_ts,
        columns=columns or ["open", "high", "low", "close", "volume"],
        reload_latest=reload_latest,
    )
