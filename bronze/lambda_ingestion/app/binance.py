import json
import urllib3
from datetime import datetime, timezone
from common_utils.logger import info, warning, error

http = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=2.0, read=5.0), retries=False
)


def fetch_klines(symbol: str, interval: str, limit: int = 1):
    info(
        f"Fetching klines from Binance | symbol={symbol} interval={interval} limit={limit}"
    )
    BASE_URL = "https://api.binance.com"
    url = f"{BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    info(
        "Binance request sent",
        extra={"symbol": symbol, "interval": interval, "limit": limit},
    )
    response = http.request("GET", url, fields=params)

    if response.status != 200:
        raise RuntimeError(
            f"Binance API error | status={response.status} symbol={symbol} interval={interval}"
        )

    try:
        klines = json.loads(response.data.decode("utf-8"))
    except json.JSONDecodeError:
        raise RuntimeError("Failed to decode Binance response")
    finally:
        response.release_conn()
    info("Binance response received", extra={"symbol": symbol, "interval": interval})
    if not klines or len(klines[0]) < 7:
        raise Exception("Invalid kline data received")
    kline = klines[0]
    event_dt = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
    info(f"Kline event time: {event_dt.isoformat()}")
    record = {
        "exchange": "binance",
        "symbol": symbol,
        "interval": interval,
        "event_time": event_dt.isoformat(),
        "open_time": kline[0],
        "close_time": kline[6],
        "open": kline[1],
        "high": kline[2],
        "low": kline[3],
        "close": kline[4],
        "volume": kline[5],
        "ingest_time": datetime.now(timezone.utc).isoformat(),
    }
    info(
        "Fetched kline",
        extra={
            "symbol": symbol,
            "open_time": record["open_time"],
            "close_time": record["close_time"],
        },
    )
    return record
