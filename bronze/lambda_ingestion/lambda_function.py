import os
import json
import urllib3
import boto3
from datetime import datetime, timezone

# ========== AWS CLIENTS ==========
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT")
if AWS_ENDPOINT:
    ssm = boto3.client("ssm", endpoint_url=AWS_ENDPOINT)
    s3 = boto3.client("s3", endpoint_url=AWS_ENDPOINT)
else:
    ssm = boto3.client("ssm")
    s3 = boto3.client("s3")

http = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=2.0, read=5.0), retries=False
)

# ========== CONSTANTS ==========
BUCKET = "coin-prices-bucket"
BASE_URL = "https://api.binance.com"

# ========== PARAMETER CACHE ==========
_PARAMETERS = None


def get_parameters():
    """
    Fetch runtime parameters from SSM Parameter Store.
    Cached across Lambda warm invocations.
    """
    global _PARAMETERS
    if _PARAMETERS is not None:
        return _PARAMETERS

    symbols = ssm.get_parameter(Name="/coin/binance/symbols", WithDecryption=False)[
        "Parameter"
    ]["Value"].split(",")

    interval = ssm.get_parameter(Name="/coin/binance/interval", WithDecryption=False)[
        "Parameter"
    ]["Value"]

    _PARAMETERS = {
        "symbols": [s.strip() for s in symbols],
        "interval": interval,
    }
    return _PARAMETERS


# ========== BINANCE API ==========
def fetch_kline(symbol: str, interval: str, limit: int = 1):
    """
    Fetch kline data from Binance REST API.
    """
    url = f"{BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    response = http.request("GET", url, fields=params)

    if response.status != 200:
        raise Exception(f"Binance API error: {response.status}")

    data = json.loads(response.data.decode("utf-8"))

    if not data or len(data[0]) < 7:
        raise Exception("Invalid kline data")

    return data[0]


# ========== LAMBDA HANDLER ==========
def lambda_handler(event, context):
    params = get_parameters()
    interval = params["interval"]

    results = []

    for symbol in params["symbols"]:
        # ---- Fetch candle ----
        kline = fetch_kline(symbol, interval)

        # ---- EVENT TIME (CRITICAL) ----
        event_dt = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)

        event_date = event_dt.strftime("%Y-%m-%d")
        event_hour = event_dt.strftime("%H")

        # ---- RAW RECORD ----
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

        # ---- IDEMPOTENT S3 KEY ----
        key = (
            f"bronze/"
            f"coin_prices/"
            f"symbol={symbol}/"
            f"event_date={event_date}/"
            f"hour={event_hour}/"
            f"open_time={kline[0]}.json"
        )

        # ---- WRITE TO S3 ----
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(record),
            ContentType="application/json"
        )

        # ---- STRUCTURED LOG ----
        print(json.dumps({
            "symbol": symbol,
            "interval": interval,
            "event_time": record["event_time"],
            "s3_key": key
        }))

        results.append(key)

    return {
        "status": "SUCCESS",
        "files_written": results,
        "invoked_at": datetime.now(timezone.utc).isoformat()
    }
