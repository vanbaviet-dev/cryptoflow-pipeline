from datetime import datetime, timezone
from common_utils.logger import info, warning, error
from .binance import fetch_klines
from .s3_writer import write_record
from .config import get_parameters

BUCKET = "coin-prices-bucket"

def lambda_handler(event, context):
    info("Lambda handler started")
    try:
        info("Fetching parameters from SSM")
        params = get_parameters()
        info("Parameters fetched successfully")
    except Exception as e:
        error(f"Error fetching parameters: {e}")
        return {
            "statusCode": 500,
            "body": {"error": "Failed to fetch parameters"},
        }

    interval = params["interval"]

    results = []

    for symbol in params["symbols"]:
        # ---- Fetch candle ----
        info(f"START fetch kline | symbol={symbol} interval={interval}")
        try:
            kline = fetch_klines(symbol, interval)
            info(f"SUCCESS fetch kline | symbol={symbol}")
        except Exception as e:
            error(f"Error fetching kline for {symbol}: {e}")
            continue
        try:
            info(f"Writing kline to S3 for {symbol}")
            event_dt = datetime.fromtimestamp(
                kline["open_time"] / 1000, tz=timezone.utc
            )
            write_record(symbol, kline, event_dt, BUCKET)
            info(f"SUCCESS write kline to S3 | symbol={symbol}")
        except Exception as e:
            error(f"Error writing kline to S3 for {symbol}: {e}")
            continue

        info(f"INGESTED kline | symbol={symbol} event_time={kline['event_time']}")
        results.append(f"{symbol}-{kline['open_time']}")
    info("Lambda handler completed")
    return {
        "statusCode": 200,
        "body": {
            "ingested_records": results,
        },
    }
