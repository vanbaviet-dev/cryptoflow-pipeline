import boto3
import json
from datetime import datetime
from common_utils.logger import info

s3 = boto3.client("s3")


def write_record(symbol: str, kline: dict, event_dt: datetime, BUCKET: str):
    event_date = event_dt.strftime("%Y-%m-%d")
    event_hour = event_dt.strftime("%H")
    if "open_time" not in kline:
        raise ValueError("kline dictionary missing 'open_time' key")
    
    # ---- IDEMPOTENT S3 KEY ----
    key = (
        f"bronze/"
        f"coin_prices/"
        f"symbol={symbol}/"
        f"event_date={event_date}/"
        f"hour={event_hour}/"
        f"open_time={kline['open_time']}.json"
    )

    # ---- WRITE TO S3 ----
    try:
        info("Writing record to S3", extra={"bucket": BUCKET, "key": key})
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(kline,ensure_ascii=False),
            ContentType="application/json",
        )
    except Exception as e:
        raise RuntimeError(f"Failed to write record to S3 | key={key}") from e
