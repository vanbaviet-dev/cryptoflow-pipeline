import boto3
import botocore.exceptions
from common_utils.logger import info, error

ssm = boto3.client("ssm")
_PARAMETERS = None


def get_parameters():
    """
    Fetch runtime parameters from SSM Parameter Store.
    Cached across Lambda warm invocations.
    """
    global _PARAMETERS

    if _PARAMETERS is not None:
        return _PARAMETERS

    try:
        info("Fetching parameters from SSM")

        symbols_param = ssm.get_parameter(
            Name="/coin/binance/symbols", WithDecryption=False
        )["Parameter"]["Value"]

        interval = ssm.get_parameter(
            Name="/coin/binance/interval", WithDecryption=False
        )["Parameter"]["Value"]

    except botocore.exceptions.ClientError as e:
        error("Failed to fetch parameters from SSM", exc_info=True)
        raise RuntimeError("SSM parameter fetch failed") from e

    symbols = [s.strip() for s in symbols_param.split(",") if s.strip()]

    if not symbols:
        raise ValueError("SSM parameter symbols is empty")

    if not interval:
        raise ValueError("SSM parameter interval is empty")

    _PARAMETERS = {
        "symbols": symbols,
        "interval": interval,
    }

    info(
        "Parameters loaded",
        extra={
            "symbols_count": len(symbols),
            "interval": interval,
        },
    )

    return _PARAMETERS
