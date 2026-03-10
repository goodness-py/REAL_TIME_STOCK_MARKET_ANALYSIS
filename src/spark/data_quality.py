import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from config import VALID_SYMBOLS

logger = logging.getLogger(__name__)


def check_not_null(df: DataFrame, column: str) -> dict:
    """Check that a column has no null values."""
    total = df.count()
    null_count = df.filter(col(column).isNull()).count()
    passed = null_count == 0

    return {
        "check": f"{column}_not_null",
        "total_rows": total,
        "failed_rows": null_count,
        "passed": passed,
        "message": f"OK" if passed else f"{null_count} null values found in '{column}'"
    }


def check_positive_prices(df: DataFrame) -> dict:
    """Check that all price columns (open, high, low, close) are greater than 0."""
    price_columns = ["open", "high", "low", "close"]
    failed_rows = df.filter(
        (col("open") <= 0) |
        (col("high") <= 0) |
        (col("low") <= 0) |
        (col("close") <= 0)
    ).count()
    passed = failed_rows == 0

    return {
        "check": "prices_positive",
        "total_rows": df.count(),
        "failed_rows": failed_rows,
        "passed": passed,
        "message": "OK" if passed else f"{failed_rows} rows with non-positive prices found"
    }


def check_high_gte_low(df: DataFrame) -> dict:
    """Check that high price is always greater than or equal to low price."""
    failed_rows = df.filter(col("high") < col("low")).count()
    passed = failed_rows == 0

    return {
        "check": "high_gte_low",
        "total_rows": df.count(),
        "failed_rows": failed_rows,
        "passed": passed,
        "message": "OK" if passed else f"{failed_rows} rows where high < low (invalid price range)"
    }


def check_valid_symbols(df: DataFrame) -> dict:
    """Check that all symbols are within the expected set (TSLA, MSFT, GOOGL)."""
    failed_rows = df.filter(~col("symbol").isin(list(VALID_SYMBOLS))).count()
    passed = failed_rows == 0

    return {
        "check": "valid_symbols",
        "total_rows": df.count(),
        "failed_rows": failed_rows,
        "passed": passed,
        "message": "OK" if passed else f"{failed_rows} rows with unexpected stock symbols"
    }


def run_quality_checks(df: DataFrame, batch_id: int) -> bool:
    """
    Run all data quality checks on a batch DataFrame.

    Returns True if all checks pass (safe to load to MySQL).
    Returns False if any check fails (block the load).
    """
    logger.info(f"[DQ] Running data quality checks on batch {batch_id}...")

    checks = [
        check_not_null(df, "symbol"),
        check_not_null(df, "date"),
        check_not_null(df, "open"),
        check_not_null(df, "close"),
        check_positive_prices(df),
        check_high_gte_low(df),
        check_valid_symbols(df),
    ]

    all_passed = True

    for result in checks:
        status = "✅ PASS" if result["passed"] else "❌ FAIL"
        logger.info(f"[DQ] {status} | {result['check']} | {result['message']}")

        if not result["passed"]:
            all_passed = False

    if all_passed:
        logger.info(f"[DQ] All checks passed for batch {batch_id}. Proceeding to load.")
    else:
        logger.error(f"[DQ] Quality checks FAILED for batch {batch_id}. Load blocked.")

    return all_passed