"""
Test script for data_quality.py logic.
Uses pandas to simulate Spark DataFrame checks — no Docker needed.

Run with: python test_data_quality.py
"""

import pandas as pd

VALID_SYMBOLS = ["TSLA", "MSFT", "GOOGL"]

# ── Replicate the checks using pandas ────────────────────────────────────────

def check_not_null(df: pd.DataFrame, column: str) -> dict:
    null_count = df[column].isna().sum()
    passed = null_count == 0
    return {
        "check": f"{column}_not_null",
        "total_rows": len(df),
        "failed_rows": int(null_count),
        "passed": passed,
        "message": "OK" if passed else f"{null_count} null values found in '{column}'"
    }

def check_positive_prices(df: pd.DataFrame) -> dict:
    failed = df[(df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0)]
    passed = len(failed) == 0
    return {
        "check": "prices_positive",
        "total_rows": len(df),
        "failed_rows": len(failed),
        "passed": passed,
        "message": "OK" if passed else f"{len(failed)} rows with non-positive prices found"
    }

def check_high_gte_low(df: pd.DataFrame) -> dict:
    failed = df[df["high"] < df["low"]]
    passed = len(failed) == 0
    return {
        "check": "high_gte_low",
        "total_rows": len(df),
        "failed_rows": len(failed),
        "passed": passed,
        "message": "OK" if passed else f"{len(failed)} rows where high < low (invalid price range)"
    }

def check_valid_symbols(df: pd.DataFrame) -> dict:
    failed = df[~df["symbol"].isin(VALID_SYMBOLS)]
    passed = len(failed) == 0
    return {
        "check": "valid_symbols",
        "total_rows": len(df),
        "failed_rows": len(failed),
        "passed": passed,
        "message": "OK" if passed else f"{len(failed)} rows with symbols not in {VALID_SYMBOLS}"
    }

def run_quality_checks(df: pd.DataFrame, batch_id: int) -> bool:
    print(f"\n[DQ] Running data quality checks on batch {batch_id}...")

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
        print(f"[DQ] {status} | {result['check']} | {result['message']}")
        if not result["passed"]:
            all_passed = False

    if all_passed:
        print(f"[DQ] ✅ All checks passed — safe to load to MySQL.\n")
    else:
        print(f"[DQ] ❌ Quality checks FAILED — batch blocked from MySQL.\n")

    return all_passed


# ── TEST CASES ────────────────────────────────────────────────────────────────

def test_good_data():
    print("=" * 60)
    print("TEST 1: Good data — all checks should PASS")
    print("=" * 60)
    df = pd.DataFrame([
        {"id": "1", "symbol": "TSLA", "date": "2024-03-09 10:00:00", "open": 180.5, "high": 182.0, "low": 179.0, "close": 181.0},
        {"id": "2", "symbol": "MSFT", "date": "2024-03-09 10:00:00", "open": 415.0, "high": 417.5, "low": 413.0, "close": 416.0},
        {"id": "3", "symbol": "GOOGL", "date": "2024-03-09 10:00:00", "open": 140.0, "high": 141.5, "low": 139.0, "close": 140.8},
    ])
    result = run_quality_checks(df, batch_id=1)
    assert result == True, "TEST 1 FAILED — expected all checks to pass"
    print("TEST 1 RESULT: PASSED ✅\n")


def test_null_symbol():
    print("=" * 60)
    print("TEST 2: Null symbol — should FAIL")
    print("=" * 60)
    df = pd.DataFrame([
        {"id": "1", "symbol": None, "date": "2024-03-09 10:00:00", "open": 180.5, "high": 182.0, "low": 179.0, "close": 181.0},
        {"id": "2", "symbol": "MSFT", "date": "2024-03-09 10:00:00", "open": 415.0, "high": 417.5, "low": 413.0, "close": 416.0},
    ])
    result = run_quality_checks(df, batch_id=2)
    assert result == False, "TEST 2 FAILED — expected null symbol check to fail"
    print("TEST 2 RESULT: PASSED ✅\n")


def test_negative_price():
    print("=" * 60)
    print("TEST 3: Negative price — should FAIL")
    print("=" * 60)
    df = pd.DataFrame([
        {"id": "1", "symbol": "TSLA", "date": "2024-03-09 10:00:00", "open": -10.0, "high": 182.0, "low": 179.0, "close": 181.0},
        {"id": "2", "symbol": "MSFT", "date": "2024-03-09 10:00:00", "open": 415.0, "high": 417.5, "low": 413.0, "close": 416.0},
    ])
    result = run_quality_checks(df, batch_id=3)
    assert result == False, "TEST 3 FAILED — expected negative price check to fail"
    print("TEST 3 RESULT: PASSED ✅\n")


def test_high_less_than_low():
    print("=" * 60)
    print("TEST 4: High < Low (corrupted price) — should FAIL")
    print("=" * 60)
    df = pd.DataFrame([
        {"id": "1", "symbol": "TSLA", "date": "2024-03-09 10:00:00", "open": 180.5, "high": 170.0, "low": 179.0, "close": 181.0},
    ])
    result = run_quality_checks(df, batch_id=4)
    assert result == False, "TEST 4 FAILED — expected high < low check to fail"
    print("TEST 4 RESULT: PASSED ✅\n")


def test_invalid_symbol():
    print("=" * 60)
    print("TEST 5: Invalid symbol (AAPL not in config) — should FAIL")
    print("=" * 60)
    df = pd.DataFrame([
        {"id": "1", "symbol": "AAPL", "date": "2024-03-09 10:00:00", "open": 180.5, "high": 182.0, "low": 179.0, "close": 181.0},
    ])
    result = run_quality_checks(df, batch_id=5)
    assert result == False, "TEST 5 FAILED — expected invalid symbol check to fail"
    print("TEST 5 RESULT: PASSED ✅\n")


# ── RUN ALL TESTS ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n🧪 Starting Data Quality Tests...\n")
    test_good_data()
    test_null_symbol()
    test_negative_price()
    test_high_less_than_low()
    test_invalid_symbol()
    print("=" * 60)
    print("🎉 All tests completed!")
    print("=" * 60)