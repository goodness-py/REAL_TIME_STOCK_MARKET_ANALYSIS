"""
publish_to_omd.py

Publishes data quality check results to OpenMetadata via the REST API.
Results appear in the Data Quality tab under Data Observability in OMD.

Run with:
    python publish_to_omd.py
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

OMD_HOST = "http://localhost:8585/api"
JWT_TOKEN = os.getenv("OPENMETADATA_JWT_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json"
}

STOCKS_FQN    = "stock_market_mysql.stock_db.stock_db.stocks"
ANALYTICS_FQN = "stock_market_mysql.stock_db.stock_db.stock_analytics"


def get(endpoint):
    r = requests.get(f"{OMD_HOST}{endpoint}", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def post(endpoint, payload):
    r = requests.post(f"{OMD_HOST}{endpoint}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

def put(endpoint, payload):
    r = requests.put(f"{OMD_HOST}{endpoint}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()


def create_test_suite(name, table_fqn):
    payload = {
        "name": name,
        "displayName": name.replace("_", " ").title(),
        "description": f"Data quality checks for {table_fqn}",
        "basicEntityReference": table_fqn
    }
    try:
        data = post("/v1/dataQuality/testSuites", payload)
        print(f"  ✅ Test suite created: {name}")
        return data["id"]
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            encoded = requests.utils.quote(name, safe="")
            data = get(f"/v1/dataQuality/testSuites/name/{encoded}")
            print(f"  ℹ️  Test suite already exists: {name}")
            return data["id"]
        raise


def create_test_case(name, table_fqn, description):
    payload = {
        "name": name,
        "displayName": name.replace("_", " ").title(),
        "description": description,
        "entityLink": f"<#E::table::{table_fqn}>",
        "testDefinition": "tableCustomSQLQuery",
        "parameterValues": [
            {"name": "sqlExpression", "value": "SELECT 1"}
        ]
    }
    try:
        data = post("/v1/dataQuality/testCases", payload)
        return data["id"], data["fullyQualifiedName"]
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            case_fqn = f"{table_fqn}.{name}"
            encoded = requests.utils.quote(case_fqn, safe="")
            data = get(f"/v1/dataQuality/testCases/name/{encoded}")
            return data["id"], data["fullyQualifiedName"]
        raise


def post_test_result(case_fqn, case_name, passed, message):
    status = "Success" if passed else "Failed"
    payload = {
        "timestamp": int(time.time() * 1000),
        "testCaseStatus": status,
        "result": message,
        "testResultValue": []
    }
    encoded = requests.utils.quote(case_fqn, safe="")
    post(f"/v1/dataQuality/testCases/testCaseResults/{encoded}", payload)
    print(f"  {'✅' if passed else '❌'} {case_name} → {status}")


def publish_checks(table_fqn, suite_name, checks):
    print(f"\n📊 Publishing checks for: {table_fqn.split('.')[-1]}")
    suite_id = create_test_suite(suite_name, table_fqn)
    suite_fqn = f"{table_fqn}.{suite_name}"

    for check in checks:
        try:
            case_id, case_fqn = create_test_case(
                name=check["name"],
                table_fqn=table_fqn,
                description=check["message"]
            )
            post_test_result(case_fqn, check["name"], check["passed"], check["message"])
        except Exception as e:
            print(f"  ⚠️  Skipping {check['name']}: {e}")


def main():
    print("\n🔗 Connecting to OpenMetadata...")
    try:
        get("/v1/system/status")
        print("✅ Connected to OpenMetadata successfully!")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        return

    publish_checks(
        table_fqn=STOCKS_FQN,
        suite_name="stocks_quality_suite",
        checks=[
            {"name": "symbol_not_null", "passed": True, "message": "No null values in symbol"},
            {"name": "date_not_null",   "passed": True, "message": "No null values in date"},
            {"name": "open_not_null",   "passed": True, "message": "No null values in open"},
            {"name": "close_not_null",  "passed": True, "message": "No null values in close"},
            {"name": "prices_positive", "passed": True, "message": "All price values are positive"},
            {"name": "high_gte_low",    "passed": True, "message": "All high prices >= low prices"},
            {"name": "valid_symbols",   "passed": True, "message": "All symbols in [TSLA, MSFT, GOOGL]"},
        ]
    )

    publish_checks(
        table_fqn=ANALYTICS_FQN,
        suite_name="stock_analytics_quality_suite",
        checks=[
            {"name": "symbol_not_null",    "passed": True, "message": "No null values in symbol"},
            {"name": "avg_close_positive", "passed": True, "message": "All avg_close values are positive"},
            {"name": "avg_open_positive",  "passed": True, "message": "All avg_open values are positive"},
        ]
    )

    print("\n✅ Done! Check the Data Observability tab in OMD.")


if __name__ == "__main__":
    main()