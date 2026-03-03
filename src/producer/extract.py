import requests
from config import logger, headers, url


def connect_to_api():
    stocks = ['TSLA', 'MSFT', 'GOOGL']
    json_response = []

    for stock in range(0, len(stocks)):  # Loops over the indices of the stocks list: 0, 1, 2.

        querystring = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": f"{stocks[stock]}",
            "outputsize": "compact",
            "interval": "5min",
            "datatype": "json"
        }

        try:
            response = requests.get(url, headers=headers, params=querystring)  # Sends an HTTP GET request to url

            response.raise_for_status()

            data = response.json()

            logger.info("Stocks successfully loaded")

            json_response.append(data)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error on stock: {e}")
            break
    return json_response


def extract_json(response):
    records = []

    for data in response:
        print(data['Meta Data'])
        symbol = list(data['Meta Data'].values())[1]

        for date_str, metrics in data['Time Series (5min)'].items():
            record = {
                "symbol": symbol,
                "date": date_str,
                "open":  float(metrics["1. open"]),   # ← convert to float
                "high":  float(metrics["2. high"]),   # ← convert to float
                "low":   float(metrics["3. low"]),    # ← convert to float
                "close": float(metrics["4. close"]),  # ← convert to float
            }

            records.append(record)

    return records
