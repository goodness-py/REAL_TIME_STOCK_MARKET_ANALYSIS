# Real-Time Stock Market Analysis Pipeline

## Business Context

**MarketPulse Analytics** is a leading financial technology firm based in New York City, specializing in real-time market insights for institutional investors. Founded in 2016, MarketPulse provides actionable financial data to hedge funds, asset managers, and electronic brokers across multiple global exchanges.

### The Problem

As data volume and client demand increased, MarketPulse faced three critical pain points:

| Pain Point | Impact |
|---|---|
| **Data Latency** | Delays in integrating data from multiple sources affected accuracy of insights — even a 1 second delay could result in significant financial losses for clients |
| **Scalability** | Existing infrastructure struggled during peak periods (market opens, earnings reports), causing performance bottlenecks |
| **System Reliability** | Lack of robust monitoring made it impossible to detect anomalies in real-time, leading to compliance risks |

### The Solution

This project builds a **scalable, real-time data pipeline** that:
- Streams stock market data with low latency through Apache Kafka
- Processes data in real-time using Apache Spark
- Validates data quality before it lands in MySQL
- Stores raw and processed data in MySQL
- Publishes quality check results to OpenMetadata for live observability
- Delivers actionable insights via interactive Metabase dashboards

### Business Impact

- ⚡ **Faster Decision-Making** — clients can act on financial data instantly
- ✅ **Data Quality Assurance** — bad data is blocked before it reaches the database
- 📊 **Live Observability** — quality check results visible in OpenMetadata in real time
- 📈 **Improved Client Trust** — real-time, transparent data reporting
- 🏆 **Market Leadership** — positions MarketPulse as a leader in high-frequency trading analytics

---

## Project Overview

A real-time data pipeline that extracts live stock data from the Alpha Vantage API, streams it through Apache Kafka, processes it with Apache Spark, validates data quality before storage, stores it in MySQL, publishes quality results to OpenMetadata, and visualizes it with Metabase.

All components are containerized with Docker for easy deployment.

---

## Architecture

![Architecture Diagram](img/image.png)

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Producer, data extraction & quality checks |
| Apache Kafka | 7.4.10 (KRaft) | Message streaming |
| Apache Spark | 3.5.1 | Real-time data processing |
| MySQL | 8.0 | Data storage |
| OpenMetadata | 1.12.1 | Data catalogue, dictionary & observability |
| Metabase | 0.48.0 | Data visualization |
| Docker | Latest | Containerization |

---

## Project Structure

```
REAL_TIME_STOCK_MARKET_ANALYSIS/
├── src/
│   ├── producer/
│   │   ├── config.py            # API config + configurable VALID_SYMBOLS
│   │   ├── extract.py           # Fetches stock data from Alpha Vantage API
│   │   └── main.py              # Sends data to Kafka topic
│   └── spark/
│       ├── spark_job.py         # Reads from Kafka, runs quality checks, writes to MySQL
│       └── data_quality.py      # 7 data quality checks with circuit breaker pattern
├── publish_to_omd.py            # Publishes quality results to OpenMetadata
├── test_data_quality.py         # Local tests for all quality checks
├── docker-compose-omd.yml       # OpenMetadata Docker stack
├── .env                         # Environment variables (not committed)
├── compose.yml                  # Main pipeline Docker services
├── Dockerfile                   # Producer container
├── Dockerfile.spark             # Spark job container
├── requirements.txt             # Producer dependencies
└── requirements.spark.txt       # Spark dependencies
```

---

## Services

| Service | Port | Description |
|---|---|---|
| Kafka | 9092 | Internal broker |
| Kafka UI | 8085 | Visual Kafka dashboard |
| Spark Master | 8081 | Spark web UI |
| Spark Worker | — | Executes Spark jobs |
| MySQL | 3307 | Stock market database |
| Metabase | 3000 | Visualization dashboard |
| OpenMetadata | 8585 | Data catalogue & observability UI |
| OMD MySQL | 3308 | OpenMetadata internal database |

---

## Database Tables

### `stocks` — Raw stock data

| Column | Type | Description |
|---|---|---|
| id | VARCHAR(36) | Unique record ID (UUID) |
| symbol | VARCHAR(10) | Stock ticker (TSLA, MSFT, GOOGL) |
| date | DATETIME | Timestamp of the data point |
| open | FLOAT | Opening price |
| high | FLOAT | Highest price |
| low | FLOAT | Lowest price |
| close | FLOAT | Closing price |

### `stock_analytics` — Processed analytics

| Column | Type | Description |
|---|---|---|
| symbol | VARCHAR(10) | Stock ticker |
| avg_open | FLOAT | Average opening price |
| avg_high | FLOAT | Average highest price |
| avg_low | FLOAT | Average lowest price |
| avg_close | FLOAT | Average closing price |

---

## Data Quality Checks

Data quality is enforced inside the Spark pipeline via `data_quality.py`. All checks run on every batch before data is written to MySQL. If any check fails, the entire batch is blocked — no bad data lands in the database.

### `stocks` table — 7 checks

| Check | Rule |
|---|---|
| `symbol_not_null` | symbol column must never be null |
| `date_not_null` | date column must never be null |
| `open_not_null` | open price must never be null |
| `close_not_null` | close price must never be null |
| `prices_positive` | open, high, low, close must all be > 0 |
| `high_gte_low` | high price must always be >= low price |
| `valid_symbols` | symbol must be in the configured VALID_SYMBOLS list |

### `stock_analytics` table — 3 checks

| Check | Rule |
|---|---|
| `symbol_not_null` | symbol column must never be null |
| `avg_close_positive` | avg_close must be > 0 |
| `avg_open_positive` | avg_open must be > 0 |

### Configuring Valid Symbols

Add to your `.env` file to override the default:

```
VALID_SYMBOLS=TSLA,MSFT,GOOGL,AAPL
```

---

## OpenMetadata Integration

This project integrates with [OpenMetadata](https://open-metadata.org) for data cataloguing, documentation, and live quality observability.

### What OMD Provides

- **Data Catalogue** — all tables automatically discovered and listed
- **Data Dictionary** — column-level descriptions for `stocks` and `stock_analytics`
- **Data Observability** — quality check results visible per table with timestamps and pass/fail history

### Starting OMD

```bash
docker compose -f docker-compose-omd.yml up -d
```

Access the UI at: http://localhost:8585  
Default login: `admin@open-metadata.org` / `admin`

### Publishing Quality Results to OMD

1. Generate a Personal Access Token in OMD:
   - Profile (top right) → View Profile → Access Token → Generate (90 days)

2. Add the token to your `.env`:
```
OPENMETADATA_JWT_TOKEN=your_token_here
```

3. Run the publish script:
```bash
python publish_to_omd.py
```

Results appear at: **Explore → Tables → stocks → Data Observability → Data Quality**

---

## Stocks Tracked

- **TSLA** — Tesla Inc.
- **MSFT** — Microsoft Corporation
- **GOOGL** — Alphabet Inc. (Google)

---

## Getting Started

### Prerequisites
- Docker Desktop installed
- Python 3.11+
- Alpha Vantage API key (via RapidAPI)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/goodness-py/REAL_TIME_STOCK_MARKET_ANALYSIS.git
cd REAL_TIME_STOCK_MARKET_ANALYSIS
```

2. Create a `.env` file in the root directory:
```
API_KEY=your_api_key_here
MYSQL_HOST=db
MYSQL_PORT=3306
MYSQL_DATABASE=stock_db
MYSQL_USER=root
MYSQL_PASSWORD=your_password_here
VALID_SYMBOLS=TSLA,MSFT,GOOGL
OPENMETADATA_JWT_TOKEN=your_omd_token_here
```

3. Start the main pipeline:
```bash
docker compose up --build
```

4. (Optional) Start OpenMetadata:
```bash
docker compose -f docker-compose-omd.yml up -d
```

5. Access the dashboards:
- **Metabase:** http://localhost:3000
- **Kafka UI:** http://localhost:8085
- **Spark UI:** http://localhost:8081
- **OpenMetadata:** http://localhost:8585

### Running Quality Check Tests Locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python test_data_quality.py
```

---

## How It Works

1. **Producer** (`main.py`) fetches intraday stock data for TSLA, MSFT and GOOGL from the Alpha Vantage API every run and sends each record as a JSON message to the Kafka topic `stock_topic`.

2. **Kafka** stores the messages in the `stock_topic` topic and makes them available for consumers.

3. **Spark Streaming** (`spark_job.py`) reads messages from Kafka in real-time, parses the JSON, and passes each batch to the quality check module.

4. **Data Quality** (`data_quality.py`) runs 10 checks across both tables on every batch. If any check fails, the batch is blocked and logged. If all checks pass, data is written to MySQL.

5. **MySQL** stores raw records in the `stocks` table and computed averages in `stock_analytics`.

6. **OpenMetadata** (`publish_to_omd.py`) receives quality check results and publishes them to the Data Observability tab — making pipeline health visible to anyone with OMD access.

7. **Metabase** connects to MySQL and visualizes both tables as interactive dashboards.

---

## Key Learnings

- Building a real-time streaming pipeline from scratch
- Separation of concerns — each service has one clear responsibility
- KRaft mode Kafka (no Zookeeper)
- Spark Structured Streaming with `foreachBatch`
- Data quality as a first-class concern — circuit breaker pattern in streaming pipelines
- OpenMetadata REST API — test suites, test cases, and result publishing
- Data cataloguing and observability with OpenMetadata
- Docker networking across multiple Compose stacks
- Containerizing multi-service applications with Docker Compose
- Data visualization with Metabase

---

## Future Improvements

- Integrate `publish_to_omd.py` directly into the Spark pipeline for automatic result publishing after each batch
- Switch to continuous streaming (currently runs once per execution)
- Add more stock symbols
- Implement price spike alerts via OMD alerting
- Migrate to PostgreSQL
- Deploy to cloud (AWS/GCP)
- Implement Star Schema data model

---

## Proposed Star Schema

The current schema uses 2 flat tables. A production-grade implementation would use a **Star Schema** for better performance, scalability and querying.

### Schema Diagram

```
                 dim_stock
                 (TSLA, MSFT, GOOGL)
                      │
                      │ symbol
                      │
dim_date ─── date_id ─┼─── fact_stock_prices ─── time_id ─── dim_time
(dates)               │         (prices)                       (times)
                      │
                      │ symbol
                      │
                 fact_stock_analytics
                    (averages)
```