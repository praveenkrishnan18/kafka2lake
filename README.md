# kafka2lake

> **Real-Time CDC Streaming Pipeline: PostgreSQL → Kafka → Azure Data Lake**
>
> A production-ready, beginner-friendly Change Data Capture (CDC) streaming pipeline demonstrating how to capture database changes, stream them through Apache Kafka, and persist them to Azure Data Lake Storage Gen2 using Python and containerized infrastructure.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Running & Testing](#running--testing)
- [How CDC Works](#how-cdc-works)
- [Cleanup & Troubleshooting](#cleanup--troubleshooting)
- [Next Steps](#next-steps)
- [License](#license)

---

## Overview

**kafka2lake** is a complete, runnable data pipeline that captures changes from a PostgreSQL database via **triggers**, publishes them to **Apache Kafka**, and streams them into **Azure Data Lake Storage Gen2 (Bronze Layer)** using a Python consumer.

This project is ideal for:
- **Data Engineering portfolios** — demonstrates end-to-end streaming architecture
- **Learning CDC patterns** — trigger-based CDC as a stepping stone to Debezium
- **Azure integration** — practical ADLS Gen2 connectivity from Python
- **Local development & testing** — full stack runs via Docker Compose

### Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Database** | PostgreSQL | 15 |
| **Message Broker** | Apache Kafka | 7.4.0 |
| **Producer** | Python | 3.11+ |
| **Consumer** | Python | 3.11+ |
| **Cloud Storage** | Azure ADLS Gen2 | — |
| **Infrastructure** | Docker Compose | Latest |
| **Testing** | pytest + mocks | — |

---

## Key Features

✅ **No Pre-Data / No Duplicates**
- Fresh PostgreSQL schema with empty `customers` table
- Watermark-based producer ensures no event re-publishing on restart
- Kafka consumer group offsets prevent re-consumption
- File overwrite strategy for idempotent uploads

✅ **Containerized Infrastructure**
- PostgreSQL, Kafka, Zookeeper, pgAdmin — all via Docker Compose
- Single command to bootstrap the entire stack

✅ **Comprehensive Logging**
- Centralized logger with file + console output
- Timestamps and log levels for debugging

✅ **Production Patterns**
- Connection pooling concepts
- Error handling & retries
- Graceful shutdown
- Configuration management via `.env`

✅ **Test Coverage**
- 17+ unit tests with mocked dependencies
- No external services required for test suite
- pytest fixtures for reusability

✅ **Scalable Design**
- Modular producer/consumer architecture
- Kafka topics and consumer groups for horizontal scaling
- ADLS path structure supports multiple tables

---

## Architecture

### System Diagram

<img width="1536" height="1024" alt="kafka2lake" src="https://github.com/user-attachments/assets/a12e1761-7ed9-4caf-995a-e91f73b5bb99" />


### Data Flow

1. **PostgreSQL INSERT/UPDATE/DELETE** → Trigger captures operation
2. **Trigger writes to `cdc_events`** → Audit log with full row snapshot
3. **Producer polls `cdc_events`** → Fetches only new events (watermark > last_event_id)
4. **Publishes to Kafka topic** → `cdc_customers` topic
5. **Consumer subscribes to topic** → Receives messages from Kafka
6. **Deserializes JSON message** → Reconstructs event object
7. **Uploads to ADLS Gen2** → Bronze layer path: `cdc/customers/cdc_YYYYMMDD_HHMMSS_<id>.json`

### Deduplication Guarantees

| Layer | Strategy | How It Works |
|-------|----------|-------------|
| **Producer** | Watermark persistence | Tracks `last_event_id` on disk; polls only `event_id > watermark` |
| **Kafka** | Consumer group offsets | `enable_auto_commit=True` prevents re-reading same partition offset |
| **ADLS** | Filename + overwrite | Event ID embedded in filename; `overwrite=True` ensures idempotency |

---

## Prerequisites

### System Requirements

| Requirement | Specification |
|------------|----------------|
| **OS** | Windows / macOS / Linux |
| **Docker** | 20.10+ (with Compose) |
| **Python** | 3.11 or higher |
| **RAM** | 4 GB minimum (8 GB recommended for comfortable testing) |
| **Disk** | 2 GB free (PostgreSQL + Kafka containers) |

### Azure Setup

You'll need an **Azure Storage Account** with:
- ✅ **Hierarchical namespace enabled** (ADLS Gen2 requirement)
- ✅ **Storage Account Key** (for authentication)
- ✅ **A container** named `bronze` (or custom name in `.env`)

**Create ADLS Gen2 account in Azure Portal:**
1. Create Storage Account → Enable "Data Lake Storage Gen2" ✓
2. Copy **Storage account name** and **access key** from Keys section
3. Create container (e.g., `bronze`)

---

## Quick Start

### Step 1 — Clone or Download

```bash
git clone https://github.com/yourusername/kafka2lake.git
cd kafka2lake
```

Or download the ZIP and unzip:
```bash
unzip kafka2lake.zip
cd kafka2lake
```

---

### Step 2 — Configure Environment

Create or update `.env` file in the project root:

```env
# ─── Azure ADLS Gen2 (REQUIRED) ──────────────────────────────
ADLS_ACCOUNT_NAME=yourstorageaccount
ADLS_ACCOUNT_KEY=your_storage_account_key_here
ADLS_CONTAINER_NAME=bronze
ADLS_BRONZE_PATH=cdc/customers/

# ─── PostgreSQL (Docker defaults) ────────────────────────────
POSTGRES_USER=cdc_user
POSTGRES_PASSWORD=cdc_password
POSTGRES_DB=cdc_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# ─── Kafka (Docker defaults) ────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=cdc_customers
KAFKA_GROUP_ID=cdc_consumer_group

# ─── Pipeline ────────────────────────────────────────────────
POLL_INTERVAL_SECONDS=5
LOG_LEVEL=INFO
```

> **⚠️ Important:** Add `.env` to `.gitignore` — never commit Azure credentials!

---

### Step 3 — Start Infrastructure

```bash
docker-compose up -d
```

**Expected output:**
```
Creating zookeeper ... done
Creating postgres ... done
Creating kafka ... done
Creating pgadmin ... done
```

**Verify containers are running:**
```bash
docker-compose ps
```

### Step 4 — Create Python Virtual Environment

```bash
# macOS / Linux
python3 -m venv venv
source venv/bin/activate

# Windows (PowerShell)
python -m venv venv
venv\Scripts\Activate.ps1

# Windows (Command Prompt)
python -m venv venv
venv\Scripts\activate
```

Install dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

### Step 5 — Start the Producer

```bash
python run_producer.py
```

**Expected output:**
```
2024-06-15 10:00:01 | INFO | producer.db_poller | No watermark file found — starting from event_id=0 (fresh run).
2024-06-15 10:00:01 | INFO | producer.db_poller | Connected to PostgreSQL (localhost:5432).
2024-06-15 10:00:01 | INFO | producer.db_poller | Kafka producer connected to localhost:9092.
2024-06-15 10:00:01 | INFO | producer.db_poller | Polling cdc_events every 5 second(s) …
```

*Keep this terminal open. Open a new terminal for the next steps.*

---

### Step 6 — Start the Consumer (New Terminal)

Activate venv (same as Step 4), then:

```bash
python run_consumer.py
```

**Expected output:**
```
2024-06-15 10:00:05 | INFO | consumer.kafka_consumer | Kafka consumer connected (broker: localhost:9092, topic: cdc_customers).
2024-06-15 10:00:05 | INFO | consumer.kafka_consumer | Listening for messages …
```

*Keep this terminal open. Open a new terminal for the next steps.*

---

### Step 7 — Trigger CDC Events (New Terminal)

Connect to PostgreSQL:
```bash
docker exec -it postgres psql -U cdc_user -d cdc_db
```

Run sample DML operations:
```sql
-- INSERT
INSERT INTO customers (customer_name, email, updated_at)
VALUES ('Alice Johnson', 'alice@example.com', NOW());

-- UPDATE
UPDATE customers
SET    email = 'alice.new@example.com', updated_at = NOW()
WHERE  customer_name = 'Alice Johnson';

-- DELETE
DELETE FROM customers
WHERE  customer_name = 'Alice Johnson';

-- Verify events captured
SELECT event_id, operation, customer_name, email
FROM   cdc_events
ORDER BY event_id;
```

**Producer output (terminal from Step 5):**
```
2024-06-15 10:00:15 | INFO | producer.db_poller | Found 3 new event(s).
2024-06-15 10:00:15 | INFO | producer.db_poller | Publishing event_id=1 (INSERT) to Kafka.
2024-06-15 10:00:15 | INFO | producer.db_poller | Publishing event_id=2 (UPDATE) to Kafka.
2024-06-15 10:00:15 | INFO | producer.db_poller | Publishing event_id=3 (DELETE) to Kafka.
2024-06-15 10:00:15 | INFO | producer.db_poller | Saved watermark: event_id=3.
```

**Consumer output (terminal from Step 6):**
```
2024-06-15 10:00:15 | INFO | consumer.kafka_consumer | Received message from partition 0, offset 0.
2024-06-15 10:00:15 | INFO | consumer.adls_uploader | Uploading cdc_20240615_100015_1.json to ADLS.
2024-06-15 10:00:15 | INFO | consumer.adls_uploader | Successfully uploaded to bronze/cdc/customers/cdc_20240615_100015_1.json.
[... repeat for events 2, 3]
```

---

### Step 8 — Verify Files in ADLS

Open **Azure Portal** → your Storage Account → **Containers → bronze → cdc/customers/**

You should see three JSON files:
```
cdc_20240615_100015_1.json    ← INSERT
cdc_20240615_100015_2.json    ← UPDATE
cdc_20240615_100015_3.json    ← DELETE
```

Sample file content:
```json
{
  "event_id": 1,
  "operation": "INSERT",
  "customer_id": 1,
  "customer_name": "Alice Johnson",
  "email": "alice@example.com",
  "updated_at": "2024-06-15T10:00:15",
  "captured_at": "2024-06-15T10:00:15.123456"
}
```

---

## Project Structure

```
kafka2lake/
├── README.md                     ← This file
├── requirements.txt              ← Python dependencies
├── .env                          ← Environment config (add to .gitignore!)
├── .gitignore                    ← Git ignore rules
│
├── docker-compose.yml            ← PostgreSQL, Kafka, Zookeeper, pgAdmin
│
├── run_producer.py               ← Entry point: Start CDC poller
├── run_consumer.py               ← Entry point: Start Kafka consumer
├── reset_watermark.py            ← Dev utility: Reset watermark for replay
│
├── config/
│   ├── __init__.py
│   ├── settings.py               ← Load and validate .env variables
│   └── logger.py                 ← Centralized logging setup
│
├── producer/
│   ├── __init__.py
│   └── db_poller.py              ← Poll PostgreSQL, publish to Kafka
│                                   (handles watermarking)
│
├── consumer/
│   ├── __init__.py
│   ├── kafka_consumer.py         ← Consume from Kafka, deserialize
│   └── adls_uploader.py          ← Upload JSON events to ADLS Gen2
│
├── sql/
│   ├── init.sql                  ← Schema creation + CDC trigger
│   └── manual_dml.sql            ← Example INSERT/UPDATE/DELETE statements
│
├── tests/
│   ├── __init__.py
│   ├── test_pipeline.py          ← 17+ unit tests (all mocked)
│   └── conftest.py               ← pytest fixtures (if needed)
│
├── logs/
│   └── cdc_pipeline.log          ← Auto-created at runtime
│
└── watermark.json                ← Auto-created at runtime (gitignored)
```

---

## Configuration

### Environment Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `ADLS_ACCOUNT_NAME` | string | — | Azure storage account name (**required**) |
| `ADLS_ACCOUNT_KEY` | string | — | Azure storage account key (**required**) |
| `ADLS_CONTAINER_NAME` | string | `bronze` | Container name in ADLS |
| `ADLS_BRONZE_PATH` | string | `cdc/customers/` | Path prefix for uploads |
| `POSTGRES_USER` | string | `cdc_user` | PostgreSQL username |
| `POSTGRES_PASSWORD` | string | `cdc_password` | PostgreSQL password |
| `POSTGRES_DB` | string | `cdc_db` | Database name |
| `POSTGRES_HOST` | string | `localhost` | PostgreSQL hostname |
| `POSTGRES_PORT` | int | `5432` | PostgreSQL port |
| `KAFKA_BOOTSTRAP_SERVERS` | string | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | string | `cdc_customers` | Topic name |
| `KAFKA_GROUP_ID` | string | `cdc_consumer_group` | Consumer group ID |
| `POLL_INTERVAL_SECONDS` | int | `5` | Producer polling interval (seconds) |
| `LOG_LEVEL` | string | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Watermark File

**Location:** `watermark.json` (project root)

**Structure:**
```json
{
  "last_event_id": 42
}
```

**Behavior:**
- Created automatically after first producer run
- Persisted after each successful publish
- Used to filter events on restart (only `event_id > last_event_id`)
- Can be reset with `python reset_watermark.py` (dev only)

---

## Running & Testing

### Unit Tests

Run all tests (no Docker or Azure credentials needed):

```bash
# Activate venv first
source venv/bin/activate

# Run all tests
pytest -v

# Run specific test file
pytest tests/test_pipeline.py -v

# Run specific test function
pytest tests/test_pipeline.py::test_settings_defaults -v

# Run with coverage
pytest --cov=producer --cov=consumer --cov=config tests/
```

**Expected output:**
```
tests/test_pipeline.py::test_settings_defaults                   PASSED
tests/test_pipeline.py::test_get_logger_returns_logger           PASSED
tests/test_pipeline.py::test_load_watermark_returns_zero_when... PASSED
tests/test_pipeline.py::TestCDCPoller::test_connect_postgres... PASSED
tests/test_pipeline.py::TestCDCPoller::test_fetch_new_events... PASSED
tests/test_pipeline.py::TestADLSUploader::test_connect_creates... PASSED
...
17 passed in 0.42s
```

### Development Commands

**Check container logs:**
```bash
docker-compose logs -f postgres    # PostgreSQL
docker-compose logs -f kafka       # Kafka broker
docker-compose logs -f zookeeper   # Zookeeper
```

**Reset watermark (replay all events):**
```bash
python reset_watermark.py
# Restart producer to re-publish all events
```

**Stop all containers:**
```bash
docker-compose down
```

**Remove all data and start fresh:**
```bash
docker-compose down -v  # -v removes named volumes
docker-compose up -d
```

---

## How CDC Works

### Trigger-Based CDC (This Project)

```sql
-- PostgreSQL Trigger (from sql/init.sql)
CREATE TRIGGER cdc_trigger
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW
EXECUTE FUNCTION log_cdc_event();
```

**Behavior:**
1. Every DML operation on `customers` fires the trigger
2. Trigger calls `log_cdc_event()` function
3. Function inserts a row into `cdc_events` table with:
   - `event_id` (auto-increment)
   - `operation` (INSERT, UPDATE, DELETE)
   - Full row snapshot (as JSONB)
   - `captured_at` timestamp

**Advantages:**
- ✅ Simple, no external dependencies
- ✅ Works with any PostgreSQL version
- ✅ Easy to debug (events visible in table)

**Limitations:**
- ❌ Not log-based (PostgreSQL WAL not used)
- ❌ Requires polling (not streaming directly)
- ❌ Slower than Debezium for high-volume databases

### Comparison: Trigger vs. Log-Based CDC

| Aspect | Trigger (This Project) | Log-Based (Debezium) |
|--------|----------------------|----------------------|
| **Mechanism** | SQL trigger → audit table | PostgreSQL WAL reader |
| **Setup** | Manual SQL | Connector config |
| **Dependencies** | PostgreSQL, Python | Debezium, Java, Kafka Connect |
| **Performance** | Good for < 1K ops/sec | Excellent for > 1K ops/sec |
| **Maturity** | Learning project | Production-grade |

---

## Cleanup & Troubleshooting

### Common Issues

**Q: Producer/Consumer fails with "Connection refused"**
- Ensure `docker-compose up -d` completed successfully
- Check if containers are running: `docker-compose ps`
- Check logs: `docker-compose logs postgres`

**Q: "No Azure credentials provided"**
- Verify `.env` file exists in project root
- Check `ADLS_ACCOUNT_NAME` and `ADLS_ACCOUNT_KEY` are set
- Azure SDK requires either env vars or `.env` file

**Q: Consumer not receiving messages**
- Verify producer is publishing: check producer logs
- Check Kafka topic exists: `docker-compose logs kafka | grep topic`
- Reset consumer group: restart consumer, it will re-read from earliest offset

**Q: "Watermark file corrupted"**
```bash
python reset_watermark.py
# Or manually delete watermark.json and restart producer
```

### Cleanup

**Stop and remove containers:**
```bash
docker-compose down
```

**Remove all data and start fresh:**
```bash
docker-compose down -v
rm watermark.json
docker-compose up -d
```

**Stop producer/consumer gracefully:**
- Press `Ctrl+C` in each terminal
- Wait a few seconds for cleanup

---

## Next Steps

### Immediate (1–2 weeks)

- [ ] **Debezium Integration** — Replace trigger-based CDC with WAL-based
  - Add Kafka Connect container
  - Use Debezium PostgreSQL connector
  - Remove manual trigger setup

- [ ] **Parquet Format** — Write to Parquet instead of JSON
  ```python
  import pyarrow.parquet as pq
  table = pa.Table.from_pydict(event_dict)
  pq.write_table(table, file_path)
  ```

- [ ] **Error Handling & Retries** — Add exponential backoff
  - Implement retry decorator for Kafka publish
  - Handle ADLS transient failures

### Medium (1 month)

- [ ] **Silver Layer** — Data transformation & enrichment
  - Deduplicate based on event_id
  - Handle NULL values & type conversions
  - Write to Parquet with schema validation

- [ ] **Schema Registry** — Avro or Protobuf schemas
  - Evolve schema safely
  - Validate messages before upload

- [ ] **Azure Event Hubs** — Managed Kafka replacement
  - Switch from self-hosted Kafka to Event Hubs
  - Easier operational overhead

### Advanced (2+ months)

- [ ] **Orchestration** — Apache Airflow or Azure Data Factory
  - Trigger pipeline on schedule
  - Monitor producer/consumer health
  - Auto-restart on failure

- [ ] **Containerized Deployment** — Docker images for producer/consumer
  - Push to Azure Container Registry
  - Deploy to Azure Container Apps or AKS

- [ ] **Real-Time Lakehouse** — Delta Lake integration
  - Write to Delta Lake format (ACID semantics)
  - Enable time-travel queries
  - Integration with Databricks

- [ ] **Monitoring & Observability**
  - Add Prometheus metrics
  - Set up Grafana dashboards
  - Alert on lag or failure

---

## License

This project is licensed under the **MIT License**. See `LICENSE` file for details.

You're free to use, modify, and distribute this code for personal, educational, or commercial purposes.

---

## Contributing

Contributions are welcome! Please feel free to:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit changes (`git commit -m 'Add your feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Open a Pull Request

For major changes, open an issue first to discuss proposed changes.

---

## Support

If you encounter issues or have questions:
1. Check the [Troubleshooting](#cleanup--troubleshooting) section
2. Review container logs: `docker-compose logs`
3. Open an issue on GitHub with:
   - Error message (full stack trace)
   - Steps to reproduce
   - Environment (OS, Python version, Docker version)

---

**Happy streaming! 🚀**
