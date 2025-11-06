## 📘 Spark + Kafka Streaming Pipeline (Dockerized)

### 🧩 Overview

This project sets up a **complete local streaming environment** using:

* **Apache Kafka** & **Zookeeper** (via Confluent images)
* **Apache Spark 3.5.1** (official Python3 multi-arch image)
* A **Python Kafka Producer** sending random JSON events
* A **Spark Structured Streaming Job** consuming from Kafka and writing to:

  * Parquet (`data/output/parquet/`)
  * CSV (`data/output/csv/`)
  * Console (for quick inspection)

> ✅ Fully tested on Mac M1/M2/M3 (Apple Silicon) and Intel Docker Desktop.

---

### 📂 Project Structure

```
spark-kafka-pipeline/
├── docker-compose.yml
├── .env
├── requirements.txt
├── .gitignore
├── spark/
│   └── app.py
└── producer/
    ├── Dockerfile
    ├── requirements.txt
    └── producer.py
```

---

### ⚙️ Prerequisites

* Docker Desktop ≥ 4.x
* Free ports: 2181, 9092, 9094, 7077, 8080, 8081
* Internet access for pulling images and Spark Kafka connector packages

---

### 🔧 Environment Configuration (`.env`)

```env
# Kafka & Spark core settings
KAFKA_TOPIC=events
MSGS_PER_SEC=5
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
OUTPUT_PATH=/opt/spark-output/parquet

# Spark streaming control
TRIGGER_SECONDS=30            # time interval between batches
MAX_RECORDS_PER_FILE=5000     # max records per file
FILES_PER_PARTITION=1         # number of output files per day (dt) partition
```

You can tune these parameters to balance data freshness and file count.
For example, increasing `TRIGGER_SECONDS` to 60 or 120 will create fewer files.

---

### 🧱 Build & Run

```bash
# Build and start all containers
docker compose up -d --build

# Watch Spark streaming logs
docker compose logs -f spark-streaming

# Watch Kafka producer logs
docker compose logs -f producer
```

---

### 🔍 Verify Setup

**List Kafka Topics**

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

**Inspect Output Files (on Host)**

```bash
ls data/output/parquet/dt=*
ls data/output/csv/dt=*
```

**Web Interfaces**

* Spark Master UI → [http://localhost:8080](http://localhost:8080)
* Spark Worker UI → [http://localhost:8081](http://localhost:8081)

---

### 💡 Data Flow

1. **Producer** emits JSON messages to the Kafka topic defined in `.env` (default: `events`):

   ```json
   {
     "event_id": "evt-001",
     "ts": "2025-11-06T12:30:00Z",
     "value": 45.67,
     "source": "demo-producer"
   }
   ```

2. **Kafka Broker** stores and publishes messages to subscribers.

3. **Spark Streaming Job** reads events from Kafka, parses JSON, and writes to:

   * Console (for logs)
   * Parquet (`data/output/parquet/dt=YYYY-MM-DD`)
   * CSV (`data/output/csv/dt=YYYY-MM-DD`)

4. Output files are **batched every `TRIGGER_SECONDS` seconds**,
   with limits on **records per file** and **files per partition**
   (reducing the explosion of small files).

---

### 🧠 Key Files

#### `spark/app.py`

* Reads streaming data from Kafka
* Parses JSON messages with schema
* Writes to both **Parquet** and **CSV**
* Limits file count via:

  * `TRIGGER_SECONDS`
  * `MAX_RECORDS_PER_FILE`
  * `FILES_PER_PARTITION`
* Outputs to console for quick debugging
* Includes checkpointing for recovery and exactly-once semantics

#### `producer/producer.py`

* Sends random JSON messages to Kafka
* Controlled by `MSGS_PER_SEC` in `.env`
* Uses `kafka-python` client
* Auto-connects to the broker `kafka:9092`

#### `docker-compose.yml`

* Defines services: Zookeeper, Kafka, Spark Master/Worker, Producer
* Uses `apache/spark:3.5.1-python3` image (multi-arch)
* Automatically creates Kafka topic via `kafka-init` script

---

### 🗃 Output Folders (on Host)

```
data/output/
├── parquet/
│   ├── dt=2025-11-06/
│   │   └── part-00000-xxxx.snappy.parquet
│   └── _chk/
└── csv/
    ├── dt=2025-11-06/
    │   └── part-00000-xxxx.csv
    └── _chk/
```

Each partition folder (`dt=YYYY-MM-DD`) contains the data for that date.
File count is automatically controlled via `.env` parameters.

---

### 🧼 Stop & Cleanup

```bash
docker compose down -v
docker system prune -f
```

---

### 🧩 Tips & Troubleshooting

| Problem                 | Likely Cause                  | Solution                                                   |
| ----------------------- | ----------------------------- | ---------------------------------------------------------- |
| Too many output files   | Frequent micro-batches        | Increase `TRIGGER_SECONDS` or reduce `FILES_PER_PARTITION` |
| No data in Parquet/CSV  | Producer not sending messages | Check `docker compose logs -f producer`                    |
| Spark job not starting  | Kafka topic not yet created   | Restart `spark-streaming` after `kafka-init` finishes      |
| Slow performance on Mac | Emulation or heavy batching   | Adjust batch size or enable Rosetta in Docker              |

---

### ✨ Maintainer

**Sandeep Mohanty**
*Data Engineering Project — Kafka + PySpark Streaming Pipeline (2025)*

