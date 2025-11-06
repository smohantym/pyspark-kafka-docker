## 📘 Spark + Kafka Streaming Pipeline (Dockerized)

### 🧩 Overview

This project spins up a **complete local streaming environment** using:

* **Apache Kafka** & **Zookeeper** (via Confluent images)
* **Apache Spark 3.5.1** (official multi-arch Python3 image)
* A **Python Kafka Producer** sending JSON events
* A **Spark Structured Streaming Job** consuming from Kafka, writing to:

  * Parquet (`data/output/parquet/`)
  * CSV (`data/output/csv/`)
  * Console (for debug)

> ✅ Tested on Mac M1/M2/M3 (Apple Silicon) & Intel Docker Desktop

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
* Ports free: 2181, 9092, 9094, 7077, 8080, 8081
* Internet access (for pulling images and Spark packages)

---

### 🔧 Environment Configuration (`.env`)

```env
# Kafka & Spark environment
KAFKA_TOPIC=events
MSGS_PER_SEC=5
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
OUTPUT_PATH=/opt/spark-output/parquet
```

You can adjust message rate or topic name easily here.

---

### 🧱 Build & Run

```bash
# Build and start everything
docker compose up -d --build

# View Spark logs (streaming job)
docker compose logs -f spark-streaming

# View Producer logs
docker compose logs -f producer
```

---

### 🔍 Verify Setup

**List Kafka Topics:**

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

**Inspect Output Files (on Host):**

```bash
ls data/output/parquet/dt=*
ls data/output/csv/dt=*
```

**Spark UI:** [http://localhost:8080](http://localhost:8080)
**Worker UI:** [http://localhost:8081](http://localhost:8081)

---

### 💡 Data Flow

1. **Producer** emits JSON messages to Kafka topic `events`

   ```json
   {
     "event_id": "evt-123",
     "ts": "2025-11-06T12:30:00Z",
     "value": 45.67,
     "source": "demo-producer"
   }
   ```

2. **Kafka Broker** stores and serves them to subscribers.

3. **Spark Structured Streaming** reads live events, parses JSON,
   and writes results to:

   * **Console** (stdout)
   * **Parquet** (`data/output/parquet/dt=YYYY-MM-DD`)
   * **CSV** (`data/output/csv/dt=YYYY-MM-DD`)

---

### 🧠 Key Files

#### `spark/app.py`

* Reads from Kafka topic
* Parses JSON schema
* Writes to Parquet and CSV simultaneously
* Prints stream records to console
* Uses checkpointing for exactly-once semantics

#### `producer/producer.py`

* Uses `kafka-python` to send random events
* Rate-controlled via `MSGS_PER_SEC` from `.env`
* Automatically connects to `kafka:9092`

#### `docker-compose.yml`

* Manages Zookeeper, Kafka, Spark (master/worker/streaming), and Producer
* Uses official `apache/spark:3.5.1-python3` image
* Auto-creates Kafka topic via `kafka-init`

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

---

### 🧼 Stop & Cleanup

```bash
docker compose down -v
docker system prune -f
```
---
### ✨ Author

**Sandeep Mohanty**
Kafka + PySpark Streaming Pipeline (2025)
