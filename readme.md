# Real-Time Clickstream Pipeline

A complete data pipeline that ingests continuous user click events, stores raw data in a Data Lake, and processes historical data nightly for consumer trends.

**Tech Stack:** Python FastAPI, Apache Kafka, Apache Storm, Apache Spark, HDFS

---

## How the tools fit together

- **Data Lake / HDFS (Bronze Layer)** — Raw JSON clickstream logs land here first, partitioned by date using Hive-style directories (`year=/month=/day=`). Nothing is transformed; it's the source of truth.

- **Apache Storm (Speed Layer / Silver)** — Consumes the live Kafka stream for real-time processing. If a user is performing malicious activity, Storm immediately flags and fires an alert (sub-second latency).

- **Apache Spark (Batch Layer / Gold)** — Runs daily scheduled jobs over historical data in HDFS to build recommendation models (e.g. "People who bought X also bought Y") and aggregate daily activity reports.

---

## Architecture

```
[ Browser / Website ] (JavaScript Tracker)
        │
        ▼  (HTTP POST / Beacon API)
[ NGINX ] (SSL termination, security headers)
        │
        ▼  (Reverse Proxy)
[ Ingestion API ] (Python FastAPI)
        │
        ▼  (Async Producer Push)
[ Apache Kafka ] (Event Backbone / Buffer)
        │
   ┌────┴────────────────────────┐
   ▼                             ▼
[ Apache Storm ]          [ Data Lake Connector ]
 Speed / Silver Layer      Kafka → HDFS consumer
                                │
                                ▼
                        [ HDFS / NameNode + DataNode ]
                         Bronze Layer (raw NDJSON)
                                │
                                ▼
                        [ Apache Spark ] (Batch / Gold Layer)
```

---

## Medallion Layers

The pipeline is built incrementally across three layers:

1. **Bronze** — Raw, unmodified events written to HDFS as NDJSON, partitioned by date.
2. **Silver** — Real-time fraud detection and event enrichment via Apache Storm.
3. **Gold** — Aggregated reports and ML features produced nightly by Apache Spark.

---

## Steps to run

**1.** Deploy the dummy site (`dummy-site/index.html` + `js/tracker.js`)with nginx or any server of your choice keeping in mind security.

**2.** Start the local stack — everything below runs via Docker Compose for dev ease:

```bash
docker compose up --build
```

**3.** The compose starts the ingestion server which collects click events and pushes them into Kafka. This layer could be autoscaled as load increases, and Kafka could have its own dedicated cluster. Although this is a portfolio project, the ingestion layer still has three defenses at its boundary:

- **CORS** — only accepts requests from your frontend domain
- **Schema validation** — rejects malformed or oversized payloads before they hit Kafka, protecting the data lake from garbage data
- **Rate limiting** — 100 requests / 60 min per IP, prevents DDoS or event-spamming scripts from overwhelming the message broker

**4.** *(Personal setup only — skip if your site and ingestion server are on the same machine)* Since the dummy site is hosted on a remote server but the ingestion server runs locally on a MacBook, an SSH reverse tunnel bridges the two. It lets NGINX on the VPS forward `/api/collect` requests to the local machine without exposing the MacBook directly to the internet — no open inbound ports, no firewall rules. The connection is initiated outbound from the Mac, so the outside world never reaches it directly:

```bash
ssh -R 127.0.0.1:8080:localhost:8080 vps
```

**5.** Events flow from Kafka into HDFS via the `data-lake-connector` service. It batches events (flush at 50 events or every 10 seconds, whichever comes first) and writes NDJSON files under:

```
/lake/raw/clickstream/year=YYYY/month=MM/day=DD/batch_<timestamp>.json
```

> **Note on production:** In a real deployment this connector would be replaced with Kafka Connect (HDFS Sink Connector) — no custom consumer code needed. The `data-lake-connector` folder exists to make the data flow explicit and self-contained for this portfolio project.
