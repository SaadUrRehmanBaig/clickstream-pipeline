# Real-Time Clickstream Pipeline

A complete data pipeline that ingests continuous user click events, stores raw data in a Data Lake, and processes historical data nightly for consumer trends.

**Tech Stack:** Python FastAPI, Apache Kafka, Apache Storm, Apache Spark , HDFS

---

## How the tools fit together

- **Data Lake / HDFS** — Raw JSON clickstream logs are dumped directly into an HDFS-backed Data Lake.

- **Apache Storm (Speed Layer)** — Consumes the live clickstream stream for processing. If a user is performing some malacious activity Storm immediately flags and fires a real-time alert (sub-second latency).

- **Apache Spark (Batch Layer)** — Runs daily scheduled jobs over historical data in HDFS to build recommendation models (e.g. "People who bought X also bought Y") and aggregate daily activity reports.

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
[ Apache Storm ]          [ HDFS / Data Lake ]
 Real-time Fraud Layer     Batch / Bronze Layer
                                │
                                ▼  
                        [ Apache Spark] (batch layer)
```

---

## Steps to run

**1.** deploy a dummy site html, js with nginx or any server of your choice keeping in mind security.

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
