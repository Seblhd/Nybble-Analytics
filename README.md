# Nybble Security Analytics (NSA)

**Nybble Security Analytics (NSA)** is a NextGen SIEM built on modern streaming technologies—**Apache Kafka** and **Apache Flink**—to process very large volumes of security telemetry in **true real time**.

NSA is designed for teams that need fast, scalable detection and investigation pipelines, and who want to combine **real-time analytics** with the ability to **reprocess historical data** when new threats emerge.

---

## Key Capabilities

### 🔁 Event Replay (Kafka + Flink)
NSA can replay historical logs using Kafka retention and Flink processing.  
This is especially useful when a newly exploited **zero-day** or **CVE** appears: you can re-run past events with new detection logic to determine whether the vulnerability was exploited **before it was publicly known**.

### 🧩 Sigma-Native Correlation
The correlation engine is **Sigma-native**, meaning:
- No rule conversion pipeline required
- Use Sigma detection rules **directly**
- Faster rule onboarding and sharing

### ⏱️ Complex Event Processing (CEP) with Stateful Detection
Using Flink state, NSA supports **complex, multi-step scenarios** across:
- multiple data sources
- long time windows (from **hours to weeks**)

This enables detections that require context, sequences, and time-based behavior—not just single-event matches.

### ⚡ Real-Time Parsing, Normalization & Enrichment
NSA performs data processing on the fly:
- Parsing
- Normalization
- Enrichment

All done in-stream, with minimal latency.

---

## High-Level Architecture

- **Apache Kafka**: ingestion and data pipeline (durable, scalable streaming backbone)
- **Apache Flink**: real-time processing + correlation engine + CEP/stateful logic
- **Elasticsearch**: storage and indexing for logs and alerts (search/analytics)

---

## Project Status

> This repository contains the NSA project documentation and implementation artifacts.  
> Nybble Security Analytics is currently a PoC/MVP
> Project not maintained at this time

---

## Getting Started

### Prerequisites
- Apache Kafka 
- Apache Flink
- Elasticsearch

---

## Configuration (placeholder)

Common configuration areas:
- Kafka brokers + topics (raw events, normalized events, alerts)
- Flink job parameters (parallelism, checkpoints, state backend)
- Elasticsearch endpoints + index templates
- Enrichment sources (CMDB / IPAM / IAM / Threat Intel)

---

## Detection & Correlation

### Sigma Rules
- Store Sigma rules in `rules/`
- NSA loads and evaluates Sigma rules natively


### CEP Scenarios
CEP rules/scenarios typically define:
- event sequences
- temporal constraints
- cross-source joins
- stateful thresholds

Compatiable with Sigma Correlation rules.
