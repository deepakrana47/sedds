## Project Overview:
You are working for a large multinational bank that maintains a Master Data Management (MDM) platform. The MDM holds the "golden copy" of critical business entity data like clients, employees, products, and facilities. It does not manage transactional data, only entity information.

Currently, multiple downstream systems across the bank require access to the MDM data for building BI reports, dashboards, and predictive analytics. These systems need local copies of entity data because real-time API calls to MDM are not scalable or efficient for heavy read and analytical workloads.

## Baseline solution 
having each downstream system connect directly to MDM vai API.
is not scalable and puts unnecessary load on the MDM system.

## Problem Statement:
Direct connections to MDM for each downstream system are not scalable. Need a centralized, efficient method to distribute entity data to multiple downstream consumers without overloading MDM.

## System Architecture:
* MDM → Hive Tables (Daily) → Spark Processing → Kafka Topics → Downstream Systems
  - MDM → Exports complete daily entity data to Hive tables in Hadoop.
  - Spark Application → Reads, processes, and publishes data to Kafka.
  - Kafka Topics → Downstream systems subscribe and consume data as needed.

## 
