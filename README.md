# Data Platform MVP (E-Commerce Orders)

## 🧩 Problem Statement
Modern businesses generate a lot of data, but often:

- It lives in different systems and formats
- There are no rules to validate it
- Data pipelines break silently
- Reports and dashboards are built manually
- Teams don’t trust the numbers they see

In short: **The business has data, but it can’t rely on it or observe it when things go wrong.**

## 🏭 Solution Overview
This MVP is a **mini data factory for an online store** that:

- **Collects order events in real-time** using Kafka (the data delivery system)
- **Enforces rules on how data should look** using Schema Registry (the rule book)
- **Stores raw events as structured Avro files** in cloud storage (the warehouse)
- **Automates data processing** using Airflow + dbt (the factory managers and transformation engine)
- **Validates data quality before publishing** using Great Expectations + dbt tests (the quality control team)
- **Tracks failures and data lineage** using OpenLineage + Marquez (the pipeline CCTV system)
- **Defines business metrics as reusable code** using dbt MetricFlow (the official metric rule board)
- **Provisions all infrastructure automatically** using Terraform (the construction robots)

## 📊 Business Impact
This platform enables:

- Accurate **Revenue Reporting**
- Correct **Average Order Value (AOV)**
- Consistent **Order Tracking**
- Faster insights without manual work
- Early detection of bad data and pipeline failures
- Visibility into *where* and *why* things break
- A scalable, reproducible data setup like real companies use