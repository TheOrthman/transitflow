# TransitFlow Architecture

## Overview

TransitFlow is a batch-oriented data engineering platform for ingesting,
validating, transforming, publishing, cataloging, and querying NYC TLC
Yellow Taxi trip data.

The platform is designed around four main goals:

1. reliable ingestion
2. explicit data quality handling
3. safe reruns through idempotency
4. independently verifiable cloud outputs

---

## High-Level Architecture

```mermaid
flowchart LR
    A[NYC TLC Public Dataset] --> B[TLC Downloader]

    B --> C[Local Raw Parquet]

    C --> D[PySpark Processing]

    D --> E[Schema Validation]
    E --> F[Data Quality Rules]

    F --> G[VALID Records]
    F --> H[WARNING Records]
    F --> I[QUARANTINE Records]

    G --> J[Curated Output]
    H --> J

    I --> K[Quarantine Output]

    F --> L[Quality Metrics]

    C --> M[S3 Raw Layer]
    J --> N[S3 Curated Layer]
    K --> O[S3 Quarantine Layer]
    L --> P[S3 Metrics Layer]

    N --> Q[AWS Glue Data Catalog]
    Q --> R[Amazon Athena]

    R --> S[Reconciliation]
    S --> T[Recruiter Demo]

