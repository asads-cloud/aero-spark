flowchart LR
  %% Aero Spark — Architecture

  subgraph Ingest["Ingest"]
    SRC[Source CSVs (Flight delays)]
    S3RAW[(Amazon S3\nraw/ingest_date=YYYY-MM-DD/)]
  end

  subgraph Orchestration["Orchestration"]
    MWAA[Airflow on AWS MWAA\n(DAGs, scheduling)]
  end

  subgraph Compute["Transform"]
    DBX[Databricks Spark Jobs\n(ETL/validation)]
  end

  subgraph Storage["Curated Storage"]
    S3PROC[(Amazon S3\nprocessed/\npartitioned: year/month/day/origin)]
  end

  subgraph CatalogQuery["Catalog & Query"]
    CRAWLER[AWS Glue Crawler\n(update partitions/schema)]
    CATALOG[AWS Glue Data Catalog]
    ATHENA[Athena / SQL\n+ Downstream ML]
  end

  subgraph Governance["Governance & Ops"]
    IAM[IAM Roles & Permissions]
    CW[CloudWatch Logs (MWAA/Jobs)]
    POL[S3 bucket policies\n(versioning, SSE-S3)]
  end

  %% Flows
  SRC -->|upload| S3RAW
  MWAA -->|trigger job| DBX
  S3RAW -->|read| DBX
  DBX -->|write Parquet| S3PROC
  S3PROC --> CRAWLER --> CATALOG --> ATHENA

  %% Visibility / Controls
  MWAA --- CW
  DBX --- CW
  IAM --- MWAA
  IAM --- DBX
  POL --- S3RAW
  POL --- S3PROC
rnrn<!-- BEGIN:AERO-SPARK-ARCH v1 -->

# Aero Spark — Phase 1 Architecture

## Overview
This pipeline ingests raw flight-delay CSVs to S3 **raw**, validates basic structure, then (in later phases) transforms to partitioned Parquet for query/ML.

```mermaid
flowchart TD

  %% Sources
  A[Local CSVs\n(data/samples)] -->|aws s3 sync| B[S3 Bucket\n`aero-spark-data-uc3pslda`]

  %% Raw zone
  subgraph Z1[Raw Zone (Landing)]
    B --> B1[raw/flight_delays/\ningest_date=YYYY-MM-DD/]
  end

  %% Validation (Phase 2+)
  B1 -->|schema checks (27 cols)\nnullability & types| C[Validation (Great Expectations)]

  %% Transform (Phase 2+)
  C --> D[Spark job | Databricks Jobs\n(dbx-aero profile)]
  D --> E[S3 Processed (Parquet)\nprocessed/flight_delays/\nyear=YYYY/month=MM/day=DD/origin=XXX/]

  %% Catalog & Query (Phase 2+)
  E --> F[Glue Crawler]
  F --> G[Glue Data Catalog]
  G --> H[Athena SQL]
  E --> I[ML Consumers (notebooks/training)]

  %% Orchestration & Observability (Phase 2+)
  subgraph Z2[Orchestration & Observability]
    J[MWAA (Airflow DAG)] -->|schedule & tasks| A
    J --> C
    J --> D
    J --> F
    K[CloudWatch Logs & Metrics]
    J -. emits logs .-> K
    D -. job logs .-> K
    F -. crawler logs .-> K
  end

  %% Notes/Styling
  classDef zone fill:#f6f8fa,stroke:#d0d7de,rx:6,ry:6;
  class Z1,Z2 zone;
Data Zones
Zone    Purpose    Storage Layout Example    Format    Retention (placeholder)
raw    Immutable landings by ingest_date    s3://aero-spark-data-uc3pslda/raw/flight_delays/ingest_date=2025-10-20/    CSV (UTF-8)    ≥ 90 days
processed    Curated, query-ready partitions    s3://aero-spark-data-uc3pslda/processed/flight_delays/year=2025/month=10/day=20/origin=LHR/    Parquet    ≥ 1 year

Key Conventions
All “local” timestamps have no TZ suffix; only ingest_ts is UTC (Z).

Proposed primary key: flight_date, origin, dest, airline_code, flight_number, scheduled_dep.

Processed partitions: year, month, day, origin (derived from flight_date, origin).

Phase 1 Scope
Focus on schema and data movement to raw.

Databricks, Airflow, and GE validation are diagrammed but implemented in later phases.

<!-- END:AERO-SPARK-ARCH v1 -->
