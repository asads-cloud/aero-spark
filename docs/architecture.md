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
