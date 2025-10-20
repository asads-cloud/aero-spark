# Aero Spark — Baseline Handoff Summary (Setup & Planning)

**Date (UTC):** 2025-10-20 12:56:31  
**AWS Account:** 155186308102  
**AWS Profile:** aero-admin  
**Region:** eu-west-1  
**Invoker ARN:** arn:aws:iam::155186308102:user/aero-admin

## Repositories
- GitHub: \aero-spark\ (branch: \main\)

## S3
- Data bucket: \$BucketName\
  - Raw prefix: \s3://aero-spark-data-uc3pslda/raw/\
  - Processed prefix: \s3://aero-spark-data-uc3pslda/processed/\

## Tooling
- Python: Python 3.11.9
- Databricks CLI: Databricks CLI v0.273.0
- Terraform: Terraform v1.13.3

## Auth Profiles
- AWS CLI profile: \$Profile\ (region \$Region\)
- Databricks CLI profile: \dbx-aero\

## Artifacts
- Diagram: \docs/architecture.md\ (Mermaid)
- CI: \.github/workflows/ci.yml\
- Dev env: \.venv\, \
equirements*.txt\, \.pre-commit-config.yaml\

## Next stages — Data & Schema Design (preview)
- Choose source CSV schema (flight delays)
- Define raw → curated schema mapping
- Decide partitioning: \year/month/day/origin\
- Draft Great Expectations suites for raw validation
