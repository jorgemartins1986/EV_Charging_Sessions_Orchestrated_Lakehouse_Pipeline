# EV Charging Sessions — Orchestrated Lakehouse Pipeline

## 📌 Overview
This project models **user behavior and station utilization** from EV charging sessions data.  
It uses a fully orchestrated AWS pipeline with Terraform IaC, PySpark ETL, and modern data formats.

**Core Features**:
- End-to-end AWS ETL using Glue, S3, Athena, DynamoDB, Step Functions
- Data quality gates with Deequ
- Storage in **Parquet** for analytics and **Apache Iceberg** for curated, mutable datasets
- Automated orchestration and monitoring
- CI/CD with GitHub Actions

---

## 🛠 Tech Stack
- **AWS S3** — data lake storage
- **AWS Glue** — ETL jobs (PySpark + Deequ)
- **AWS Step Functions** — orchestration
- **AWS Lambda** — lightweight tasks
- **AWS Athena** — SQL queries on S3
- **AWS DynamoDB** — utilization lookups
- **AWS CloudWatch** — logs + alarms
- **Terraform** — infrastructure as code
- **GitHub Actions** — CI/CD automation
- **Formats** — Parquet + Apache Iceberg
- **(Optional)** — QuickSight dashboards

---

## 📂 Repository Structure
(see tree above)

---

## 🚀 Workflow
1. **Data Ingestion** — Raw CSV uploaded to S3 (`raw/ev_sessions/`)
2. **Trigger** — EventBridge starts Step Functions state machine
3. **ETL** — Glue job cleans, transforms, validates with Deequ, writes to Parquet + Iceberg
4. **Aggregation** — Utilization metrics computed and stored in DynamoDB
5. **Validation** — Athena runs sanity queries
6. **Notification** — Success/failure alerts via SNS

---

## 📊 Key Metrics
- Avg session duration per site
- Peak hours and utilization ratios
- Connector type usage distribution

---

## 📈 Architecture
*(diagram placeholder — to be added in `docs/architecture.png`)*

---

## 🏗 Setup & Deployment
1. **Clone repo**
2. **Configure AWS credentials**
3. **Deploy infrastructure**
   ```bash
   cd infra
   terraform init
   terraform apply
4. **Upload sample CSV to raw bucket**
5. **Trigger Step Functions execution**

---

## 🧪 Local Development
- Run PySpark jobs locally against /data-samples
- Validate Deequ checks before deploying to Glue

## 📜 License
**MIT License**