# 🚀 SpaceX ETL Pipeline with PostgreSQL & Airflow 🌌

## TL;DR  
This project extracts data from the SpaceX API, loads it into a local PostgreSQL database, transforms it using dbt, and orchestrates everything with an Airflow DAG. The entire pipeline runs inside Docker containers (via Docker Compose) for local development. PostgreSQL credentials are managed through environment variables for flexibility and reusability.

---

## 🌐 Airflow Web UI

![Airflow UI](airflow-web-ui.png)

---

## 📊 Architecture Overview

### 1. SpaceX API 🚀  
- **What:** Public API providing real-time launch and mission data from SpaceX.  
- **Role:** Primary data source for the ETL pipeline.

### 2. Data Extraction Scripts 📥  
- **What:** Python modules that retrieve data from the SpaceX API.  
- **Role:** Extract structured data and persist it to a local CSV file.

### 3. PostgreSQL Loader 🔄  
- **What:** Python logic that reads the CSV and loads it into a PostgreSQL database.  
- **Role:** Populate a raw data table used as a base for transformations.

### 4. Data Transformation with dbt 🧹  
- **What:** A `dbt` project to create staging and analytics tables inside PostgreSQL.  
- **Role:** Transform raw data into structured layers following best practices (e.g., medallion architecture).

### 5. Orchestration with Airflow ⏱️  
- **What:** An Airflow DAG manages the end-to-end process: extraction, loading, and transformation.  
- **Role:** Automates and monitors the pipeline workflow.

### 6. Local Development with Docker Compose 🐳  
- **What:** Docker Compose configuration to spin up all services: Airflow, PostgreSQL, and supporting tools.  
- **Role:** Simplifies the local development and testing workflow.

### 7. Environment Variables & Configuration 📦  
- **What:** PostgreSQL credentials and paths are configured using `.env` files or Docker Compose environment variables.  
- **Role:** Promote reusability and avoid hardcoding sensitive or environment-specific values.

### 8. Entrypoint Script (`entrypoint.sh`) 🛠️  
- **What:** Shell script to bootstrap Airflow (initialize DB, parse DAGs, run scheduler, etc.)  
- **Role:** Ensures containers are ready to execute workflows on start.

---

## 🚀 Running Locally

### 1. Clone the Repository  
```bash
git clone https://github.com/brunolnetto/spacex-dbt-mvp.git
cd spacex-dbt-mvp
```

### 2. Start the Stack  
```bash
docker-compose up --build
```

### 3. Access Airflow  
- URL: [http://localhost:8080](http://localhost:8080)  
- Default credentials: `airflow / airflow`

---

## 🧪 Airflow DAG Tasks

1. `extract_and_load_to_csv` – Fetch data from the SpaceX API and write it to a CSV file.
2. `load_to_postgres` – Load CSV data into the raw table in PostgreSQL.
3. `run_dbt_models` – Run dbt models that transform the raw table into refined datasets.

---

## 💡 Improvements in Progress

- ✅ Switch from row-by-row inserts to bulk inserts using `executemany()`.
- 🔒 Environment-variable driven configuration.
- 📈 Add test coverage and CI integration.

