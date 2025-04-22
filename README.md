

# 🌀 Airflow + DBT + Metabase Stack



This project provides a production-like local environment using **Apache Airflow**, **dbt**, **Redis**, **PostgreSQL**, and **Metabase**, all orchestrated via **Docker Compose**.



> Ideal for modern data workflow development, scheduling, and exploration.


---

## 🚀 Quickstart

### 1. Clone the repository

```bash
git clone https://github.com/your-org/your-repo.git
cd your-repo
```

### 2. Set environment variables

Create a `.env` file based on `.env.example`:

```bash
cp .env.example .env
```

Fill in the required credentials like `POSTGRES_USER`, `POSTGRES_PASSWORD`, etc.

### 3. Start the services

```bash
docker-compose up --build
```

### 4. Access the interfaces

| Service     | URL                    | Description                        |
|-------------|------------------------|------------------------------------|
| Airflow UI  | http://localhost:8080  | DAG orchestration & monitoring     |
| Flower      | http://localhost:5555  | Celery task queue monitoring       |
| Metabase    | http://localhost:3000  | Data exploration & BI dashboards   |
| Minio       | http://localhost:9090  | S3-like file storage system        |
| PGAdmin     | http://localhost:5050  | DBMS tool for Postgres database    |

---

## 🧱 Stack Components

- **Airflow**: Workflow orchestration engine;
- **Redis**: Message broker for Celery;
- **PostgreSQL**: Metadata DB for Airflow and Metabase;
- **dbt**: SQL-based data transformation (mounted into Airflow);
- **Metabase**: BI platform for querying and dashboards;
- **Minio**: S3-like storage service for unstructured data;
- **PgAdmin**: Web-based DBMS tool for Postgres;
- **Flower**: Real-time monitoring of Celery workers.

---

## 📁 Project Structure

```bash
.
├── dags/           # Airflow DAGs
├── dbt/            # dbt project
├── .env            # Environment variables
├── Dockerfile      # Custom Airflow image
├── docker-compose.yml
└── entrypoint.sh   # Airflow/dbt initialization script
```

---

## 🛠️ Useful Commands

### Rebuild services after changes

```bash
docker-compose down
docker-compose up --build
```

### Shut everything down

```bash
docker-compose down -v
```

---

## ✅ Health & Logs

Each service includes Docker health checks and basic logging:

- Logs are rotated (`max-size=5m`, `max-file=2`)
- Health checks ensure services wait for dependencies (e.g., Postgres before Airflow)

---

## 🧪 Tip: Local Testing

You can test your DAGs or dbt models directly inside the container:

```bash
docker exec -it airflow-webserver bash
airflow dags list
dbt run
```

---

## 🧯 Troubleshooting

- **Airflow not starting?** Check PostgreSQL health in logs.
- **Metabase errors?** Ensure `MB_DB_*` env vars are correctly set.
- **Volumes not syncing?** Restart Docker and try again.

---


