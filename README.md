# 🚀 SpaceX ETL Pipeline 🌌

Welcome to the **SpaceX ETL Pipeline** project! This repository contains an end-to-end data pipeline that extracts, transforms, and loads SpaceX launch data using Python, dbt, and Google Cloud Run.

---

## 📋 Overview

This project is designed to:

- **Extract** data from the SpaceX API 🚀
- **Load** data into Google Cloud Storage (GCS) and BigQuery 📊
- **Transform** data using dbt 🧹
- **Schedule** daily runs with Cloud Scheduler ⏰
- **Deploy** as a serverless container on Cloud Run for maximum cost efficiency 🌱

---

## 🧠 Architecture

The pipeline runs as a containerized Python application triggered daily by an HTTP request from Cloud Scheduler. The main cloud components:

- **Docker Container** 🐳 – Bundles your DAG logic, dbt project, and extraction scripts.
- **Cloud Run** ☁️ – Executes the container on demand in a serverless, autoscaling environment.
- **Cloud Scheduler** 📆 – Triggers the Cloud Run endpoint once a day via HTTP.

---

## 🛠️ Prerequisites

Before you begin, make sure you have:

- A **GCP project** with billing enabled 💳
- Installed the [gcloud CLI](https://cloud.google.com/sdk)
- Installed [Docker](https://www.docker.com/)
- Enabled these GCP APIs:
  - Cloud Run
  - Artifact Registry or Container Registry
  - Cloud Scheduler
  - Cloud Build (optional for CI/CD)
  - BigQuery
  - Cloud Storage

---

## 🚦 Step-by-Step Deployment Guide

### ✅ Step 1: Clone the Repository

```bash
git clone https://github.com/vzucher/spacex.git
cd spacex
