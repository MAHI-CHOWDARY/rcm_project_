# 🏥 Healthcare Revenue Cycle Management (RCM) – Data Engineering Pipeline

This project implements an end-to-end data engineering pipeline for Healthcare Revenue Cycle Management (RCM). It integrates patient, transaction, and claim data from multiple hospitals into a unified data warehouse in **Google BigQuery** with proper dimensional modeling and SCD Type 2 tracking.

---

## 📌 Project Overview

**Goals:**
- Extract patient and transaction data from multiple sources
- Unify and clean data
- Apply SCD Type 2 to track historical patient changes
- Perform dimensional modeling (star schema)
- Load facts and dimensions into Google BigQuery
- Partition and cluster tables for performance

---

## 📁 Folder Structure

rcm_healthcare_project/
│
├── Data/ # Contains raw CSVs (claims, procedure codes)
│ ├── claims/
│ └── cptcodes.csv
│
├── src/ # Python source modules
│ ├── extract.py
│ ├── datacleaning.py
│ ├── dimensional.py
│ ├── scdtype2.py
│ ├── load.py
| ├── logger.py
│
├── run_pipeline.py # Main orchestration script
├── .env # Environment variables (not committed)
├── .gitignore
├── requirements.txt
└── README.md


---

## ⚙️ Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/MAHI-CHOWDARY/rcm_project_.git
   cd rcm_project_

2. **Create and activate a virtual environment**

    python -m venv venv
    source venv/bin/activate      # macOS/Linux
    venv\Scripts\activate         # Windows


3. **Install required dependencies**
    pip install -r requirements.txt

4. **Set up .env file**
    GOOGLE_APPLICATION_CREDENTIALS=your_gcp_service_account_key.json
    BQ_DATASET=datset_name
    BQ_PROJECTID=id

5. **Enable BigQuery API**

    Go to Google Cloud Console
    Enable the BigQuery API
    Make sure the service account has BigQuery Data Editor permissions



🚀 Run the Pipeline
    python run_pipeline.py


**This script performs the following phases:**

    Extraction: Loads data from hospitals and CSVs

    Transformation: Cleans and standardizes data

    Dimensional Modeling: Builds star schema (facts + dimensions)

    SCD Type 2: Tracks historical patient changes

    Loading: Pushes all tables to BigQuery