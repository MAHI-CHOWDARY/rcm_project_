# data_extractor.py

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from config.settings import MYSQL_CONFIG
from src.logger import get_logger
import os
import time

logger = get_logger("DataExtractor")

REQUIRED_CLAIMS_COLUMNS = {"ClaimID", "PatientID"}

class DataExtractor:
    def __init__(self):
        self.connections = {}

    def connect(self, source_name):
        cfg = MYSQL_CONFIG[source_name]
        try:
            conn = mysql.connector.connect(**cfg)
            self.connections[source_name] = conn
            logger.info(f"✅ Connected to {source_name} database.")
        except Exception as e:
            logger.error(f"❌ Connection failed for {source_name}: {e}")
            raise

    def extract_patients(self, source_name):
        start = time.time()
        self.connect(source_name)
        query = "SELECT * FROM patients"
        df = pd.read_sql(query, self.connections[source_name])
        df["source"] = source_name
        duration = round(time.time() - start, 2)
        logger.info(f"📄 Extracted {len(df)} patients from {source_name} in {duration}s.")
        return df

    def extract_transactions(self, source_name, start_date=None, end_date=None):
        self.connect(source_name)
        query = "SELECT * FROM transactions"
        if start_date and end_date:
            query += f" WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'"
        df = pd.read_sql(query, self.connections[source_name])
        df["source"] = source_name
        logger.info(f"📄 Extracted {len(df)} transactions from {source_name}.")
        return df

    def extract_claims_csv(self, folder_path):
        all_claims = []
        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(folder_path, filename)
                try:
                    df = pd.read_csv(file_path)
                    if "PatientID" in df.columns:
                        # Add 'source' based on filename (e.g., "claims_hospital_a.csv")
                        source = "hospital_a" if "a" in filename.lower() else "hospital_b"
                        df["source"] = source
                        all_claims.append(df)
                        logger.info(f"✅ Loaded {len(df)} records from {filename}")
                    else:
                        logger.warning(f"⚠️ Skipping {filename} — 'PatientID' column missing")
                except Exception as e:
                    logger.error(f"❌ Error reading {filename}: {e}")
        return pd.concat(all_claims, ignore_index=True)


    def unify_patients(self, df_a, df_b):
        df = pd.concat([df_a, df_b], ignore_index=True)

        if "unified_patient_id" not in df.columns and "PatientID" in df.columns:
            df["unified_patient_id"] = df["source"] + "_" + df["PatientID"].astype(str)

        logger.info(f"🧩 Combined patient records: {len(df)} with unified_patient_id.")
        return df

    def unify_transactions(self, df_a, df_b):
        df = pd.concat([df_a, df_b], ignore_index=True)

        # ✅ Assign unified_patient_id using source and PatientID
        df["unified_patient_id"] = df["source"] + "_" + df["PatientID"].astype(str)

        logger.info(f"🧾 Combined transaction records: {len(df)} with unified_patient_id added")

        return df


    def standardize_patient_schema(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        logger.info(f"🧼 Standardizing patient schema for {source}")

        if source == "hospital_b":
            rename_map = {
                "ID": "PatientID",
                "F_Name": "FirstName",
                "L_Name": "LastName",
                "M_Name": "MiddleName"
            }
            df = df.rename(columns=rename_map)

        required_columns = [
            "PatientID", "FirstName", "LastName", "MiddleName",
            "SSN", "PhoneNumber", "Gender", "DOB",
            "Address", "ModifiedDate", "source"
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        return df[required_columns]
