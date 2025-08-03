# src/transform.py

import pandas as pd
import re
import uuid
from datetime import datetime
from src.logger import get_logger

logger = get_logger("DataTransformer")

# ----------------------
# 3.1 Data Cleansing
# ----------------------
def remove_duplicates(df, subset=None):
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    after = len(df)
    logger.info(f"🧹 Removed {before - after} duplicate rows.")
    return df

def standardize_names(df, columns=["FirstName", "LastName", "MiddleName"]):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.title()
    return df

def clean_phone_numbers(df, phone_column="PhoneNumber"):
    def format_phone(p):
        p = re.sub(r"\D", "", str(p))
        return f"+1-{p[-10:-7]}-{p[-7:-4]}-{p[-4:]}" if len(p) >= 10 else None
    df[phone_column] = df[phone_column].apply(format_phone)
    return df

def validate_emails(df, email_col="Email"):
    if email_col not in df.columns:
        return df
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    df["EmailValid"] = df[email_col].apply(lambda x: bool(re.match(pattern, str(x))))
    return df

def flag_quality_issues(df):
    df["DataQualityFlag"] = df.isnull().any(axis=1).astype(int)
    return df

# ----------------------
# 3.2 Business Logic
# ----------------------
def calculate_age(df, dob_column="DOB"):
    today = pd.Timestamp(datetime.today().date())
    df["Age"] = pd.to_datetime(df[dob_column], errors='coerce').apply(lambda x: (today - x).days // 365 if pd.notnull(x) else None)
    return df

def compute_coverage(df):
    df["CoveragePercent"] = (df["PaidAmount"] / df["Amount"]).round(2).fillna(0)
    return df

def categorize_payment_status(df):
    def status(row):
        if pd.isnull(row["PaidAmount"]):
            return "Pending"
        elif row["PaidAmount"] == 0:
            return "Denied"
        elif row["PaidAmount"] < row["Amount"]:
            return "Partial"
        else:
            return "Paid"
    df["PaymentStatus"] = df.apply(status, axis=1)
    return df

def add_time_dimensions(df, date_col="ServiceDate"):
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["Year"] = df[date_col].dt.year
    df["Month"] = df[date_col].dt.month
    df["Quarter"] = df[date_col].dt.quarter
    df["Weekday"] = df[date_col].dt.day_name()
    return df

# ----------------------
# 3.3 Common Data Model
# ----------------------
def normalize_patient_data(df):
    df = remove_duplicates(df, subset=["PatientID"])
    df = standardize_names(df)
    df = clean_phone_numbers(df)
    df = calculate_age(df)
    df = flag_quality_issues(df)
    return df

def standardize_procedure_codes(df: pd.DataFrame) -> pd.DataFrame:
    if "ProcedureCode" in df.columns:
        df["ProcedureCode"] = df["ProcedureCode"].astype(str).str.upper().str.strip()
    if "ProcedureDescription" in df.columns:
        df["ProcedureDescription"] = df["ProcedureDescription"].astype(str).str.title().str.strip()
    return df


def generate_transaction_keys(df):
    df["TransactionKey"] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

# Optional: Wrapper functions

def transform_patients(df):
    logger.info("✨ Transforming patient data")
    return normalize_patient_data(df)

def transform_transactions(df):
    logger.info("💸 Transforming transaction data")
    df = standardize_procedure_codes(df)
    df = compute_coverage(df)
    df = categorize_payment_status(df)
    df = add_time_dimensions(df)
    df = generate_transaction_keys(df)
    return df
