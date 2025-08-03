# src/modeling.py

import pandas as pd
from datetime import datetime

# -----------------------------
# DIMENSION TABLES
# -----------------------------

def create_dim_patients(patients_df):
    dim_patients = patients_df.copy()
    dim_patients = dim_patients.reset_index(drop=True)
    dim_patients["patient_sk"] = dim_patients.index + 1  # surrogate key
    return dim_patients[["patient_sk", "unified_patient_id", "FirstName", "LastName", "MiddleName",
                         "Gender", "DOB", "SSN", "PhoneNumber", "Address", "source"]]

def create_dim_providers(transactions_df):
    dim_providers = transactions_df[["ProviderID"]].drop_duplicates().copy()
    dim_providers = dim_providers.reset_index(drop=True)
    dim_providers["provider_sk"] = dim_providers.index + 1
    return dim_providers[["provider_sk", "ProviderID"]]

def create_dim_procedures(transactions_df):
    dim_procedures = transactions_df[["ProcedureCode"]].drop_duplicates().copy()
    dim_procedures = dim_procedures.reset_index(drop=True)
    dim_procedures["procedure_sk"] = dim_procedures.index + 1
    return dim_procedures[["procedure_sk", "ProcedureCode"]]

def create_dim_date(df, date_columns):
    dates = pd.Series()
    for col in date_columns:
        dates = pd.concat([dates, pd.to_datetime(df[col], errors='coerce')])
    dates = pd.to_datetime(dates.dropna().unique())
    dim_date = pd.DataFrame({"date": dates})
    dim_date["date_sk"] = dim_date.index + 1
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["day_of_week"] = dim_date["date"].dt.dayofweek
    return dim_date[["date_sk", "date", "year", "month", "day", "quarter", "day_of_week"]]

# -----------------------------
# FACT TABLES
# -----------------------------

def create_fact_transactions(transactions_df, dim_patients, dim_providers, dim_procedures, dim_date):
    fact = transactions_df.copy()

    # Merge with dim_patients to get patient_sk
    fact = fact.merge(dim_patients[["unified_patient_id", "patient_sk"]], on="unified_patient_id", how="left")

    # Merge with dim_providers to get provider_sk
    fact = fact.merge(dim_providers, on="ProviderID", how="left")

    # Merge with dim_procedures to get procedure_sk
    fact = fact.merge(dim_procedures, on="ProcedureCode", how="left")

    # Merge with dim_date to get service_date_sk
    dim_date_renamed = dim_date.rename(columns={"date": "ServiceDate"})
    fact = fact.merge(dim_date_renamed[["ServiceDate", "date_sk"]], on="ServiceDate", how="left")
    fact = fact.rename(columns={"date_sk": "service_date_sk"})

    # Final selected columns (including surrogate keys and actual ServiceDate)
    return fact[[
        "TransactionID",
        "patient_sk",
        "provider_sk",
        "procedure_sk",
        "service_date_sk",
        "ServiceDate",            # ✅ Include actual date
        "Amount",
        "AmountType",
        "PaidAmount",
        "ClaimID",
        "PayorID",
        "VisitType"
    ]]

    fact = transactions_df.copy()

    fact = fact.merge(dim_patients[["unified_patient_id", "patient_sk"]], on="unified_patient_id", how="left")
    fact = fact.merge(dim_providers, on="ProviderID", how="left")
    fact = fact.merge(dim_procedures, on="ProcedureCode", how="left")

    dim_date_renamed = dim_date.rename(columns={"date": "ServiceDate"})
    
    
      # ✅ Step 3: Add ServiceDate back using the surrogate key
    fact = fact.merge(dim_date[["date_sk", "date"]], left_on="service_date_sk", right_on="date_sk", how="left")
    fact = fact.rename(columns={"date": "ServiceDate"})

    # Optional: Clean up
    fact.drop(columns=["date_sk"], inplace=True)

    return fact[["TransactionID", "patient_sk", "provider_sk", "procedure_sk", "service_date_sk",
                 "ServiceDate",  # ✅ Now included in output
                 "Amount", "AmountType", "PaidAmount", "ClaimID", "PayorID", "VisitType"]]

def create_fact_claims(claims_df, dim_patients, dim_date):
    fact = claims_df.copy()

    # Map patient surrogate key
    fact["unified_patient_id"] = fact["source"] + "_" + fact["PatientID"].astype(str)
    fact = fact.merge(dim_patients[["unified_patient_id", "patient_sk"]], on="unified_patient_id", how="left")

    # Map date surrogate keys
    for col in ["ServiceDate", "PaidDate"]:
        if col in fact.columns:
            fact[col] = pd.to_datetime(fact[col], errors='coerce')
            fact = fact.merge(
                dim_date[["date", "date_sk"]],
                left_on=col,
                right_on="date",
                how="left",
                suffixes=("", f"_{col}")
            )
            fact.rename(columns={f"date_sk": f"{col}_sk"}, inplace=True)
            fact.drop(columns=["date"], inplace=True)

    # Add claim surrogate key
    fact.insert(0, "claim_sk", range(1, len(fact) + 1))

    return fact


# -----------------------------
# VALIDATION
# -----------------------------

def validate_referential_integrity(fact_df, dim_patients, dim_providers, dim_procedures, dim_date):
    print("🛠️ Validating data integrity...")

    checks = [
        ("patient_sk", dim_patients, "patient_sk"),
        ("provider_id", dim_providers, "provider_id") if dim_providers is not None else None,
        ("procedure_code", dim_procedures, "procedure_code") if dim_procedures is not None else None,
        ("transaction_date", dim_date, "date") if dim_date is not None else None
    ]

    for check in checks:
        if check is None:
            continue
        fact_key, dim_df, dim_key = check

        if fact_key not in fact_df.columns:
            print(f"❌ Column {fact_key} not found in fact table.")
            continue
        if dim_key not in dim_df.columns:
            print(f"❌ Column {dim_key} not found in dimension table.")
            continue

        # Check values
        mask = ~fact_df[fact_key].astype(str).isin(dim_df[dim_key].astype(str))
        missing = fact_df[mask]
        print(f"🔎 {fact_key} ➝ {dim_key} missing: {len(missing)}")

    print("✅ Referential integrity check completed.")

    print("🛠️ Validating data integrity...")

    checks = [
        ("patient_sk", dim_patients, "patient_sk"),
        ("provider_id", dim_providers, "provider_id"),
        ("procedure_code", dim_procedures, "procedure_code"),
        ("transaction_date", dim_date, "date")
    ]

    for fact_key, dim_df, dim_key in checks:
        if fact_key not in fact_df.columns:
            print(f"❌ Column {fact_key} not found in fact table.")
            continue
        if dim_key not in dim_df.columns:
            print(f"❌ Column {dim_key} not found in dimension table.")
            continue

        # Create a boolean mask that matches the index of fact_df
        mask = ~fact_df[fact_key].astype(str).isin(dim_df[dim_key].astype(str))
        missing = fact_df[mask]

        print(f"🔎 {fact_key} ➝ {dim_key} missing: {len(missing)}")

    print("✅ Referential integrity check completed.")


def validate_business_rules(df):
    issues = {}
    if "Amount" in df.columns:
        issues["invalid_amount"] = df[df["Amount"] <= 0]
    if "ServiceDate" in df.columns:
        issues["invalid_service_date"] = df[pd.to_datetime(df["ServiceDate"], errors="coerce").isna()]
    return issues
