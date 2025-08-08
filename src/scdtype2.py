import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import pytz

def read_existing_dim_patients(table_id):
    client = bigquery.Client()
    try:
        query = f"SELECT * FROM `{table_id}`"
        df = client.query(query).to_dataframe()
        print(f"✅ Loaded existing dim_patients: {len(df)} rows")
        return df
    except Exception as e:
        print(f"⚠️ Could not read from `{table_id}`: {e}")
        return pd.DataFrame()  # return empty if table doesn't exist

def normalize_columns(df, cols):
    """Strip and lowercase for comparison consistency."""
    for col in cols:
        df[col] = df[col].astype(str).str.strip().str.lower()
    return df

def apply_scd_type_2(existing_dim, new_data):
    scd_columns = ["Address", "PhoneNumber", "FirstName", "LastName", "MiddleName", "SSN", "Gender", "DOB"]
    key = "unified_patient_id"
    utc = pytz.UTC
    today = pd.Timestamp(datetime.today(), tz=utc).normalize()
    far_future = pd.Timestamp("2099-12-31", tz=utc)

    existing_dim = existing_dim.copy()
    new_data = new_data.copy()

    # If existing_dim is empty, initialize required columns
    if existing_dim.empty:
        existing_dim = pd.DataFrame(columns=new_data.columns.tolist() + [
            "effective_date", "expiry_date", "is_current", "version", "patient_sk"
        ])

    # Ensure required columns exist
    for col in ["effective_date", "expiry_date", "is_current", "version", "patient_sk"]:
        if col not in existing_dim.columns:
            if col == "effective_date":
                existing_dim[col] = pd.NaT
            elif col == "expiry_date":
                existing_dim[col] = pd.NaT
            elif col == "is_current":
                existing_dim[col] = False
            elif col == "version":
                existing_dim[col] = 1
            elif col == "patient_sk":
                existing_dim[col] = 0

    # Normalize string columns
    existing_dim = normalize_columns(existing_dim, [key] + scd_columns)
    new_data = normalize_columns(new_data, [key] + scd_columns)

    # Fix types
    existing_dim["is_current"] = existing_dim["is_current"].astype(bool)
    existing_dim["version"] = pd.to_numeric(existing_dim["version"], errors="coerce").fillna(1).astype(int)
    existing_dim["patient_sk"] = pd.to_numeric(existing_dim["patient_sk"], errors="coerce").fillna(0).astype(int)

    # Convert dates to datetime (UTC-safe)
    existing_dim["DOB"] = pd.to_datetime(existing_dim["DOB"], errors="coerce")
    new_data["DOB"] = pd.to_datetime(new_data["DOB"], errors="coerce")
    existing_dim["effective_date"] = pd.to_datetime(existing_dim["effective_date"], utc=True, errors="coerce")
    existing_dim["expiry_date"] = pd.to_datetime(existing_dim["expiry_date"], utc=True, errors="coerce")

    scd_records = []
    next_sk = existing_dim["patient_sk"].max() + 1 if not existing_dim.empty else 1

    for _, new_row in new_data.iterrows():
        # Match by key and is_current
        match = existing_dim[
            (existing_dim[key] == new_row[key]) & (existing_dim["is_current"] == True)
        ]

        if match.empty:
            # New record
            new_row["effective_date"] = today
            new_row["expiry_date"] = far_future
            new_row["is_current"] = True
            new_row["version"] = 1
            new_row["patient_sk"] = next_sk
            next_sk += 1
            scd_records.append(new_row)
        else:
            latest = match.iloc[0]
            if not all(new_row[col] == latest[col] for col in scd_columns):
                # Update current record
                existing_dim.loc[match.index, "expiry_date"] = today
                existing_dim.loc[match.index, "is_current"] = False

                new_row["effective_date"] = today
                new_row["expiry_date"] = far_future
                new_row["is_current"] = True
                new_row["version"] = latest["version"] + 1
                new_row["patient_sk"] = next_sk
                next_sk += 1
                scd_records.append(new_row)

    # Append new SCD records
    if scd_records:
        scd_df = pd.DataFrame(scd_records)
        updated_dim = pd.concat([existing_dim, scd_df], ignore_index=True)
    else:
        updated_dim = existing_dim

    # Final timezone conversion
    updated_dim["effective_date"] = pd.to_datetime(updated_dim["effective_date"], utc=True)
    updated_dim["expiry_date"] = pd.to_datetime(updated_dim["expiry_date"], utc=True)

    return updated_dim
