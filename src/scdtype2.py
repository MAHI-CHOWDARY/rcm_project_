# src/scdtype2.py

import pandas as pd
from datetime import datetime, date

def apply_scd_type_2(existing_dim, new_data):
    
    scd_columns = ["Address", "PhoneNumber", "FirstName", "LastName", "MiddleName", "SSN", "Gender", "DOB"]
    key = "unified_patient_id"

    today = pd.to_datetime(datetime.today().date())  # type: pd.Timestamp
    far_future = pd.to_datetime("2099-12-31")
  # expiry for active records

    existing_dim = existing_dim.copy()
    new_data = new_data.copy()

    # Ensure SCD columns exist and clean up
    for col in ["effective_date", "expiry_date", "is_current", "version"]:
        if col not in existing_dim.columns:
            if col == "effective_date":
                existing_dim[col] = today
            elif col == "expiry_date":
                existing_dim[col] = far_future
            elif col == "is_current":
                existing_dim[col] = True
            elif col == "version":
                existing_dim[col] = 1

    scd_records = []
    for _, new_row in new_data.iterrows():
        match = existing_dim[
            (existing_dim[key] == new_row[key]) & (existing_dim["is_current"] == True)
        ]
        if match.empty:
            new_row["effective_date"] = today
            new_row["expiry_date"] = far_future
            new_row["is_current"] = True
            new_row["version"] = 1
            scd_records.append(new_row)
        else:
            latest = match.iloc[0]
            if not all(new_row[col] == latest[col] for col in scd_columns):
                # Expire the current record
                existing_dim.loc[match.index, "expiry_date"] = today
                existing_dim.loc[match.index, "is_current"] = False

                # Add new version
                new_row["effective_date"] = today
                new_row["expiry_date"] = far_future
                new_row["is_current"] = True
                new_row["version"] = latest["version"] + 1
                scd_records.append(new_row)

    if scd_records:
        scd_df = pd.DataFrame(scd_records)
        updated_dim = pd.concat([existing_dim, scd_df], ignore_index=True)
    else:
        updated_dim = existing_dim

    # Ensure datetime columns are of type datetime64[ns] for BigQuery
    updated_dim["effective_date"] = pd.to_datetime(updated_dim["effective_date"])
    updated_dim["expiry_date"] = pd.to_datetime(updated_dim["expiry_date"])

    return updated_dim
