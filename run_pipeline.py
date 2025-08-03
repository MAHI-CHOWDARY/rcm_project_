# # run_pipeline.py

# import os
# from dotenv import load_dotenv

# from src.extract import DataExtractor
# from src.scdtype2 import apply_scd_type_2
# from src.datacleaning import transform_patients, transform_transactions
# from src.dimensional import (
#     create_dim_patients,
#     create_dim_providers,
#     create_dim_procedures,
#     create_dim_date,
#     create_fact_transactions,
#     create_fact_claims,
#     validate_referential_integrity,
# )
# from src.load import load_to_bigquery

# # Load environment variables like GOOGLE_APPLICATION_CREDENTIALS
# load_dotenv()

# def main():
#     extractor = DataExtractor()

#     print("\n======================")
#     print("🔍 Phase 2: Extraction")
#     print("======================")

#     print("\n🧑‍⚕️ Extracting patients...")
#     patient_a = extractor.extract_patients("hospital_a")
#     patient_b = extractor.extract_patients("hospital_b")
#     print(f"✅ patient_a: {len(patient_a)} rows, patient_b: {len(patient_b)} rows")

#     print("\n🧼 Standardizing patient schemas...")
#     patient_a = extractor.standardize_patient_schema(patient_a, source="hospital_a")
#     patient_b = extractor.standardize_patient_schema(patient_b, source="hospital_b")

#     print("\n🔗 Unifying patients...")
#     unified_patients = extractor.unify_patients(patient_a, patient_b)

#     print("\n💰 Extracting transactions...")
#     transaction_a = extractor.extract_transactions("hospital_a")
#     transaction_b = extractor.extract_transactions("hospital_b")

#     print("\n🔗 Unifying transactions...")
#     unified_transactions = extractor.unify_transactions(transaction_a, transaction_b)

#     print("\n📄 Extracting claims...")
#     claims_df = extractor.extract_claims_csv("Data/claims")

#     print("\n============================")
#     print("🧽 Phase 3: Transformation")
#     print("============================")

#     clean_patients = transform_patients(unified_patients)
#     print(unified_transactions.columns)
#     clean_transactions = transform_transactions(unified_transactions)

#     print(len(clean_patients))
#     print(len(clean_transactions))

#     print("\n============================")
#     print("📐 Phase 4: Dimensional Modeling")
#     print("============================")

#     dim_patients = create_dim_patients(clean_patients)
#     dim_providers = create_dim_providers(clean_transactions)
#     dim_procedures = create_dim_procedures(clean_transactions)

#     print("\n📅 Creating date dimension...")
#     dim_date = create_dim_date(clean_transactions, date_columns=["VisitDate", "ServiceDate", "PaidDate"])

#     print("\n📊 Creating fact_transactions...")
#     fact_transactions = create_fact_transactions(
#         clean_transactions, dim_patients, dim_providers, dim_procedures, dim_date
#     )
#     print(fact_transactions.columns)

#     print("\n📑 Creating fact_claims...")
#     fact_claims = create_fact_claims(claims_df, dim_patients, dim_date)

#     print("\n🛠️ Validating data integrity...")
#     validate_referential_integrity(fact_transactions, dim_patients, dim_providers, dim_procedures, dim_date)
#     validate_referential_integrity(fact_claims, dim_patients, None, None, dim_date)

#     print("\n============================")
#     print("📜 Phase 5: SCD Type 2 Implementation")
#     print("============================")

#     print("\n🕵️ Detecting historical changes in patient data...")
#     scd_patients = apply_scd_type_2(existing_dim=dim_patients, new_data=clean_patients)

#     print("\n============================")
#     print("🚀 Phase 6: Loading to BigQuery")
#     print("============================")

#     load_to_bigquery(scd_patients, "dim_patients", partition_field="effective_date", cluster_fields=["unified_patient_id"])
#     load_to_bigquery(dim_providers, "dim_providers")
#     load_to_bigquery(dim_procedures, "dim_procedures")
#     load_to_bigquery(dim_date, "dim_date", partition_field="date")
#     print(fact_transactions["ServiceDate"])
#     print(fact_transactions.head(1))
#     load_to_bigquery(fact_transactions, "fact_transactions",partition_field="ServiceDate",cluster_fields=["ClaimID"])
#     load_to_bigquery(fact_claims, "fact_claims", partition_field="ServiceDate", cluster_fields=["ClaimID"])

#     print("\n✅ Pipeline completed successfully!")


# if __name__ == "__main__":
#     main()







import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from src.extract import DataExtractor
from src.scdtype2 import apply_scd_type_2
from src.datacleaning import transform_patients, transform_transactions
from src.dimensional import (
    create_dim_patients,
    create_dim_providers,
    create_dim_procedures,
    create_dim_date,
    create_fact_transactions,
    create_fact_claims,
    validate_referential_integrity,
)
from src.load import load_to_bigquery

# Load environment variables (GOOGLE_APPLICATION_CREDENTIALS, PROJECT_ID, DATASET_ID)
load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")

def read_existing_dim_patients():
    client = bigquery.Client()
    query = f"""
        SELECT * 
        FROM `{PROJECT_ID}.{DATASET_ID}.dim_patients`
        WHERE is_current = TRUE
    """
    return client.query(query).to_dataframe()

def main():
    extractor = DataExtractor()

    print("\n======================")
    print("🔍 Phase 2: Extraction")
    print("======================")

    patient_a = extractor.standardize_patient_schema(
        extractor.extract_patients("hospital_a"), source="hospital_a"
    )
    patient_b = extractor.standardize_patient_schema(
        extractor.extract_patients("hospital_b"), source="hospital_b"
    )
    unified_patients = extractor.unify_patients(patient_a, patient_b)

    transaction_a = extractor.extract_transactions("hospital_a")
    transaction_b = extractor.extract_transactions("hospital_b")
    unified_transactions = extractor.unify_transactions(transaction_a, transaction_b)

    claims_df = extractor.extract_claims_csv("Data/claims")

    print("\n============================")
    print("🧽 Phase 3: Transformation")
    print("============================")

    clean_patients = transform_patients(unified_patients)
    clean_transactions = transform_transactions(unified_transactions)

    print("\n============================")
    print("📐 Phase 4: Dimensional Modeling")
    print("============================")

    dim_providers = create_dim_providers(clean_transactions)
    dim_procedures = create_dim_procedures(clean_transactions)
    dim_date = create_dim_date(clean_transactions, date_columns=["VisitDate", "ServiceDate", "PaidDate"])

    print("\n📜 Phase 5: SCD Type 2 - Incremental")
    print("=====================================")

    try:
        existing_dim_patients = read_existing_dim_patients()
        print(f"📥 Existing dim_patients loaded: {len(existing_dim_patients)} rows")
    except Exception as e:
        print("⚠️ No existing dim_patients found or failed to load. Starting fresh.")
        existing_dim_patients = pd.DataFrame()

    updated_dim_patients = apply_scd_type_2(existing_dim=existing_dim_patients, new_data=clean_patients)

    print("\n📊 Creating fact tables...")
    fact_transactions = create_fact_transactions(
        clean_transactions, updated_dim_patients, dim_providers, dim_procedures, dim_date
    )
    fact_claims = create_fact_claims(claims_df, updated_dim_patients, dim_date)

    validate_referential_integrity(fact_transactions, updated_dim_patients, dim_providers, dim_procedures, dim_date)
    validate_referential_integrity(fact_claims, updated_dim_patients, None, None, dim_date)

    print("\n🚀 Phase 6: Loading to BigQuery")
    print("===============================")

    load_to_bigquery(updated_dim_patients, "dim_patients", partition_field="effective_date", cluster_fields=["unified_patient_id"])
    load_to_bigquery(dim_providers, "dim_providers")
    load_to_bigquery(dim_procedures, "dim_procedures")
    load_to_bigquery(dim_date, "dim_date", partition_field="date")
    load_to_bigquery(fact_transactions, "fact_transactions", partition_field="ServiceDate", cluster_fields=["ClaimID"])
    load_to_bigquery(fact_claims, "fact_claims", partition_field="ServiceDate", cluster_fields=["ClaimID"])

    print("\n✅ Pipeline completed successfully!")

if __name__ == "__main__":
    main()
