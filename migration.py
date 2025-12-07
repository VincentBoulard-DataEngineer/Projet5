"""Migration script.

Read a CSV file.
Sanitize the dataset.
Insert data into 2 collections (patients and admission) in MongoDB.
"""

import logging
import os
import time
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from tqdm import tqdm
from bson import ObjectId

CONFIG = {
    "mongo_uri": os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
    "db_name": os.getenv("MONGO_DB_NAME", "healthcare_db"),
    "csv_path": os.getenv("CSV_PATH", "data/healthcare_dataset.csv"),
    "drop_collections": os.getenv("DROP_COLLECTIONS", "true").lower() == "true",
    "log_dir": os.getenv("LOG_DIR", "./logs")
}

os.makedirs(CONFIG["log_dir"], exist_ok=True)

# Logger configuration and initialization
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# File handler
log_path = os.path.join(CONFIG["log_dir"], "migration.log")
file_handler = logging.FileHandler(log_path, mode="a")
file_handler.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Formatters
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler.setFormatter(fmt)
console_handler.setFormatter(fmt)

# Register handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def normalize_name(name_str:str) -> str | None:
    """Title case normailsation with strip.

    Args:
        name_str (str): The string to normalize.

    Returns:
        (str | None): Normalized string or None if value is missing.
    """

    if pd.isna(name_str):
        return None
    return name_str.strip().title()

def parse_date(date_str:str) -> pd.Timestamp | None:
    """
    Convert a date string into a pandas Timestamp object.

    Args:
        date_str (str): The date value as a string

    Returns:
        (pd.Timestamp | None): A Timestamp object or None if value is missing.
    """

    if pd.isna(date_str):
        return None
    return pd.to_datetime(date_str)

def create_index(patients_col:Collection, admissions_col:Collection) -> None:
    """
    Create collection indexes.

    Args:
        patients_col (Collection): Patients collection.
        admissions_col (Collection): Admissions collection.

    Returns:
        None
    """

    patients_col.create_index([
        ("name", 1),
        ("age", 1),
        ("gender", 1),
        ("blood_type", 1)
    ])
    admissions_col.create_index([
        ("patient_id", 1),
        ("date_of_admission", -1),
        ("hospital", 1),
        ("room_number", 1)
    ])

def main():
    """
    Loop through dataframe to insert documents
    """

    start_time = time.time()

    logger.info("=== Migration process begin ===")
    logger.info("MONGO_URI: %s", CONFIG["mongo_uri"])
    logger.info("DB_NAME: %s", CONFIG["db_name"])
    logger.info("CSV_PATH : %s", CONFIG["csv_path"])
    logger.info("DROP_COLLECTIONS : %s", CONFIG["drop_collections"])

    client = MongoClient(CONFIG["mongo_uri"])
    db = client[CONFIG["db_name"]]

    patients_col = db["patients"]
    admissions_col = db["admissions"]

    if CONFIG["drop_collections"]:
        logger.info("Collections drop")
        patients_col.drop()
        admissions_col.drop()

    logger.info("Loading CSV file")
    df = pd.read_csv(CONFIG["csv_path"])

    # Column name normalization
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    logger.info("--- Preprocess statistics")
    logger.info("Total lines: %s", len(df))
    logger.info("Columns type:\n%s", df.dtypes.to_string())
    logger.info("Missing values per column:\n%s", df.isna().sum().to_string())
    logger.info("Total duplicates: %s", df.duplicated().sum())

    df = df.drop_duplicates()
    logger.info("Total lines after removing duplicates: %s", len(df))

    create_index(patients_col, admissions_col)

    # Existing documents caching
    patients_cache = {}
    admissions_cache = {}

    # Documents creation counter
    created_patients = 0
    created_admissions = 0

    def process_patient(data:tuple) -> ObjectId:
        """Handle patient creation/cache lookup."""

        nonlocal created_patients

        # Tuple identifying a patient
        name = normalize_name(data.name)
        age = data.age
        gender = data.gender
        blood_type = data.blood_type

        patient_key = (name, age, gender, blood_type)

        if patient_key in patients_cache:
            patient_id = patients_cache[patient_key]
        else:
            existing_patient = None

            patient_doc = {
                "name": name,
                "age": age,
                "gender": gender,
                "blood_type": blood_type
            }

            if not CONFIG["drop_collections"]:
                existing_patient = patients_col.find_one(patient_doc)

            if existing_patient:
                patient_id = existing_patient["_id"]
            else:
                patient_id = patients_col.insert_one(patient_doc).inserted_id
                patients_cache[patient_key] = patient_id
                created_patients += 1

        return patient_id

    def process_admission(data:tuple, patient_id:ObjectId) -> None:
        """Handle admission creation/cache lookup."""

        nonlocal created_admissions

        date_of_admission = parse_date(data.date_of_admission)
        discharge_date = parse_date(data.discharge_date)

        # Tuple identifying an admission
        admission_key = (patient_id, date_of_admission, data.hospital, data.room_number)

        if admission_key not in admissions_cache:
            existing = None

            if not CONFIG["drop_collections"]:
                admission_doc_find = {
                    "patient_id": patient_id,
                    "date_of_admission": date_of_admission,
                    "hospital": data.hospital,
                    "room_number": data.room_number
                }
                existing = admissions_col.find_one(admission_doc_find)

            if not existing:
                admission_doc_insert = {
                    "patient_id": patient_id,
                    "medical_condition": data.medical_condition,
                    "date_of_admission": date_of_admission,
                    "doctor": data.doctor,
                    "hospital": data.hospital,
                    "insurance_provider": data.insurance_provider,
                    "billing_amount": data.billing_amount,
                    "room_number": data.room_number,
                    "admission_type": data.admission_type,
                    "discharge_date": discharge_date,
                    "medication": data.medication,
                    "test_results": data.test_results
                }
                admission_id = admissions_col.insert_one(admission_doc_insert).inserted_id
                admissions_cache[admission_key] = admission_id
                created_admissions += 1

    logger.info("Database insertions begin")

    for row in tqdm(df.itertuples(index=False), total=len(df), desc="Progress"):
        process_admission(row, process_patient(row))

    logger.info("Database insertions end")

    logger.info(
        "Patient documents: %s (created during this process: %s)",
        patients_col.count_documents({}),
        created_patients
    )
    logger.info(
        "Admission documents: %s (created during this process: %s)",
        admissions_col.count_documents({}),
        created_admissions
    )
    total = time.time() - start_time
    logger.info("=== Migration process end (%.2f seconds) ===", total)

if __name__ == "__main__":
    main()
