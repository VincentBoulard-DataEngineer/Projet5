import os
import pandas as pd
from pymongo import MongoClient
from bson.objectid import ObjectId
from tqdm import tqdm


# ================================
# Configuration
# ================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "healthcare_db")
CSV_PATH = os.getenv("CSV_PATH", "healthcare_dataset.csv")
DROP_COLLECTIONS = os.getenv("DROP_COLLECTIONS", "true").lower() == "true"


# ================================
# Fonctions de nettoyage
# ================================
def normalize_name(name):
  if pd.isna(name):
    return None
  return name.strip().title()

def parse_date(date_str):
  try:
    return pd.to_datetime(date_str)
  except:
    return None


# ================================
# Connexion à Mongo
# ================================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

patients_col = db["patients"]
admissions_col = db["admissions"]

if DROP_COLLECTIONS:
  print("Suppression des collections...")
  patients_col.drop()
  admissions_col.drop()


# ================================
# Chargement du CSV
# ================================
print(f"Chargement du fichier {CSV_PATH}...")
df = pd.read_csv(CSV_PATH)

# Nettoyage du nom des colonnes
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]


# ================================
# Vérifications avant migration
# ================================
print(f"Nombre total de lignes : {len(df)}")

# Colonnes et types
print("\nColonnes et types :")
print(df.dtypes)

# Nombre de valeurs manquantes par colonne
print("\nValeurs manquantes par colonne :")
print(df.isna().sum())

# Nombre de doublons (avant suppression)
num_duplicates = df.duplicated(subset=["name", "age", "gender", "blood_type"]).sum()
print(f"\nNombre de doublons détectés : {num_duplicates}")


# -------------------------------
# Suppression des doublons
# -------------------------------
df = df.drop_duplicates()
print(f"Nombre de lignes après suppression des doublons : {len(df)}")

# ================================
# Migration du CSV vers MongoDB
# ================================
print("Début de la migration...")
patients_cache = {}

for row in tqdm(df.itertuples(index=False), total=len(df), desc="Migration"):

  # Tuple identifiant un patient
  name = normalize_name(row.name)
  age = int(row.age) if not pd.isna(row.age) else None
  gender = row.gender
  blood_type = row.blood_type
  patient_key = (name, age, gender, blood_type)

  # Récupération ou insertion patient
  if patient_key in patients_cache:
    patient_id = patients_cache[patient_key]
  else:
    patient_doc = {
        "name": name,
        "age": age,
        "gender": gender,
        "blood_type": blood_type
    }
    patient_id = patients_col.insert_one(patient_doc).inserted_id

    patients_cache[patient_key] = patient_id

  # Insertion admission
  admission_doc = {
      "patient_id": ObjectId(patient_id),
      "medical_condition": row.medical_condition,
      "hospital": row.hospital,
      "doctor": row.doctor,
      "insurance_provider": row.insurance_provider,
      "billing_amount": float(row.billing_amount) if not pd.isna(row.billing_amount) else None,
      "room_number": row.room_number,
      "admission_type": row.admission_type,
      "date_of_admission": parse_date(row.date_of_admission),
      "discharge_date": parse_date(row.discharge_date),
      "medication": row.medication,
      "test_results": row.test_results
  }

  admissions_col.insert_one(admission_doc)

print("\nMigration terminée avec succès !")

# ================================
# Vérifications après migration
# ================================
print("\nStatistiques post-migration :")
print(f"Nombre de patients : {patients_col.count_documents({})}")
print(f"Nombre d'admissions : {admissions_col.count_documents({})}")