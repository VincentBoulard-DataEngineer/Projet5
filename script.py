import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm


# ================================
# Configuration
# ================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "healthcare_db")
CSV_PATH = os.getenv("CSV_PATH", "data/healthcare_dataset.csv")
DROP_COLLECTIONS = os.getenv("DROP_COLLECTIONS", "true").lower() == "true"


# ================================
# Fonctions de nettoyage
# ================================
# Enlève les espaces inutile et met la 1ère lettre en majuscule
def normalize_name(name):
  if pd.isna(name):
    return None
  return name.strip().title()

# Convertit les chaines date en objet datetime
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
print(f"\nChargement du fichier {CSV_PATH}...")
df = pd.read_csv(CSV_PATH)

# Nettoyage du nom des colonnes
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]


# ================================
# Vérifications avant migration
# ================================
print("\nStatistiques avant migration :")
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


# ================================
# Suppression des doublons
# ================================
df = df.drop_duplicates()
print(f"Nombre de lignes après suppression des doublons : {len(df)}")


# ================================
# Création d'index
# ================================
# Pour la recherche de patient exsistant
patients_col.create_index([("name", 1), ("age", 1), ("gender", 1), ("blood_type", 1)])
# Pour faciliter la récupération de toutes les admissions d'un patient
admissions_col.create_index([("patient_id", 1)])
# Pour la recherche d'admission existante
admissions_col.create_index([("patient_id", 1), ("date_of_admission", -1), ("hospital", 1),("room_number", 1)])

# ================================
# Migration du CSV vers MongoDB
# ================================
print("\nDébut de la migration...")

# Cache des documents existants
patients_cache = {}
admissions_cache = {}

for row in tqdm(df.itertuples(index=False), total=len(df), desc="Progression"):

  # Tuple identifiant un patient
  name = normalize_name(row.name)
  age = int(row.age) if not pd.isna(row.age) else None
  gender = row.gender
  blood_type = row.blood_type

  patient_key = (name, age, gender, blood_type)

  # Récupération du patient_id en cache
  if patient_key in patients_cache:
    patient_id = patients_cache[patient_key]
  else:
    existing_patient = None

    # Si la collection n'a pas été vidée, on vérifie si le patient existe déjà
    if not DROP_COLLECTIONS:
      existing_patient = patients_col.find_one({
        "name": name,
        "age": age,
        "gender": gender,
        "blood_type": blood_type
      })

    # On assigne le patient_id si retrouvé en base, sinon on le crée
    if existing_patient:
      patient_id = existing_patient["_id"]
    else:
      patient_doc = {
        "name": name,
        "age": age,
        "gender": gender,
        "blood_type": blood_type
      }
      patient_id = patients_col.insert_one(patient_doc).inserted_id

    # On ajoute le patien_id dans le cache
    patients_cache[patient_key] = patient_id

  # Tuple identifiant une admission
  admission_key = (patient_id, row.date_of_admission, row.hospital, row.room_number)
  
  # On traite l'admission si elle n'est pas dans le cache
  if admission_key not in admissions_cache:
    existing = None

    # Si la collection n'a pas été vidée, on vérifie si l'admission existe déjà
    if not DROP_COLLECTIONS:
      existing = admissions_col.find_one({
        "patient_id": patient_id,
        "date_of_admission": row.date_of_admission,
        "hospital": row.hospital,
        "medical_condition": row.medical_condition
      })

    # On insère si rien n'a été trouvé
    if not existing:
      admission_doc = {
        "patient_id": patient_id,
        "medical_condition": row.medical_condition,
        "hospital": row.hospital,
        "doctor": row.doctor,
        "insurance_provider": row.insurance_provider,
        "billing_amount": row.billing_amount,
        "room_number": row.room_number,
        "admission_type": row.admission_type,
        "date_of_admission": row.date_of_admission,
        "discharge_date": row.discharge_date,
        "medication": row.medication,
        "test_results": row.test_results
      }
      
      admissions_col.insert_one(admission_doc)

print("\nMigration terminée avec succès !")

# ================================
# Vérifications après migration
# ================================
print("\nStatistiques après migration :")
print(f"Nombre de patients : {patients_col.count_documents({})}")
print(f"Nombre d'admissions : {admissions_col.count_documents({})}")
