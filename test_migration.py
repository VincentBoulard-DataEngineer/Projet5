import pytest
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "data/healthcare_dataset.csv")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

patients_col = db["patients"]
admissions_col = db["admissions"]


def test_collections_exist():
    """Vérifie que les collections existent"""
    assert "patients" in db.list_collection_names()
    assert "admissions" in db.list_collection_names()


def test_collections_not_empty():
    """Vérifie que les collections ne sont pas vides"""
    assert patients_col.count_documents({}) > 0
    assert admissions_col.count_documents({}) > 0


def test_no_duplicate_patients():
    """Vérifie qu'il n'y a pas de doublons patients"""
    pipeline = [
        {"$group": {"_id": {"name": "$name", "age": "$age", "gender": "$gender", "blood_type": "$blood_type"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(patients_col.aggregate(pipeline))
    assert len(duplicates) == 0