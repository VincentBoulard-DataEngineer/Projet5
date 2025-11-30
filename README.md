# Projet 5 - Maintenez et documentez un système de stockage des données sécurisé et performant

Ce script permet de migrer un dataset CSV de patients et leurs admissions vers une base MongoDB, avec nettoyage, suppression de doublons et statistiques avant/après migration.

---

## Fonctionnalités

1. **Chargement du CSV**  
   - Nettoie les noms de colonnes (espaces remplacés par `_` et mots en minuscules).  
   - Supprime les doublons.

2. **Nettoyage et transformation des données**  
   - **Noms** : première lettre de chaque nom en majuscule (`title case`), suppression des espaces superflus.  
   - **Dates** : conversion des dates (`date_of_admission` et `discharge_date`) en objets `datetime`.

3. **Statistiques pré-migration**  
   - Nombre total de lignes, types de colonnes, valeurs manquantes.  
   - Nombre de doublons

4. **Insertion dans MongoDB**  
   - Création des collections `patients` et `admissions`.  
   - Vérification de l’existence d’un patient pour éviter les doublons.  
   - Insertion des admissions liées à chaque patient avec vérification d'existence.  
   - Barre de progression avec `tqdm`.

5. **Statistiques post-migration**  
   - Nombre de patients et admissions insérés.  

6. **Tests Pytest** (facultatif)  
   - Vérification des collections existantes.  
   - Vérification que les collections ne sont pas vides.  
   - Vérification de l’absence de doublons patients.

---

## Schéma MongoDB

![Schéma MongoDB](/assets/schema.png "Schéma MongoDB")

---

## Configuration

Le script utilise les variables d’environnement suivantes :

| Variable           | Description                               | Valeur par défaut             |
|--------------------|-------------------------------------------|-------------------------------|
| `MONGO_URI`        | URI de connexion MongoDB                  | `mongodb://localhost:27017/`  |
| `MONGO_DB_NAME`    | Nom de la base MongoDB                    | `healthcare_db`               |
| `CSV_PATH`         | Chemin du fichier CSV                     | `data/healthcare_dataset.csv` |
| `DROP_COLLECTIONS` | Supprimer les collections avant migration | `true`                        |

---

## Installation et utilisation (Docker Compose)

1. Construire les images :

        docker-compose build

2. Lancer la migration

        docker-compose up

3. Suivre la migration  
   - La barre de progression indique l’avancement.  
   - Les statistiques avant et après migration sont affichées automatiquement.

---

## Tests avec Pytest

Les tests peuvent également être exécutés dans Docker (fichier `test_migration.py`).

- Vérification des collections existantes.  
- Vérification que les collections ne sont pas vides.  
- Vérification de l’absence de doublons patients.

Pour exécuter les tests :

    docker-compose run --rm migration pytest -vv

---

## Notes

- Si `DROP_COLLECTIONS` est `True`, les collections sont supprimées avant migration.  
- Si `DROP_COLLECTIONS` est `False`, le script vérifie l’existence des patients et des admissions avant insertion.  
- La barre de progression est utile pour les fichiers CSV volumineux.  
- Le script assure un format uniforme pour les noms et les dates afin de garantir la cohérence des données.
