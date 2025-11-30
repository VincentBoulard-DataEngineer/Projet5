# Image Python légère
FROM python:3.11-slim

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installation des dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du script de migration
COPY script.py .

# Commande exécutée par défaut
CMD ["python", "script.py"]