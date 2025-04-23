# src/config.py
import os
from datetime import datetime
import sys

# Gérer le chemin racine selon le contexte (normal ou PyInstaller)
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Obtenir la date du jour
actual_date = datetime.now()
year = actual_date.strftime("%Y")
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")

# Chemins des fichiers
SOURCE_FILE = os.path.join(BASE_DIR, "matrice_sources.xlsx")
SETTINGS_FILE = os.path.join(BASE_DIR, "source_settings.json")
DATABASE_FILE = os.path.join(BASE_DIR, "database.db")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "Downloads", f"{month}-{day}")
DEST_PATH = DOWNLOAD_DIR  # utilisé dans downloader.py

TEMP_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Pharma_Downloads")

def get_download_dir(date_str=None):
    """Retourne le répertoire de téléchargement pour une date donnée (format MM-DD ou date du jour)."""
    if date_str:
        month, day = date_str.split("-")
    else:
        month, day = actual_date.strftime("%m"), actual_date.strftime("%d")
    path = os.path.join(BASE_DIR, "Downloads", f"{month}-{day}")
    os.makedirs(path, exist_ok=True)
    return path