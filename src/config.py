# src/config.py
import os
from datetime import datetime

# Obtenir la date du jour
actual_date = datetime.now()
year = actual_date.strftime("%Y")
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")

# Chemins des fichiers
SOURCE_FILE = "C:/Users/Julien/PycharmProjects/PythonProject/Scrap_data_CDG/matrice sources.xlsx"
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}")
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "source_settings.json")
DEST_PATH = DOWNLOAD_DIR  # Utilisé dans downloader.py
TEMP_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

def get_download_dir(date_str=None):
    """Retourne le répertoire de téléchargement pour une date donnée (format MM-DD ou date du jour)."""
    if date_str:
        month, day = date_str.split("-")
    else:
        month, day = actual_date.strftime("%m"), actual_date.strftime("%d")
    return os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}")