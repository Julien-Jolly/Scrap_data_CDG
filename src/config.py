# src/config.py
import os
from datetime import datetime

# Chemins des fichiers
SOURCE_FILE = os.path.join(os.path.dirname(__file__), "..", "matrice_sources.xlsx")
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "source_settings.json")
TEMP_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")


def get_download_dir(date_str=None):
    """Retourne le répertoire de téléchargement pour une date donnée (format MM-DD ou date du jour)."""
    if date_str:
        try:
            # Valider que date_str est au format MM-DD
            date_obj = datetime.strptime(date_str, "%m-%d")
            month = date_obj.strftime("%m")
            day = date_obj.strftime("%d").zfill(2)
        except ValueError:
            # Si le format est incorrect, loguer l'erreur et utiliser la date du jour
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Format de date invalide : {date_str}. Utilisation de la date du jour.")
            date_obj = datetime.now()
            month = date_obj.strftime("%m")
            day = date_obj.strftime("%d").zfill(2)
    else:
        date_obj = datetime.now()
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d").zfill(2)

    downloads_path = os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}")
    return os.path.abspath(downloads_path)


# Chemins dérivés
DOWNLOAD_DIR = get_download_dir()  # Répertoire par défaut pour la date du jour
DEST_PATH = DOWNLOAD_DIR  # Utilisé dans downloader.py