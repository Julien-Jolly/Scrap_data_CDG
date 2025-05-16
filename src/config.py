# src/config.py
import os
import logging
from datetime import datetime

# Chemins des fichiers
SOURCE_FILE = os.path.join(os.path.dirname(__file__), "..", "matrice_sources.xlsx")  # Confirmé comme matrice_sources.xlsx
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "source_settings.json")
TEMP_DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "temp_downloads")

def configure_logging(log_name):
    """Configure le logging avec un fichier et un résumé."""
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"{log_name}_{log_timestamp}.log")
    summary_log_file = os.path.join(log_dir, f"summary_{log_name}_{log_timestamp}.log")

    # Configurer le logger principal
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_file)]
    )

    # Configurer le logger de résumé
    summary_logger = logging.getLogger('summary')
    summary_handler = logging.FileHandler(summary_log_file)
    summary_handler.setFormatter(logging.Formatter('%(message)s'))
    summary_logger.addHandler(summary_handler)
    summary_logger.setLevel(logging.INFO)
    summary_logger.propagate = False

    return logging.getLogger(__name__), summary_logger

def get_download_dir(date_str=None):
    """Retourne le répertoire de téléchargement pour une date donnée (format MM-DD ou date du jour)."""
    logger, _ = configure_logging("config")
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, "%m-%d")
            month = date_obj.strftime("%m")
            day = date_obj.strftime("%d").zfill(2)
        except ValueError:
            logger.error(f"Format de date invalide : {date_str}. Utilisation de la date du jour.")
            date_obj = datetime.now()
            month = date_obj.strftime("%m")
            day = date_obj.strftime("%d").zfill(2)
    else:
        date_obj = datetime.now()
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d").zfill(2)

    downloads_path = os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}", "dl_api")
    return os.path.abspath(downloads_path)