# src/downloader.py
from selenium import webdriver
from selenium.common import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from datetime import datetime
import pandas as pd
import os
import re
import logging
import time
import glob
import queue
import certifi
from urllib.parse import urlparse

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download_script.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Obtenir la date du jour
actual_date = datetime.now()
year = actual_date.strftime("%Y")
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")

# Chemins des fichiers
SOURCE_FILE = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"
DEST_PATH = os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}")
os.makedirs(DEST_PATH, exist_ok=True)

# Dossier temporaire pour les téléchargements
TEMP_DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

# Lecture du fichier source
df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
columns = df.columns.tolist()

# Fonction pour nettoyer les noms de fichiers
def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name)

# Téléchargement simple par URL
def simple_dl(row):
    final_url = row[columns[2]].format(year=year, month=month, day=day)
    logger.info(f'URL : {final_url}')
    nom_fichier = os.path.basename(final_url)

    prefix = sanitize_filename(row[columns[0]])
    fichier_destination = os.path.join(DEST_PATH, f"{prefix} - {nom_fichier}")
    logger.info(f'Destination : {fichier_destination}')

    try:
        os.makedirs(DEST_PATH, exist_ok=True)
        response = requests.get(final_url)
        response.raise_for_status()

        # Déterminer l'extension à partir de Content-Type
        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/pdf' in content_type:
            fichier_destination += '.pdf'
        elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
            fichier_destination += '.xlsx'
        elif 'text/csv' in content_type:
            fichier_destination += '.csv'
        # Ajoutez d'autres types MIME si nécessaire

        with open(fichier_destination, "wb") as fichier:
            fichier.write(response.content)
        logger.info(f"Téléchargement réussi : {fichier_destination}")
        return True, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        return False, str(e)
    except Exception as e:
        logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
        return False, str(e)

# Téléchargement via Selenium
# src/downloader.py
# src/downloader.py
# src/downloader.py
# src/downloader.py
def driver_dl(row, driver):
    url = row[columns[2]]
    xpath = row[columns[3]]

    try:
        # Charger la page initiale
        driver.get(url)
        time.sleep(2)  # Attendre le chargement initial
        logger.info(f"Page ouverte : {url}")
        logger.info(f"Utilisation du XPath : {xpath}")

        # Attendre que l'élément soit cliquable
        element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        if not element:
            logger.error(f"Élément avec le XPath {xpath} non trouvé.")
            return False, "Élément non trouvé pour le clic"

        # Récupérer l'URL cible de l'élément avant de cliquer (si disponible)
        href = element.get_attribute("href")
        logger.info(f"Attribut href de l'élément : {href}")

        # Télécharger le fichier via clic
        before_files = set(glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")))
        element.click()

        # Vérifier si un fichier est téléchargé dans TEMP_DOWNLOAD_DIR
        timeout = 30
        start_time = time.time()
        downloaded_file = None
        valid_extensions = {".xlsx", ".xls", ".csv", ".pdf", ".docx"}

        while time.time() - start_time < timeout:
            after_files = set(glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")))
            new_files = after_files - before_files
            if new_files:
                for file in new_files:
                    _, ext = os.path.splitext(file)
                    if ext.lower() in valid_extensions and "crdownload" not in file:
                        downloaded_file = file
                        break
            if downloaded_file:
                break
            time.sleep(1)

        if downloaded_file:
            # Cas classique : fichier téléchargé dans TEMP_DOWNLOAD_DIR
            downloaded_filename = os.path.basename(downloaded_file)
            logger.info(f"Fichier téléchargé : {downloaded_filename}")
            prefix = sanitize_filename(row[columns[0]])
            final_filename = f"{prefix} - {downloaded_filename}"
            destination_path = os.path.join(DEST_PATH, final_filename)
            os.makedirs(DEST_PATH, exist_ok=True)
            os.rename(downloaded_file, destination_path)
            logger.info(f"Fichier téléchargé et sauvegardé : {destination_path}")
            return True, None
        else:
            # Si aucun fichier n'est téléchargé, utiliser l'URL cible avec requests
            logger.info("Aucun fichier détecté dans TEMP_DOWNLOAD_DIR, tentative via URL cible...")
            time.sleep(2)  # Attendre une éventuelle redirection
            current_url = driver.current_url
            logger.info(f"URL après clic : {current_url}")

            # Choisir l'URL à utiliser : href si disponible, sinon current_url
            target_url = href if href else current_url

            if not target_url:
                logger.error(f"Aucune URL cible valide trouvée pour {url}")
                return False, "Aucune URL cible valide trouvée"

            # Télécharger avec requests
            response = requests.get(target_url)
            response.raise_for_status()

            # Déterminer le nom et l'extension du fichier
            content_type = response.headers.get('Content-Type', '').lower()
            downloaded_filename = os.path.basename(urlparse(target_url).path)
            if not downloaded_filename:  # Si aucun nom dans l'URL
                downloaded_filename = "document"

            # Ajouter l'extension correcte basée sur Content-Type
            if 'application/pdf' in content_type or downloaded_filename.lower().endswith('.pdf'):
                if not downloaded_filename.lower().endswith('.pdf'):
                    downloaded_filename += '.pdf'
            elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or downloaded_filename.lower().endswith('.xlsx'):
                if not downloaded_filename.lower().endswith('.xlsx'):
                    downloaded_filename += '.xlsx'
            elif 'text/csv' in content_type or downloaded_filename.lower().endswith('.csv'):
                if not downloaded_filename.lower().endswith('.csv'):
                    downloaded_filename += '.csv'
            elif 'application/vnd.ms-excel' in content_type or downloaded_filename.lower().endswith('.xls'):
                if not downloaded_filename.lower().endswith('.xls'):
                    downloaded_filename += '.xls'
            else:
                logger.warning(f"Type de contenu inconnu : {content_type}, extension par défaut .pdf")
                downloaded_filename += '.pdf'

            prefix = sanitize_filename(row[columns[0]])
            final_filename = f"{prefix} - {downloaded_filename}"
            destination_path = os.path.join(DEST_PATH, final_filename)

            # Sauvegarder le fichier
            os.makedirs(DEST_PATH, exist_ok=True)
            with open(destination_path, "wb") as fichier:
                fichier.write(response.content)
            logger.info(f"Fichier téléchargé et sauvegardé : {destination_path}")
            return True, None

    except TimeoutException:
        logger.error(f"Timeout : L'élément avec le XPath {xpath} n'a pas été trouvé dans les 10 secondes.")
        return False, "Timeout : Élément non trouvé"
    except WebDriverException as e:
        logger.error(f"Erreur WebDriver : {str(e)}")
        return False, f"Erreur WebDriver : {str(e)}"
    except Exception as e:
        logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
        return False, f"Erreur inattendue : {str(e)}"

# Fonction pour obtenir la liste des sources
def get_sources():
    sources = df[columns[0]].tolist()
    logger.info(f"Sources trouvées : {sources}")
    return sources

# Fonction principale de téléchargement
def download_files(status_queue):
    """
    Exécute le téléchargement des fichiers pour toutes les sources et envoie les mises à jour de statut via une file d'attente.

    Args:
        status_queue (queue.Queue): File d'attente pour envoyer les mises à jour de statut.

    Returns:
        tuple: (nombre de succès, total, liste des erreurs).
    """
    errors = []
    successes = 0
    total = len(df)

    # Configurer Selenium avec Chrome
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    service = Service("C:/chromedriver/chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=options)

    try:
        for index, row in df.iterrows():
            source = row[columns[0]]
            # Signaler que le téléchargement commence ou que la source est ignorée
            if row[columns[1]] != "1" and row[columns[1]] != "2":
                status_queue.put((source, "🚫 Ignoré"))
            else:
                status_queue.put((source, "⏳ En cours"))

                if row[columns[1]] == "1":
                    logger.info(f"Démarrage extraction {source} - {row[columns[2]]}")
                    success, error = simple_dl(row)
                    if success:
                        successes += 1
                        status_queue.put((source, "✅ Succès"))
                    else:
                        errors.append((source, error))
                        status_queue.put((source, "❌ Échec"))
                    logger.info(f"Fin extraction {source} - {row[columns[2]]}")

                elif row[columns[1]] == "2":
                    logger.info(f"Démarrage extraction {source} - {row[columns[2]]}")
                    success, error = driver_dl(row, driver)
                    if success:
                        successes += 1
                        status_queue.put((source, "✅ Succès"))
                    else:
                        errors.append((source, error))
                        status_queue.put((source, "❌ Échec"))
                    logger.info(f"Fin extraction {source} - {row[columns[2]]}")
    finally:
        driver.quit()

    # Signaler la fin du téléchargement
    status_queue.put(("DONE", None))
    return successes, total, errors