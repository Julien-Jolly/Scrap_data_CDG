# src/downloader.py
from selenium import webdriver
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
        response = requests.get(final_url)
        response.raise_for_status()
        with open(fichier_destination, "wb") as fichier:
            fichier.write(response.content)
        return True, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        return False, str(e)

# Téléchargement via Selenium
def driver_dl(row, driver):
    """
    Télécharge un fichier en utilisant Selenium pour interagir avec une page web.

    Args:
        row: Ligne du DataFrame contenant les informations de téléchargement.
        driver: Instance du driver Selenium.

    Returns:
        tuple: (succès (bool), erreur (str ou None)).
    """
    url = row[columns[2]]
    xpath = row[columns[3]]

    try:
        driver.get(url)
        logger.info(f"Page ouverte : {url}")
        logger.info(f"Utilisation du XPath : {xpath}")
        # Note : De nombreux XPath dans le fichier Excel utilisent des ID dynamiques (ex: address-...).
        # Pour éviter les échecs, il est recommandé de les remplacer par des XPath plus robustes, par exemple :
        # - //a[@class="link-CSV"] (pour les éléments avec une classe link-CSV)
        # - //a[contains(@title, "CSV")] (pour les éléments avec un title indiquant un CSV)
        # - //a[contains(@href, "download")] (pour les liens de téléchargement génériques)
        # Voir la liste des XPath reformulés dans la documentation.

        # Attendre que l'élément soit cliquable et le cliquer
        element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        if not element:
            raise Exception("Élément non trouvé pour le clic")

        # Récupérer les métadonnées de l'élément pour déterminer le type de fichier
        href = element.get_attribute("href")
        title = element.get_attribute("title")
        class_name = element.get_attribute("class")
        logger.info(f"Attribut href : {href}")
        logger.info(f"Attribut title : {title}")
        logger.info(f"Attribut class : {class_name}")

        # Vérifier si l'élément indique un fichier CSV
        is_csv = (
            (title and "CSV" in title.upper()) or
            (class_name and "CSV" in class_name.upper()) or
            (href and "csv" in href.lower())
        )

        # Extraire un nom de fichier potentiel à partir de l'URL (href)
        expected_filename = None
        if href:
            parsed_url = urlparse(href)
            expected_filename = os.path.basename(parsed_url.path)
            logger.info(f"Nom de fichier attendu à partir de href : {expected_filename}")
        else:
            logger.warning("Aucun attribut href trouvé, utilisation d'un nom de fichier par défaut")

        before_files = set(glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")))
        element.click()

        timeout = 30
        start_time = time.time()
        downloaded_file = None
        while time.time() - start_time < timeout:
            after_files = set(glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")))
            new_files = after_files - before_files
            if new_files and not any("crdownload" in f for f in new_files):
                downloaded_file = new_files.pop()
                break
            time.sleep(1)

        if not downloaded_file:
            raise Exception("Le téléchargement n'a pas démarré ou a échoué")

        # Déterminer l'extension du fichier téléchargé
        downloaded_filename = os.path.basename(downloaded_file)
        _, downloaded_ext = os.path.splitext(downloaded_filename)
        logger.info(f"Fichier téléchargé : {downloaded_filename} (extension : {downloaded_ext})")

        # Déterminer l'extension finale
        if expected_filename:
            _, expected_ext = os.path.splitext(expected_filename)
            if expected_ext:  # Si l'URL contient une extension claire (ex: .xlsx, .pdf)
                final_ext = expected_ext.lower()
                logger.info(f"Extension extraite de l'URL : {final_ext}")
            elif is_csv:
                logger.info("Le fichier est identifié comme un CSV, forçage de l'extension .csv")
                final_ext = ".csv"
            else:
                logger.warning("Aucune extension claire dans l'URL, forçage à .csv")
                final_ext = ".csv"
        else:
            # Si pas de href ou pas d'extension claire, vérifier si c'est un CSV
            if is_csv:
                logger.info("Le fichier est identifié comme un CSV, forçage de l'extension .csv")
                final_ext = ".csv"
            else:
                logger.warning("Impossible de déterminer le type de fichier, forçage à .csv")
                final_ext = ".csv"

        # Construire le nom de fichier final
        prefix = sanitize_filename(row[columns[0]])
        if expected_filename and expected_ext:
            base_filename = os.path.splitext(expected_filename)[0]
        else:
            base_filename = os.path.splitext(downloaded_filename)[0]

        final_filename = f"{prefix} - {base_filename}{final_ext}"
        destination_path = os.path.join(DEST_PATH, final_filename)
        os.rename(downloaded_file, destination_path)
        logger.info(f"Fichier téléchargé et sauvegardé : {destination_path}")

        return True, None
    except Exception as e:
        logger.error(f"Erreur : {type(e).__name__} - {str(e)}")
        return False, str(e)

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