# src/downloader.py
from selenium import webdriver
from selenium.common import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
import os
import re
import logging
import time
import glob
import queue
import certifi
from urllib.parse import urlparse
import json
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from src.config import SOURCE_FILE, DEST_PATH, TEMP_DOWNLOAD_DIR, year, month, day

# Configurer la journalisation sans StreamHandler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("temp_download.log")  # Fichier temporaire pour éviter les conflits
    ]
)
logger = logging.getLogger(__name__)

os.makedirs(DEST_PATH, exist_ok=True)

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name)

def simple_dl(row, columns):
    final_url = row[columns[2]].format(year=year, month=month, day=day)
    logger.info(f'URL : {final_url}')
    nom_fichier = os.path.basename(final_url)

    prefix = sanitize_filename(row[columns[0]])
    fichier_destination = os.path.join(DEST_PATH, f"{prefix} - {nom_fichier}")
    logger.info(f'Destination : {fichier_destination}')

    try:
        os.makedirs(DEST_PATH, exist_ok=True)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        response = requests.get(final_url, headers=headers)
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        if 'application/pdf' in content_type:
            fichier_destination += '.pdf'
        elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type:
            fichier_destination += '.xlsx'
        elif 'text/csv' in content_type:
            fichier_destination += '.csv'

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

def driver_dl(row, columns, driver):
    url = row[columns[2]]
    xpath = row[columns[3]]

    if not url or not isinstance(url, str) or not url.strip():
        logger.error(f"URL invalide pour la source {row[columns[0]]}: {url}")
        return False, "URL invalide"

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        logger.info(f"URL ajustée : {url}")

    try:
        logger.info(f"Tentative d'accès à l'URL : {url}")
        driver.get(url)
        logger.info(f"Page ouverte avec succès : {url}")

        content_type = driver.execute_script("return document.contentType;")
        if 'application/json' in content_type.lower() or url.endswith('json'):
            content = driver.find_element(By.TAG_NAME, "body").text
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                try:
                    pre_element = driver.find_element(By.TAG_NAME, "pre")
                    content = pre_element.text
                    data = json.loads(content)
                except:
                    logger.error(f"Le contenu de {url} n’est pas du JSON valide.")
                    return False, "Le contenu n’est pas du JSON valide"

            prefix = sanitize_filename(row[columns[0]])
            destination_path = os.path.join(DEST_PATH, f"{prefix} - data.json")
            os.makedirs(DEST_PATH, exist_ok=True)
            with open(destination_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logger.info(f"Fichier JSON sauvegardé : {destination_path}")
            return True, None

        if not xpath or xpath.strip() == "/":
            logger.error(f"XPath non fourni ou invalide pour {url}. Un XPath est requis pour les sources non-JSON.")
            return False, "XPath non fourni ou invalide"

        logger.info(f"Utilisation du XPath : {xpath}")

        element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        if not element:
            logger.error(f"Élément avec le XPath {xpath} non trouvé.")
            return False, "Élément non trouvé pour le clic"

        href = element.get_attribute("href")
        logger.info(f"Attribut href de l’élément : {href}")

        before_files = set(glob.glob(os.path.join(TEMP_DOWNLOAD_DIR, "*")))
        element.click()

        timeout = 5
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
            logger.info("Aucun fichier détecté dans TEMP_DOWNLOAD_DIR, tentative via URL cible...")
            time.sleep(2)
            current_url = driver.current_url
            logger.info(f"URL après clic : {current_url}")

            target_url = href if href else current_url

            if not target_url:
                logger.error(f"Aucune URL cible valide trouvée pour {url}")
                return False, "Aucune URL cible valide trouvée"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
            response = requests.get(target_url, headers=headers)
            response.raise_for_status()

            content_type = response.headers.get('Content-Type', '').lower()
            downloaded_filename = os.path.basename(urlparse(target_url).path)
            if not downloaded_filename:
                downloaded_filename = "document"

            if 'application/pdf' in content_type or downloaded_filename.lower().endswith('.pdf'):
                if not downloaded_filename.lower().endswith('.pdf'):
                    downloaded_filename += '.pdf'
            elif 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type or downloaded_filename.lower().endswith(
                    '.xlsx'):
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

            os.makedirs(DEST_PATH, exist_ok=True)
            with open(destination_path, "wb") as fichier:
                fichier.write(response.content)
            logger.info(f"Fichier téléchargé et sauvegardé : {destination_path}")
            return True, None

    except TimeoutException:
        logger.error(f"Timeout : L’élément avec le XPath {xpath} n’a pas été trouvé dans les 10 secondes.")
        return False, "Timeout : Élément non trouvé"
    except WebDriverException as e:
        logger.error(f"Erreur WebDriver : {str(e)}")
        return False, f"Erreur WebDriver : {str(e)}"
    except Exception as e:
        logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
        return False, str(e)

def scrape_html_table_dl(row, columns, driver):
    """Téléchargement de tous les tableaux HTML pour le type 3 avec Selenium."""
    url = row[columns[2]]

    if not url or not isinstance(url, str) or str(url).strip().lower() in ('nan', ''):
        logger.error(f"URL invalide pour la source {row[columns[0]]}: {url}")
        return False, "URL invalide"

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        logger.info(f"URL ajustée : {url}")

    try:
        logger.info(f"Tentative de scraping de l'URL avec Selenium : {url}")
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
        )
        time.sleep(3)
        html_content = driver.page_source

        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            logger.error(f"Aucun tableau trouvé pour {url}")
            return False, "Aucun tableau trouvé"

        logger.info(f"Nombre de tableaux trouvés : {len(tables)}")
        prefix = sanitize_filename(row[columns[0]])
        os.makedirs(DEST_PATH, exist_ok=True)
        success = False

        for idx, table in enumerate(tables):
            table_html = str(table)
            destination_path = os.path.join(DEST_PATH, f"{prefix} - table_{idx}.html")
            with open(destination_path, "w", encoding="utf-8") as f:
                f.write(table_html)
            logger.info(f"Tableau {idx} sauvegardé : {destination_path}")
            success = True

        if success:
            return True, None
        else:
            return False, "Aucun tableau sauvegardé"
    except TimeoutException:
        logger.error(f"Timeout : Aucun tableau trouvé dans les 20 secondes pour {url}")
        return False, "Timeout : Tableau non trouvé"
    except Exception as e:
        logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
        return False, str(e)

def get_sources():
    try:
        df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str, engine="openpyxl").fillna('')
        sources = df[df.columns[0]].tolist()
        logger.info(f"Sources trouvées : {sources}")
        return sources
    except Exception as e:
        logger.error(f"Erreur lors de la lecture des sources : {str(e)}")
        return []

def download_files(sources, status_queue):
    """Exécute le téléchargement des fichiers pour les sources données."""
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str).fillna('')
    columns = df.columns.tolist()
    df_to_download = df[df[columns[0]].isin(sources)]

    errors = []
    successes = 0
    total = len(df_to_download)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")  # Réduire les logs de ChromeDriver
    options.add_argument("--silent")  # Supprimer les messages DevTools
    service = Service(ChromeDriverManager().install())
    service.log_path = os.devnull  # Rediriger les logs de ChromeDriver
    driver = webdriver.Chrome(service=service, options=options)

    try:
        for index, row in df_to_download.iterrows():
            source = row[columns[0]]
            extraction_type = row[columns[1]]
            try:
                if extraction_type not in ["1", "2", "3"]:
                    status_queue.put((source, "🚫 Ignoré"))
                    logger.warning(f"Type d'extraction invalide pour {source} : {extraction_type}")
                    errors.append((source, f"Type d'extraction invalide : {extraction_type}"))
                    continue
                else:
                    status_queue.put((source, "⏳ En cours"))
                    logger.info(f"Démarrage extraction {source} - {row[columns[2]]}")
                    if extraction_type == "1":
                        success, error = simple_dl(row, columns)
                    elif extraction_type == "2":
                        success, error = driver_dl(row, columns, driver)
                    elif extraction_type == "3":
                        success, error = scrape_html_table_dl(row, columns, driver)

                    if success:
                        successes += 1
                        status_queue.put((source, "✅ Succès"))
                    else:
                        errors.append((source, error))
                        status_queue.put((source, "❌ Échec"))
                    logger.info(f"Fin extraction {source} - {row[columns[2]]}")
            except Exception as e:
                logger.error(f"Erreur lors du traitement de {source} : {str(e)}")
                errors.append((source, str(e)))
                status_queue.put((source, "❌ Échec"))
    except Exception as e:
        logger.error(f"Erreur critique dans download_files : {str(e)}")
        errors.append(("Global", f"Erreur critique : {str(e)}"))
    finally:
        driver.quit()

    status_queue.put(("DONE", None))
    return successes, total, errors