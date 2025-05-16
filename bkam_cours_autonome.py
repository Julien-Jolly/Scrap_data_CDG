import requests
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from urllib.parse import quote
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extract_bkam_exchange_rates_direct.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def generate_date_range(start_date, end_date):
    """Génère une liste de dates entre start_date et end_date, en excluant les week-ends."""
    dates = []
    current_date = start_date
    while current_date <= end_date:
        # Exclure samedis (weekday=5) et dimanches (weekday=6)
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += timedelta(days=1)
    return dates


def construct_url(date):
    """Construit l'URL directe pour télécharger le CSV pour une date donnée."""
    date_str = date.strftime("%d/%m/%Y")
    encoded_date = quote(date_str)
    base_url = "https://www.bkam.ma/export/blockcsv/4550/5312b6def4ad0a94c5a992522868ac0a/cc51b5ce6878a3dc655dae26c47fddf8"
    block_id = "cc51b5ce6878a3dc655dae26c47fddf8"
    return f"{base_url}?date={encoded_date}&block={block_id}"


def download_csv(url, date, output_dir, failed_dir, max_retries=3):
    """Télécharge le fichier CSV et l'enregistre localement avec réessais."""
    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/csv,application/octet-stream,*/*",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-des-changes/Cours-de-change/Cours-de-reference",
            "Connection": "keep-alive"
        }
        response = session.get(url, headers=headers, timeout=20)
        response.raise_for_status()

        # Journaliser les informations de la réponse
        content_type = response.headers.get('Content-Type', '').lower()
        status_code = response.status_code
        content_snippet = response.content[:500].decode('utf-8', errors='ignore')
        logger.debug(
            f"Réponse pour {date.strftime('%d/%m/%Y')} - Status: {status_code}, Content-Type: {content_type}, Snippet: {content_snippet[:100]}...")

        # Vérifier si la réponse est un CSV
        if 'text/csv' not in content_type and 'application/octet-stream' not in content_type:
            logger.warning(f"Contenu non-CSV pour {date.strftime('%d/%m/%Y')}: {content_type}")
            failed_filename = os.path.join(failed_dir, f"failed_{date.strftime('%Y%m%d')}.html")
            os.makedirs(failed_dir, exist_ok=True)
            with open(failed_filename, 'w', encoding='utf-8') as f:
                f.write(content_snippet)
            logger.info(f"Réponse problématique sauvegardée: {failed_filename}")
            return None

        # Écrire temporairement pour tester
        temp_filename = os.path.join(output_dir, f"temp_{date.strftime('%Y%m%d')}.csv")
        os.makedirs(output_dir, exist_ok=True)
        with open(temp_filename, 'wb') as f:
            f.write(response.content)

        # Tenter de lire les colonnes 'Devises' et 'Moyen'
        try:
            df = pd.read_csv(temp_filename, sep=';', encoding='utf-8', usecols=['Devises', 'Moyen'], skiprows=3)
            # Si valide, renommer
            date_str = date.strftime("%Y%m%d")
            csv_filename = os.path.join(output_dir, f"exchange_rates_{date_str}.csv")
            os.rename(temp_filename, csv_filename)
            logger.info(f"CSV téléchargé pour {date.strftime('%d/%m/%Y')}: {csv_filename}")
            return csv_filename
        except Exception as e:
            logger.warning(f"Contenu non-CSV valide pour {date.strftime('%d/%m/%Y')}: {str(e)}")
            failed_filename = os.path.join(failed_dir, f"failed_{date.strftime('%Y%m%d')}.html")
            os.makedirs(failed_dir, exist_ok=True)
            with open(failed_filename, 'w', encoding='utf-8') as f:
                f.write(content_snippet)
            logger.info(f"Réponse problématique sauvegardée: {failed_filename}")
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement du CSV pour {date.strftime('%d/%m/%Y')}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erreur inattendue pour {date.strftime('%d/%m/%Y')}: {type(e).__name__} - {str(e)}")
        return None


def main():
    # Paramètres pour la période complète
    start_date = datetime(2020, 5, 19)
    end_date = datetime(2025, 5, 15)

    output_dir = "bkam_exchange_rates"
    failed_dir = "failed_downloads"

    # Générer les dates (jours ouvrables uniquement)
    dates = generate_date_range(start_date, end_date)
    logger.info(
        f"Extraction des données pour {len(dates)} jours ouvrables: {start_date.strftime('%d/%m/%Y')} à {end_date.strftime('%d/%m/%Y')}")

    # Liste pour stocker les chemins des CSVs
    csv_files = []

    # Boucle sur chaque date
    for date in dates:
        url = construct_url(date)
        logger.debug(f"Traitement de l'URL: {url}")
        csv_filename = download_csv(url, date, output_dir, failed_dir)
        if csv_filename:
            csv_files.append((date, csv_filename))


if __name__ == "__main__":
    main()