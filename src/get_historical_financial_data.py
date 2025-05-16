# src/get_historical_financial_data.py
import requests
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import logging
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from src.config import get_download_dir
from src.utils import sanitize_filename

# Configurer la journalisation
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("temp_download.log")
    ]
)
logger = logging.getLogger(__name__)

# Configuration
YEARS_OF_HISTORY = 3  # Période par défaut

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    after=lambda retry_state: logger.info(f"Tentative {retry_state.attempt_number} pour {retry_state.args[2]['index_name']}")
)
def fetch_index_data(api_url, index_code, index_name, params, headers=None):
    """Effectue une requête API pour une page de données."""
    headers = headers or {}
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    logger.info(f"Envoi requête pour {index_name} (code: {index_code}) à l'URL {api_url}")
    response = requests.get(api_url, params=params, headers=headers, timeout=60)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        logger.error(f"Erreur API pour {index_name}: {data['error']}")
        raise ValueError(data['error'])
    return data

def get_historical_index_data(api_url, index_code, index_name, start_date_str, output_dir, params_template, headers=None, data_key="data", pagination_type="offset_limit"):
    """Récupère l'historique d'une source et sauvegarde en JSON."""
    all_data = []
    page = 1
    limit = params_template.get('limit', 250)
    logger.info(f"Récupération de l'historique pour : {index_name} (Code: {index_code}) depuis {start_date_str}")

    while True:
        try:
            # Préparer les paramètres pour la page actuelle
            params = {
                key: value.format(start_date=start_date_str, index_code=index_code, page=page, limit=limit)
                if isinstance(value, str) and any(var in value for var in ['{start_date}', '{index_code}', '{page}', '{limit}'])
                else value
                for key, value in params_template.items()
            }
            if pagination_type == "offset_limit":
                params['page[offset]'] = (page - 1) * limit
                params['page[limit]'] = limit
            elif pagination_type == "page_per_page":
                params['page'] = page
                params['per_page'] = limit

            data_page = fetch_index_data(api_url, index_code, index_name, params, headers)
            if not data_page.get(data_key):
                break
            all_data.extend(data_page[data_key])
            if len(data_page[data_key]) < limit:
                break
            page += 1
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête pour {index_name} à la page {page}: {e}")
            break
        except json.JSONDecodeError:
            logger.error(f"Erreur de décodage JSON pour {index_name} à la page {page}")
            break
        except ValueError as e:
            logger.error(f"Erreur API pour {index_name} à la page {page}: {e}")
            break

    if all_data:
        safe_index_name = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in index_name).rstrip()
        safe_index_name = safe_index_name.replace(' ', '_')
        output_filename = f"{safe_index_name}_historique_{YEARS_OF_HISTORY}ans.json"
        output_path = os.path.join(output_dir, output_filename)
        os.makedirs(output_dir, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Données pour {index_name} ({YEARS_OF_HISTORY} ans) sauvegardées dans {output_path} ({len(all_data)} enregistrements)")
        return True
    else:
        logger.warning(f"Aucune donnée récupérée pour {index_name}")
        return False

def api_historical_data_dl(row, columns, date_str=None):
    """Fonction de téléchargement pour le type 6 (API historique)."""
    index_name = row[columns[0]]  # Nom de la source
    api_url = row[columns[2]]     # URL de l'API
    config_str = row[columns[3]]  # Chaîne JSON avec index_code, params, headers, etc.
    prefix = sanitize_filename(index_name)

    # Déterminer le répertoire de sortie
    dest_dir = get_download_dir(date_str) if date_str else get_download_dir()
    api_dir = os.path.join(dest_dir, "dl_api")
    logger.info(f"Répertoire de destination pour API : {api_dir}")

    # Calculer la date de début
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=YEARS_OF_HISTORY)
    start_date_str = start_date.strftime("%Y-%m-%d")

    try:
        # Vérifier que les champs de base sont valides
        if not index_name or not api_url or not config_str:
            logger.error(f"Données invalides pour la source : nom={index_name}, url={api_url}, config={config_str}")
            return False, "Nom, URL ou configuration manquante"

        if not api_url.startswith(('http://', 'https://')):
            logger.error(f"URL invalide pour {index_name}: {api_url}")
            return False, "URL invalide (doit commencer par http:// ou https://)"

        # Nettoyer la chaîne JSON
        config_str = config_str.strip()
        while config_str.startswith('"') and config_str.endswith('"'):
            config_str = config_str[1:-1]
        config_str = config_str.replace('""', '"')
        logger.debug(f"Chaîne Config brute pour {index_name}: {repr(config_str)}")

        # Parser la configuration JSON
        try:
            config = json.loads(config_str)
            logger.debug(f"Config parsée pour {index_name}: {config}")
        except json.JSONDecodeError as e:
            logger.error(f"Erreur de parsing de la configuration JSON pour {index_name}: {str(e)}")
            logger.error(f"Chaîne Config problématique : {repr(config_str)}")
            return False, f"Configuration JSON invalide: {str(e)}"

        # Extraire les champs nécessaires
        index_code = config.get("index_code")
        params_template = config.get("params", {})
        headers = config.get("headers", {})
        data_key = config.get("data_key", "data")
        pagination_type = config.get("pagination_type", "offset_limit")

        if not index_code:
            logger.error(f"Code de l'indice manquant dans la configuration pour {index_name}")
            return False, "Code de l'indice manquant"

        # Récupérer et sauvegarder les données
        success = get_historical_index_data(
            api_url, index_code, index_name, start_date_str, api_dir,
            params_template, headers, data_key, pagination_type
        )
        if success:
            return True, None
        else:
            return False, "Aucune donnée récupérée"
    except Exception as e:
        logger.error(f"Erreur lors de la récupération API pour {index_name}: {str(e)}")
        return False, str(e)