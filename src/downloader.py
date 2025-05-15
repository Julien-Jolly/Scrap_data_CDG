# src/downloader.py
from selenium import webdriver
from selenium.common import TimeoutException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime, timedelta
from urllib.parse import urljoin
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
import random

from src.config import SOURCE_FILE, DEST_PATH, TEMP_DOWNLOAD_DIR, get_download_dir
from src.get_historical_financial_data import api_historical_data_dl
from src.utils import sanitize_filename  # Importation ajoutée

# Configurer la journalisation sans StreamHandler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("temp_download.log")
    ]
)
logger = logging.getLogger(__name__)

os.makedirs(DEST_PATH, exist_ok=True)

def simple_dl(row, columns, date_str=None):
    """Télécharge un fichier à partir d'une URL directe."""
    from datetime import datetime, timedelta

    # Calculer la date de la veille
    current_date = datetime.now()
    previous_date = current_date - timedelta(days=2)
    year = previous_date.strftime("%Y")
    month = previous_date.strftime("%m")
    day = previous_date.strftime("%d").zfill(2)  # Garantir deux chiffres pour le jour

    final_url = row[columns[2]].format(year=year, month=month, day=day)
    logger.debug(f"URL générée : {final_url}")
    logger.info(f'URL : {final_url}')
    nom_fichier = os.path.basename(final_url)

    prefix = sanitize_filename(row[columns[0]])
    # Utiliser le répertoire de la date spécifiée ou DEST_PATH par défaut
    dest_dir = get_download_dir(date_str) if date_str else DEST_PATH
    fichier_destination = os.path.join(dest_dir, f"{prefix} - {nom_fichier}")
    logger.info(f'Destination : {fichier_destination}')

    try:
        os.makedirs(dest_dir, exist_ok=True)
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
    url = row[columns[2]]
    if not url or not isinstance(url, str) or str(url).strip().lower() in ('nan', ''):
        logger.error(f"URL invalide pour la source {row[columns[0]]}: {url}")
        return False, "URL invalide"

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        logger.info(f"URL ajustée : {url}")

    try:
        logger.info(f"Tentative de scraping de l'URL : {url}")
        driver.get(url)
        tables = WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
        )
        time.sleep(3)
        tables_html = []
        for table in driver.find_elements(By.TAG_NAME, "table"):
            tables_html.append(table.get_attribute('outerHTML'))
        logger.info(f"Nombre de tableaux trouvés : {len(tables_html)}")
        prefix = sanitize_filename(row[columns[0]])
        os.makedirs(DEST_PATH, exist_ok=True)
        success = False
        for idx, table_html in enumerate(tables_html):
            destination_path = os.path.join(DEST_PATH, f"{prefix} - table_{idx}.html")
            with open(destination_path, "w", encoding="utf-8") as f:
                f.write(table_html)
            logger.info(f"Tableau {idx} sauvegardé : {destination_path}")
            success = True
        if success:
            return True, None
        else:
            logger.error(f"Aucun tableau trouvé pour {url}")
            return False, "Aucun tableau trouvé"
    except TimeoutException:
        logger.error(f"Timeout : Aucun tableau trouvé dans les 20 secondes pour {url}")
        return False, "Timeout : Tableau non trouvé"
    except Exception as e:
        logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
        return False, str(e)

def scrape_html_table_with_captcha_dl(row, columns):
    """Téléchargement des tableaux HTML pour le type 4, avec authentification et gestion du CAPTCHA."""
    url = row[columns[2]]
    selector = row.get(columns[3], '').strip()  # Sélecteur CSS ou index de tableau
    prefix = sanitize_filename(row[columns[0]])

    if not url or not isinstance(url, str) or str(url).strip().lower() in ('nan', ''):
        logger.error(f"URL invalide pour la source {row[columns[0]]}: {url}")
        return False, "URL invalide"

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        logger.info(f"URL ajustée : {url}")

    def get_tables(soup, selector):
        """Récupère les tableaux HTML avec un sélecteur CSS ou tous les <table>."""
        tables = []
        if selector and selector.lower() not in ('true', 'false', 'dynamic'):
            try:
                if selector.isdigit():  # Index de tableau
                    all_tables = soup.find_all('table')
                    idx = int(selector)
                    tables = [all_tables[idx]] if idx < len(all_tables) else []
                else:  # Sélecteur CSS
                    tables = soup.select(selector)
            except Exception as e:
                logger.error(f"Erreur avec le sélecteur {selector}: {str(e)}")
                return []
        else:
            tables = soup.find_all('table')
        return tables

    def login(page):
        """Effectue l'authentification sur fr.investing.com."""
        login_url = "https://fr.investing.com/"
        logger.info(f"Navigation vers la page de connexion : {login_url}")
        try:
            # Charger la page avec un timeout réduit et attendre DOMContentLoaded
            page.goto(login_url, timeout=60000, wait_until="domcontentloaded")
            logger.info("Page de connexion chargée (DOMContentLoaded)")

            # Vérifier immédiatement la présence d'un CAPTCHA
            if not handle_captcha(page):
                logger.error("Échec de la gestion du CAPTCHA avant connexion")
                return False

            # Étape 1 : Cliquer sur "Connexion"
            page.wait_for_selector("button[data-test='login-btn']", timeout=10000)
            page.click("button[data-test='login-btn']")
            logger.info("Clic sur 'Connexion' effectué")

            # Étape 2 : Cliquer sur "Continuer avec Email"
            page.wait_for_selector("button.social-auth-button_email__emi7S", timeout=10000)
            page.click("button.social-auth-button_email__emi7S")
            logger.info("Clic sur 'Continuer avec Email' effectué")

            # Étape 3 : Remplir le champ email
            page.wait_for_selector("input[name='email']", timeout=10000)
            page.fill("input[name='email']", "julien.jolly@anailynis.ma")
            logger.info("Email saisi")

            # Étape 4 : Remplir le champ mot de passe
            page.wait_for_selector("input[name='password']", timeout=10000)
            page.fill("input[name='password']", "fgXqV2p6dbfL@9i")
            logger.info("Mot de passe saisi")

            # Étape 5 : Cliquer sur le bouton "Connexion"
            page.wait_for_selector("button.signin_primaryBtn__54rGh", timeout=10000)
            page.click("button.signin_primaryBtn__54rGh")
            logger.info("Clic sur le bouton 'Connexion' effectué")

            # Attendre la confirmation de connexion
            page.wait_for_selector("button[data-test='logout-btn'], .user-area_item__nBsal", timeout=15000)
            logger.info("Connexion réussie")
            return True

        except PlaywrightTimeoutError as e:
            logger.error(f"Échec de la connexion : Timeout - {str(e)}")
            page.screenshot(path="screenshot_login_timeout.png")
            logger.info("Capture d'écran prise : screenshot_login_timeout.png")
            return False
        except Exception as e:
            logger.error(f"Échec de la connexion : {type(e).__name__} - {str(e)}")
            page.screenshot(path="screenshot_login_error.png")
            logger.info("Capture d'écran prise : screenshot_login_error.png")
            return False

    def handle_captcha(page):
        """Tente de détecter et résoudre un CAPTCHA Cloudflare Turnstile."""
        try:
            logger.info("Vérification de la présence d'un CAPTCHA")
            page.wait_for_selector("iframe[src*='challenges.cloudflare.com']", timeout=5000)
            logger.info("CAPTCHA détecté, pause pour résolution manuelle (30 secondes)")
            page.wait_for_timeout(30000)  # Attendre 30s pour résolution manuelle
            return True
        except PlaywrightTimeoutError:
            logger.info("Aucun CAPTCHA détecté")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la gestion du CAPTCHA : {str(e)}")
            page.screenshot(path="screenshot_captcha_error.png")
            logger.info("Capture d'écran prise : screenshot_captcha_error.png")
            return False

    with sync_playwright() as p:
        try:
            # Lancer le navigateur en mode non-headless pour le debug
            browser = p.chromium.launch(headless=False, slow_mo=200)  # Ralentir pour observer
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                locale="fr-FR"
            )
            page = context.new_page()

            # Authentification
            if not login(page):
                logger.error("Échec de l'authentification")
                return False, "Échec de l'authentification"

            # Gérer un CAPTCHA si présent après connexion
            if not handle_captcha(page):
                logger.error("Échec de la gestion du CAPTCHA")
                return False, "Échec de la gestion du CAPTCHA"

            # Naviguer vers l'URL cible
            logger.info(f"Chargement de l'URL cible : {url}")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            logger.info("URL cible chargée (DOMContentLoaded)")

            # Gérer un CAPTCHA si présent sur l'URL cible
            if not handle_captcha(page):
                logger.error("Échec de la gestion du CAPTCHA sur l'URL cible")
                return False, "Échec de la gestion du CAPTCHA"

            # Tenter de fermer un popup
            try:
                page.wait_for_selector(
                    ".popupCloseIcon, [aria-label='close'], .close, [id*='close'], button[class*='close'], div[class*='close'], [class*='modal-close']",
                    timeout=5000
                )
                page.click(
                    ".popupCloseIcon, [aria-label='close'], .close, [id*='close'], button[class*='close'], div[class*='close'], [class*='modal-close']"
                )
                logger.info("Popup fermé")
            except PlaywrightTimeoutError:
                logger.info("Aucun popup à fermer")

            # Attendre les tableaux
            page.wait_for_selector("table", timeout=30000)
            html_content = page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            tables = get_tables(soup, selector)

            if not tables:
                logger.error(f"Aucun tableau trouvé pour {url}")
                return False, "Aucun tableau trouvé"

            # Sauvegarder les tableaux
            os.makedirs(DEST_PATH, exist_ok=True)
            success = False
            saved_tables = 0
            for idx, table in enumerate(tables):
                table_html = str(table)
                destination_path = os.path.join(DEST_PATH, f"{prefix} - table_{idx}.html")
                with open(destination_path, "w", encoding="utf-8") as f:
                    f.write(table_html)
                logger.info(f"Tableau {idx} sauvegardé : {destination_path}")
                saved_tables += 1
                success = True

            if success:
                logger.info(f"{saved_tables} tableaux sauvegardés avec succès")
                return True, None
            else:
                logger.error("Aucun tableau sauvegardé")
                return False, "Aucun tableau sauvegardé"

        except PlaywrightTimeoutError as e:
            logger.error(f"Timeout : {str(e)}")
            page.screenshot(path="screenshot_timeout.png")
            logger.info("Capture d'écran prise : screenshot_timeout.png")
            return False, f"Timeout : {str(e)}"
        except Exception as e:
            logger.error(f"Erreur inattendue : {type(e).__name__} - {str(e)}")
            page.screenshot(path="screenshot_error.png")
            logger.info("Capture d'écran prise : screenshot_error.png")
            return False, f"Erreur : {str(e)}"
        finally:
            browser.close()

def scrape_articles_dl(row, columns):
    """Téléchargement des articles entiers du jour pour le type 5."""
    url = row[columns[2]]
    selector = row.get(columns[3], 'dl.news-list').strip()  # Sélecteur CSS par défaut : dl.news-list
    prefix = sanitize_filename(row[columns[0]])

    if not url or not isinstance(url, str) or str(url).strip().lower() in ('nan', ''):
        logger.error(f"URL invalide pour la source {row[columns[0]]}: {url}")
        return False, "URL invalide"

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        logger.info(f"URL ajustée : {url}")

    # Obtenir la date du jour pour filtrer les articles
    today = datetime.today().strftime('%d/%m/%Y')

    # Configurer Selenium
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("headless")
    options.add_argument("--silent")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("accept-language=fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--enable-unsafe-swiftshader")
    service = Service(ChromeDriverManager().install())
    service.log_path = os.devnull
    driver = None

    try:
        driver = webdriver.Chrome(service=service, options=options)
        logger.info(f"Chargement de la page des actualités : {url}")
        driver.get(url)

        # Attendre que la liste des articles soit chargée
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
        )
        logger.info(f"Liste des articles trouvée avec le sélecteur : {selector}")

        # Extraire le contenu HTML
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # Trouver la liste des articles
        news_list = soup.select_one(selector)
        if not news_list:
            logger.error(f"Aucune liste d'articles trouvée avec le sélecteur {selector}")
            return False, "Aucune liste d'articles trouvée"

        # Extraire les articles du jour
        articles = []
        dt_elements = news_list.find_all('dt', class_='dateTime')
        for dt in dt_elements:
            date_text = dt.get_text(strip=True)
            # Vérifier si la date correspond au jour en cours
            if date_text.startswith(today):
                dd = dt.find_next_sibling('dd', class_='titleDoc')
                if dd:
                    a_tag = dd.find('a')
                    if a_tag and 'href' in a_tag.attrs:
                        title = a_tag.get_text(strip=True)
                        href = a_tag['href']
                        absolute_url = urljoin(url, href)
                        articles.append({'title': title, 'url': absolute_url, 'date': date_text})

        logger.info(f"Nombre d'articles du jour trouvés : {len(articles)}")
        if not articles:
            logger.warning(f"Aucun article du jour trouvé pour {url}")
            return True, None  # Pas d'erreur, mais rien à traiter

        # Extraire et sauvegarder chaque article
        os.makedirs(DEST_PATH, exist_ok=True)
        success = False
        saved_articles = 0
        for article in articles:
            try:
                logger.info(f"Chargement de l'article : {article['url']}")
                driver.get(article['url'])
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Extraire le contenu de l'article
                article_html = driver.page_source
                article_soup = BeautifulSoup(article_html, 'html.parser')

                # Extraction générique du titre
                title_elem = article_soup.select_one('h1, .article-title, [class*="title"]')
                title = title_elem.get_text(strip=True) if title_elem else article['title']

                # Extraction générique du contenu
                content_elems = article_soup.select('article p, .article-content p, [class*="content"] p')
                content = "\n".join(elem.get_text(strip=True) for elem in content_elems) if content_elems else "Aucun contenu trouvé"

                # Créer un fichier HTML pour l'article
                safe_title = sanitize_filename(title[:50])  # Limiter la longueur du titre
                destination_path = os.path.join(DEST_PATH, f"{prefix} - {safe_title}.html")
                with open(destination_path, "w", encoding="utf-8") as f:
                    f.write(f"<h1>{title}</h1>\n")
                    f.write(f"<p>Date: {article['date']}</p>\n")
                    f.write(f"<p>URL: {article['url']}</p>\n")
                    f.write(f"<div>{content}</div>\n")
                logger.info(f"Article sauvegardé : {destination_path}")
                saved_articles += 1
                success = True

            except TimeoutException as e:
                logger.error(f"Timeout lors du chargement de l'article {article['url']}: {str(e)}")
                continue
            except WebDriverException as e:
                logger.error(f"Erreur WebDriver pour l'article {article['url']}: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Erreur inattendue pour l'article {article['url']}: {type(e).__name__} - {str(e)}")
                continue

        if success:
            logger.info(f"{saved_articles} articles sauvegardés avec succès")
            return True, None
        else:
            logger.error("Aucun article sauvegardé")
            return False, "Aucun article sauvegardé"

    except TimeoutException as e:
        logger.error(f"Timeout lors du chargement de la page des actualités {url}: {str(e)}")
        return False, f"Timeout : {str(e)}"
    except WebDriverException as e:
        logger.error(f"Erreur WebDriver pour la page des actualités {url}: {str(e)}")
        return False, f"Erreur WebDriver : {str(e)}"
    except Exception as e:
        logger.error(f"Erreur inattendue pour la page des actualités {url}: {type(e).__name__} - {str(e)}")
        return False, f"Erreur : {str(e)}"
    finally:
        if driver is not None:
            driver.quit()

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
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("headless")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("accept-language=fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--enable-unsafe-swiftshader")
    service = Service(ChromeDriverManager().install())
    service.log_path = os.devnull
    driver = None

    try:
        for index, row in df_to_download.iterrows():
            source = row[columns[0]]
            extraction_type = row[columns[1]]
            try:
                if extraction_type not in ["1", "2", "3", "4", "5", "6"]:  # Ajout du type "6"
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
                        if driver is None:
                            driver = webdriver.Chrome(service=service, options=options)
                        success, error = driver_dl(row, columns, driver)
                    elif extraction_type == "3":
                        if driver is None:
                            driver = webdriver.Chrome(service=service, options=options)
                        success, error = scrape_html_table_dl(row, columns, driver)
                    elif extraction_type == "4":
                        success, error = scrape_html_table_with_captcha_dl(row, columns)
                    elif extraction_type == "5":
                        success, error = scrape_articles_dl(row, columns)
                    elif extraction_type == "6":
                        success, error = api_historical_data_dl(row, columns)
                    if success:
                        successes += 1
                        status_queue.put((source, "✅ Succès"))
                    else:
                        errors.append((source, error))
                        status_queue.put((source, f"❌ Échec ({error})"))
                    logger.info(f"Fin extraction {source} - {row[columns[2]]}")
            except Exception as e:
                logger.error(f"Erreur lors du traitement de {source} : {str(e)}")
                errors.append((source, str(e)))
                status_queue.put((source, f"❌ Échec ({str(e)})"))
    except Exception as e:
        logger.error(f"Erreur critique dans download_files : {str(e)}")
        errors.append(("Global", f"Erreur critique : {str(e)}"))
    finally:
        if driver is not None:
            driver.quit()

    status_queue.put(("DONE", None))
    return successes, total, errors