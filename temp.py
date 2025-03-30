from playwright.sync_api import sync_playwright
import requests
from datetime import datetime
import pandas as pd
import os
import re
import logging

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
source_file = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"
dest_path = f"C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Downloads/{month}-{day}"
os.makedirs(dest_path, exist_ok=True)

# Lecture du fichier source
df = pd.read_excel(source_file, sheet_name="Source sans doub", dtype=str)
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
    fichier_destination = os.path.join(dest_path, f"{prefix} - {nom_fichier}")
    logger.info(f'Destination : {fichier_destination}')

    try:
        response = requests.get(final_url)
        response.raise_for_status()
        with open(fichier_destination, "wb") as fichier:
            fichier.write(response.content)
    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur lors du téléchargement : {e}")
        return [row[columns[0]]]

    return []

# Fonction pour extraire les paramètres de l'instruction Playwright
def parse_playwright_instruction(instruction):
    """
    Parse une instruction Playwright pour extraire les sélecteurs et les actions.
    Supporte :
    - page.get_by_role("link", name="Téléchargement CSV").click()
    - page.get_by_title("Exporter vers Excel").get_by_role("img").click()
    Retourne une liste d'étapes à exécuter.
    """
    steps = []

    # Vérifier si l'instruction se termine par .click()
    if not instruction.strip().endswith(".click()"):
        raise ValueError(f"Instruction Playwright doit se terminer par .click() : {instruction}")

    # Supprimer le .click() final pour simplifier le parsing
    instruction = instruction.replace(".click()", "")

    # Diviser l'instruction en parties (séparées par des .)
    parts = instruction.split(".")
    current_part = ""
    for part in parts:
        current_part += part
        if "(" in current_part and current_part.count("(") == current_part.count(")"):
            # On a une partie complète (ex. get_by_role("link", name="Téléchargement CSV"))
            if "get_by_role" in current_part:
                role_match = re.search(r'get_by_role\("([^"]+)"', current_part)
                name_match = re.search(r'name="([^"]+)"', current_part)
                if role_match:
                    role = role_match.group(1)
                    name = name_match.group(1) if name_match else None
                    steps.append(("get_by_role", {"role": role, "name": name}))
                else:
                    raise ValueError(f"Instruction get_by_role mal formée : {current_part}")
            elif "get_by_title" in current_part:
                title_match = re.search(r'get_by_title\("([^"]+)"', current_part)
                if title_match:
                    title = title_match.group(1)
                    steps.append(("get_by_title", {"title": title}))
                else:
                    raise ValueError(f"Instruction get_by_title mal formée : {current_part}")
            else:
                raise ValueError(f"Instruction non supportée : {current_part}")
            current_part = ""

    if not steps:
        raise ValueError(f"Aucune instruction valide trouvée dans : {instruction}")

    return steps

# Fonction pour exécuter les étapes Playwright
def execute_playwright_steps(page, steps):
    """
    Exécute les étapes Playwright extraites par parse_playwright_instruction.
    Retourne l'élément final à cliquer.
    """
    element = None
    for step_type, params in steps:
        if step_type == "get_by_role":
            role = params["role"]
            name = params["name"]
            if element is None:
                element = page.get_by_role(role, name=name) if name else page.get_by_role(role)
            else:
                element = element.get_by_role(role, name=name) if name else element.get_by_role(role)
        elif step_type == "get_by_title":
            title = params["title"]
            if element is None:
                element = page.get_by_title(title)
            else:
                element = element.get_by_title(title)
    return element

# Téléchargement via Playwright
def driver_dl(row, browser, context):
    url = row[columns[2]]
    instruction = row[columns[3]]  # Exemple : page.get_by_role("link", name="Téléchargement CSV").click()

    try:
        page = context.new_page()
        page.goto(url)
        logger.info(f"Page ouverte : {url}")

        # Extraire les étapes de l'instruction
        steps = parse_playwright_instruction(instruction)
        logger.info(f"Instruction parsée - Étapes : {steps}")

        # Exécuter les étapes pour localiser l'élément
        element = execute_playwright_steps(page, steps)
        if not element:
            raise Exception("Élément non trouvé pour le clic")

        # Attendre et cliquer sur l'élément
        with page.expect_download() as download_info:
            element.click()
        download = download_info.value

        # Vérifier que le téléchargement a bien eu lieu
        if not download:
            raise Exception("Le téléchargement n'a pas démarré")

        # Sauvegarder le fichier
        file_name = download.suggested_filename
        prefix = sanitize_filename(row[columns[0]])
        destination_path = os.path.join(dest_path, f"{prefix} - {file_name}")
        download.save_as(destination_path)
        logger.info(f"Fichier téléchargé et sauvegardé : {destination_path}")

        page.close()
    except Exception as e:
        logger.error(f"Erreur : {type(e).__name__} - {str(e)}")
        return [row[columns[0]]]

    return []

# Lancement principal
if __name__ == "__main__":
    errors = []

    # Initialisation de Playwright une seule fois pour toutes les lignes
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Mode headless pour performance
        context = browser.new_context()

        for index, row in df.iterrows():
            if row[columns[1]] == "1":
                logger.info(f"Index : {row[columns[0]]} | Valeur adjacente : {row[columns[2]]}")
                logger.info(f"Démarrage extraction {row[columns[0]]} - {row[columns[2]]}")
                errors.extend(simple_dl(row))
                logger.info(f"Fin extraction {row[columns[0]]} - {row[columns[2]]}")
                logger.info("\n---------------------------\n")

            elif row[columns[1]] == "2":
                logger.info(f"Démarrage extraction {row[columns[0]]} - {row[columns[2]]}")
                errors.extend(driver_dl(row, browser, context))
                logger.info(f"Fin extraction {row[columns[0]]} - {row[columns[2]]}")
                logger.info("\n---------------------------\n")

        # Fermeture du navigateur Playwright
        context.close()
        browser.close()

    # Affichage du résultat final
    if not errors:
        logger.info("Tous les fichiers ont bien été téléchargés")
    else:
        logger.error("Erreur de téléchargement pour l(es) index :")
        for e in errors:
            logger.error(e)