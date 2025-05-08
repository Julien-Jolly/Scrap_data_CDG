# src/parser.py
import os
import pandas as pd
import tabula
import pdfplumber
import json
import re
import csv
import logging
from bs4 import BeautifulSoup

from src.config import SETTINGS_FILE, get_download_dir
from src.utils import load_excel_data

# Configurer le logging pour s'assurer que les messages DEBUG sont visibles
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def get_downloaded_files(download_dir=None):
    """Retourne un dictionnaire des fichiers téléchargés par source, basé sur l'index du fichier Excel."""
    if download_dir is None:
        download_dir = get_download_dir()
    logger.debug(f"Vérification du répertoire : {download_dir}")
    files = {}

    # Charger les sources depuis le fichier Excel
    df = load_excel_data()
    columns = df.columns.tolist()
    sources = df[columns[0]].unique().tolist()
    # Trier les sources par longueur décroissante pour éviter les correspondances partielles
    sources.sort(key=len, reverse=True)
    logger.debug(f"Sources chargées depuis Excel (triées par longueur) : {sources}")

    if not os.path.exists(download_dir):
        logger.warning(f"Le répertoire {download_dir} n'existe pas.")
        return files

    # Parcourir les fichiers dans download_dir
    for file_name in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file_name)
        if os.path.isfile(file_path):
            file_name_lower = file_name.lower()
            matched_source = None
            for source in sources:
                # Normaliser source pour gérer espaces et casse
                source_prefix = source.strip().replace(" ", "_").lower()
                # Vérifier une correspondance exacte du préfixe avec séparateur
                if (file_name_lower.startswith(source_prefix + " - ") or
                        file_name_lower.startswith(source_prefix + "_") or
                        file_name_lower.startswith(source_prefix + ".")):
                    if matched_source:
                        logger.warning(
                            f"Ambiguité détectée : fichier '{file_name}' correspond à '{matched_source}' et '{source}'")
                    matched_source = source
                    if source not in files:
                        files[source] = []
                    files[source].append(file_path)
                    if file_name_lower.endswith(".html"):
                        logger.debug(f"Fichier HTML trouvé pour source '{source}' : {file_name} (tableau potentiel)")
                    else:
                        logger.debug(f"Fichier trouvé pour source '{source}' : {file_name}")
                    break
            if not matched_source:
                logger.warning(f"Fichier non associé à une source : {file_name}")

    # Trier les fichiers pour chaque source
    for source in files:
        files[source].sort()
        logger.debug(f"Fichiers pour {source} ({len(files[source])} fichiers) : {files[source]}")

    return files


def parse_html_table(file_path, selected_columns=None):
    """Parse un fichier HTML contenant un tableau avec BeautifulSoup."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        logger.debug(f"Contenu HTML lu depuis {file_path} : {html_content[:200]}...")

        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        if not table:
            logger.warning(f"Aucun tableau trouvé dans {file_path}")
            return []

        headers = []
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    a_tag = th.find('a')
                    p_tag = th.find('p')
                    if a_tag:
                        header_text = a_tag.get_text(strip=True)
                    elif p_tag:
                        header_text = p_tag.get_text(strip=True)
                    else:
                        header_text = th.get_text(strip=True)
                    headers.append(header_text)
        else:
            first_row = table.find('tr')
            if first_row:
                for th in first_row.find_all(['th', 'td']):
                    a_tag = th.find('a')
                    p_tag = th.find('p')
                    if a_tag:
                        header_text = a_tag.get_text(strip=True)
                    elif p_tag:
                        header_text = p_tag.get_text(strip=True)
                    else:
                        header_text = th.get_text(strip=True)
                    headers.append(header_text)
        logger.debug(f"En-têtes extraits : {headers}")

        rows = []
        tbody = table.find('tbody')
        if tbody:
            for tr in tbody.find_all('tr'):
                cells = []
                for td in tr.find_all('td'):
                    a_tag = td.find('a')
                    p_tag = td.find('p')
                    span_tag = td.find('span', class_=['text-success-500', 'text-error-500'])
                    if a_tag:
                        cell_text = a_tag.get_text(strip=True)
                    elif p_tag:
                        if span_tag:
                            cell_text = span_tag.get_text(strip=True)
                        else:
                            cell_text = p_tag.get_text(strip=True)
                    else:
                        cell_text = td.get_text(strip=True)
                    cells.append(cell_text)
                if cells:
                    rows.append(cells)
                    logger.debug(f"Ligne extraite : {cells}")
        else:
            trs = table.find_all('tr')
            start_index = 1 if headers else 0
            for tr in trs[start_index:]:
                cells = []
                for td in tr.find_all('td'):
                    a_tag = td.find('a')
                    p_tag = td.find('p')
                    span_tag = td.find('span', class_=['text-success-500', 'text-error-500'])
                    if a_tag:
                        cell_text = a_tag.get_text(strip=True)
                    elif p_tag:
                        if span_tag:
                            cell_text = span_tag.get_text(strip=True)
                        else:
                            cell_text = p_tag.get_text(strip=True)
                    else:
                        cell_text = td.get_text(strip=True)
                    cells.append(cell_text)
                if cells:
                    rows.append(cells)
                    logger.debug(f"Ligne extraite : {cells}")
        logger.debug(f"Total lignes extraites : {len(rows)}")

        return [headers] + rows if headers and rows else [headers] if headers else []
    except Exception as e:
        logger.error(f"Erreur lors du parsing HTML : {e}")
        return []


def parse_file(file_path, separator=None, page=0, selected_columns=None):
    """Parse le fichier selon son extension."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".json":
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                headers = list(data[0].keys())
                rows = [list(item.values()) for item in data]
                return [headers] + rows
        elif isinstance(data, dict):
            return [[k, str(v)] for k, v in data.items()]
        return []
    elif ext == ".csv":
        if separator is None:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    dialect = csv.Sniffer().sniff(f.read(1024))
                    separator = dialect.delimiter
                except csv.Error:
                    separator = ';'
        raw_data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=separator)
            for row in reader:
                raw_data.append([cell.strip() for cell in row])
        return raw_data
    elif ext == ".pdf":
        try:
            # Essayer tabula avec stream=True pour les tableaux sans bordures verticales
            tables = tabula.read_pdf(file_path, pages=str(page + 1), stream=True, multiple_tables=True, guess=True,
                                     columns=None, area=[50, 0, 1000, 1000])
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                # Assouplir le filtrage pour inclure les lignes avec des cellules vides
                result = [row for row in result if len(row) > 0]
                logger.debug(f"Tableau extrait avec tabula stream=True: {result}")
                return result

            # Essayer tabula avec lattice=True
            tables = tabula.read_pdf(file_path, pages=str(page + 1), lattice=True, multiple_tables=True, guess=False)
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                result = [row for row in result if len(row) > 0]
                logger.debug(f"Tableau extrait avec tabula lattice=True: {result}")
                return result

            # Essayer pdfplumber pour extraire les tableaux
            with pdfplumber.open(file_path) as pdf:
                if page >= len(pdf.pages) or page < 0:
                    logger.error(f"Page {page} invalide pour {file_path}")
                    return []
                pdf_page = pdf.pages[page]
                tables = pdf_page.extract_tables()
                if tables:
                    result = []
                    for table in tables:
                        cleaned_table = [[cell.strip() if cell else "" for cell in row] for row in table]
                        result.extend([row for row in cleaned_table if any(cell.strip() for cell in row)])
                    logger.debug(f"Tableau extrait avec pdfplumber: {result}")
                    return result

                # Fallback : extraction texte avec détection manuelle des colonnes
                text = pdf_page.extract_text() or ""
                logger.debug(f"Texte extrait avec pdfplumber: {text}")

                def parse_text_to_table(text):
                    lines = text.splitlines()
                    rows = []
                    # Filtrer les lignes pertinentes (exclure les titres comme "OBJET DE L'AVIS")
                    table_lines = [line for line in lines if
                                   line.strip() and not line.startswith(('-OBJET', '-CARACTÉRISTIQUES'))]
                    # Détecter les colonnes manuellement en utilisant un seuil d'espacement
                    for line in table_lines:
                        # Remplacer plusieurs espaces par un séparateur unique
                        line = re.sub(r'\s{2,}', '  ', line.strip())
                        # Séparer en deux colonnes (basé sur l'espacement)
                        parts = line.split('  ', 1)  # Séparer sur deux espaces
                        if len(parts) == 2:
                            rows.append([parts[0].strip(), parts[1].strip()])
                    return rows

                parsed_data = parse_text_to_table(text)
                logger.debug(f"Données extraites avec détection manuelle: {parsed_data}")
                return parsed_data if parsed_data else []
        except Exception as e:
            logger.error(f"Erreur lors du parsing PDF {file_path}: {e}")
            return []
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(file_path)
        return [df.columns.tolist()] + df.astype(str).values.tolist()
    elif ext == ".html":
        return parse_html_table(file_path, selected_columns)

    return []


def extract_data(raw_data, title_range=None, data_range=None, ignore_titles=False):
    """Extrait les titres et données selon les plages définies."""
    if not raw_data:
        logger.debug("raw_data est vide.")
        return [], []

    logger.debug(f"raw_data avant extraction: {raw_data}")
    logger.debug(f"title_range: {title_range}, data_range: {data_range}, ignore_titles: {ignore_titles}")

    # Déterminer le nombre maximum de colonnes dans raw_data
    max_cols = max(len(row) for row in raw_data) if raw_data else 0
    logger.debug(f"Nombre maximum de colonnes dans raw_data: {max_cols}")

    # Ajuster les lignes pour qu'elles aient toutes le même nombre de colonnes
    adjusted_raw_data = []
    for row in raw_data:
        if len(row) < max_cols:
            row = row + [""] * (max_cols - len(row))
        adjusted_raw_data.append(row)
    logger.debug(f"raw_data ajusté: {adjusted_raw_data}")

    if title_range and not ignore_titles:
        titles = []
        for row in adjusted_raw_data[title_range[0]:title_range[1] + 1]:
            row_titles = row[title_range[2]:title_range[3] + 1]
            if not titles:
                titles = row_titles
            else:
                titles = [f"{t} {r}".strip() if r and t else t or r for t, r in zip(titles, row_titles)]
    else:
        titles = adjusted_raw_data[0] if adjusted_raw_data and not ignore_titles else []
    logger.debug(f"Titres extraits: {titles}")

    data_start = data_range[0] if data_range else 1
    data_end = data_range[1] + 1 if data_range else len(adjusted_raw_data)
    data = [row[title_range[2]:title_range[3] + 1] for row in
            adjusted_raw_data[data_start:data_end]] if title_range and data_range else adjusted_raw_data[
                                                                                       data_start:] if adjusted_raw_data else []
    logger.debug(f"Données extraites: {data}")

    return titles, data


def load_settings():
    """Charge les paramètres sauvegardés."""
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_settings(settings):
    """Sauvegarde les paramètres."""
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=4)


def get_source_settings(source):
    """Retourne les paramètres d’une source spécifique."""
    settings = load_settings()
    return settings.get(source, {
        "separator": ";",
        "page": 0,
        "title_range": [0, 0, 0, 5],
        "data_range": [1, 10],
        "selected_table": None,
        "ignore_titles": False
    })


def update_source_settings(source, separator, page, title_range, data_range, selected_table=None, ignore_titles=False):
    """Met à jour les paramètres d’une source."""
    settings = load_settings()
    settings[source] = {
        "separator": separator,
        "page": page,
        "title_range": title_range,
        "data_range": data_range,
        "selected_table": selected_table,
        "ignore_titles": ignore_titles
    }
    save_settings(settings)