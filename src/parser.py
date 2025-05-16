# src/parser.py
import os
import pandas as pd
import tabula
import pdfplumber
import json
import re
import csv
import datetime
from bs4 import BeautifulSoup
from src.config import SETTINGS_FILE, get_download_dir, configure_logging
from src.utils import load_excel_data, clean_column_name, validate_and_format_value

# Configurer le logging
logger, _ = configure_logging("parser")

def get_downloaded_files(download_dir=None):
    """Retourne un dictionnaire des fichiers téléchargés par source."""
    if download_dir is None:
        download_dir = get_download_dir()
    logger.debug(f"Vérification du répertoire : {download_dir}")
    files = {}
    df = load_excel_data()
    columns = df.columns.tolist()
    sources = df[columns[0]].unique().tolist()
    sources.sort(key=len, reverse=True)
    logger.debug(f"Sources chargées depuis Excel : {sources}")
    if not os.path.exists(download_dir):
        logger.warning(f"Le répertoire {download_dir} n'existe pas.")
        return files
    for file_name in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file_name)
        if os.path.isfile(file_path):
            file_name_lower = file_name.lower()
            matched_source = None
            for source in sources:
                source_prefix = source.strip().replace(" ", "_").lower()
                if (file_name_lower.startswith(source_prefix + " - ") or
                        file_name_lower.startswith(source_prefix + "_") or
                        file_name_lower.startswith(source_prefix + ".")):
                    matched_source = source
                    if source not in files:
                        files[source] = []
                    files[source].append(file_path)
                    logger.debug(f"Fichier trouvé pour source '{source}' : {file_name}")
                    break
            if not matched_source:
                logger.warning(f"Fichier non associé à une source : {file_name}")
    for source in files:
        files[source].sort()
        logger.debug(f"Fichiers pour {source} : {files[source]}")
    return files

def parse_html_table(file_path, selected_columns=None):
    """Parse un fichier HTML contenant un tableau avec BeautifulSoup."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
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
        return [headers] + rows if headers and rows else [headers] if headers else []
    except Exception as e:
        logger.error(f"Erreur lors du parsing HTML : {e}")
        return []

def parse_file(file_path, separator=None, page=0, selected_columns=None):
    """Parse le fichier selon son extension."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".json":
        try:
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
        except Exception as e:
            logger.error(f"Erreur lors du parsing JSON {file_path}: {e}")
            return []
    elif ext == ".csv":
        try:
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
        except Exception as e:
            logger.error(f"Erreur lors du parsing CSV {file_path}: {e}")
            return []
    elif ext == ".pdf":
        try:
            tables = tabula.read_pdf(file_path, pages=str(page + 1), stream=True, multiple_tables=True, guess=True)
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                result = [row for row in result if len(row) > 0]
                return result
            tables = tabula.read_pdf(file_path, pages=str(page + 1), lattice=True, multiple_tables=True, guess=False)
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                result = [row for row in result if len(row) > 0]
                return result
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
                    return result
                text = pdf_page.extract_text() or ""
                def parse_text_to_table(text):
                    lines = text.splitlines()
                    rows = []
                    table_lines = [line for line in lines if
                                   line.strip() and not line.startswith(('-OBJET', '-CARACTÉRISTIQUES'))]
                    for line in table_lines:
                        line = re.sub(r'\s{2,}', '  ', line.strip())
                        parts = line.split('  ', 1)
                        if len(parts) == 2:
                            rows.append([parts[0].strip(), parts[1].strip()])
                    return rows
                parsed_data = parse_text_to_table(text)
                return parsed_data if parsed_data else []
        except Exception as e:
            logger.error(f"Erreur lors du parsing PDF {file_path}: {e}")
            return []
    elif ext in [".xls", ".xlsx"]:
        try:
            df = pd.read_excel(file_path)
            return [df.columns.tolist()] + df.astype(str).values.tolist()
        except Exception as e:
            logger.error(f"Erreur lors du parsing Excel {file_path}: {e}")
            return []
    elif ext == ".html":
        return parse_html_table(file_path, selected_columns)
    logger.error(f"Extension non supportée pour {file_path}: {ext}")
    return []

def extract_data_from_combinations(raw_data, combinations, source_name, table_name):
    """Extrait les données selon les combinaisons définies dans source_settings.json."""
    if not raw_data:
        logger.debug("raw_data est vide.")
        return [], []
    max_rows = len(raw_data)
    max_cols = max(len(row) for row in raw_data) if raw_data else 0
    table_data = []
    unique_titles = set()
    seen_titles = {}  # Pour gérer les doublons
    for idx, comb in enumerate(combinations):
        try:
            if (comb["title_row"] >= max_rows or comb["title_col"] >= max_cols or
                    comb["data_col"] >= max_cols or comb["data_row_start"] >= max_rows or
                    comb["data_row_end"] >= max_rows or
                    comb["data_row_start"] > comb["data_row_end"]):
                logger.warning(f"Combinaison {idx + 1} de {table_name} invalide : indices hors limites (lignes max: {max_rows}, colonnes max: {max_cols})")
                continue
            title_row = comb["title_row"]
            title_col = comb["title_col"]
            data_col = comb["data_col"]
            data_row_start = comb["data_row_start"]
            data_row_end = comb["data_row_end"]
            ignore_titles = comb["ignore_titles"]
            if not ignore_titles:
                title_value = raw_data[title_row][title_col] if raw_data else f"Col_{idx + 1}"
            else:
                title_value = f"Titre_{data_col + 1}"
            column_name = clean_column_name(title_value, idx, seen_titles)
            unique_titles.add(column_name)
            data_values = []
            for row_idx in range(data_row_start, data_row_end + 1):
                try:
                    value = raw_data[row_idx][data_col] if row_idx < len(raw_data) and data_col < len(raw_data[row_idx]) else None
                    formatted_value = validate_and_format_value(value)
                    data_values.append(formatted_value)
                except IndexError:
                    data_values.append(None)
            for value in data_values:
                if value is not None:
                    table_data.append({
                        "source": f"{source_name}-{table_name.replace('.html', '').replace('.csv', '')}",
                        "datetime_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        column_name: value
                    })
                    break
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction pour la combinaison {idx + 1} de {table_name} : {str(e)}")
    return table_data, unique_titles

def load_settings():
    """Charge les paramètres sauvegardés."""
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {SETTINGS_FILE}: {e}")
            return {}
    return {}

def save_settings(settings):
    """Sauvegarde les paramètres."""
    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=4)
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de {SETTINGS_FILE}: {e}")
        raise

def get_source_settings(source):
    """Retourne les paramètres d’une source spécifique."""
    settings = load_settings()
    return settings.get(source, {"tables": {}}) if source else settings

def update_source_settings(source, settings):
    """Met à jour les paramètres d’une source."""
    try:
        all_settings = load_settings()
        source_settings = all_settings.get(source, {"tables": {}})
        source_settings["tables"].update(settings.get("tables", {}))
        all_settings[source] = source_settings
        save_settings(all_settings)
        logger.debug(f"Paramètres mis à jour pour {source}: {source_settings}")
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour des paramètres pour {source}: {e}")
        raise