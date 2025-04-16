# src/parser.py
import os
import pandas as pd
import tabula
import pdfplumber
import json
import re
import csv
from bs4 import BeautifulSoup

from src.config import DOWNLOAD_DIR, SETTINGS_FILE

def get_downloaded_files():
    """Retourne une liste des fichiers téléchargés avec leur source."""
    files = {}
    for file_name in os.listdir(DOWNLOAD_DIR):
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name)):
            source = file_name.split(" - ")[0]
            if source not in files:
                files[source] = []
            files[source].append(os.path.join(DOWNLOAD_DIR, file_name))
    for source in files:
        files[source].sort()
    return files

def parse_html_table(file_path, selected_columns=None):
    """Parse un fichier HTML contenant un tableau avec BeautifulSoup."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        print(f"Contenu HTML lu depuis {file_path} : {html_content[:200]}...")

        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        if not table:
            print(f"Aucun tableau trouvé dans {file_path}")
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
        print(f"En-têtes extraits : {headers}")

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
                    print(f"Ligne extraite : {cells}")
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
                    print(f"Ligne extraite : {cells}")
        print(f"Total lignes extraites : {len(rows)}")

        return [headers] + rows if headers and rows else [headers] if headers else []
    except Exception as e:
        print(f"Erreur lors du parsing HTML : {e}")
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
            tables = tabula.read_pdf(file_path, pages=str(page + 1), lattice=True, multiple_tables=True)
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                result = [row for row in result if any(x.strip() for x in row)]
                return result
            tables = tabula.read_pdf(file_path, pages=str(page + 1), stream=True, multiple_tables=True)
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = [[x for x in row if x != 'nan'] for row in df.values]
                result = [row for row in result if any(x.strip() for x in row)]
                return result
            with pdfplumber.open(file_path) as pdf:
                if page >= len(pdf.pages) or page < 0:
                    print(f"Page {page} invalide pour {file_path}")
                    return []
                pdf_page = pdf.pages[page]
                text = pdf_page.extract_text() or ""
                def parse_text_to_table(text):
                    lines = text.splitlines()
                    rows = []
                    pattern = r"^(.*?)\s+([\d.,]+)\s+([\d.,]+)\s+([+-]?\d+,\d+)\s+([+-]?\d+[.,]\d+)%?-?$"
                    for line in lines:
                        line = line.strip()
                        match = re.match(pattern, line)
                        if match:
                            sector_name, val_2025, val_2024, evol_value, evol_percent = match.groups()
                            val_2025 = val_2025.replace(',', '.')
                            val_2024 = val_2024.replace(',', '.')
                            evol_value = evol_value.replace(',', '.')
                            evol_percent = evol_percent.replace(',', '.').rstrip('-')
                            rows.append([sector_name, val_2025, val_2024, evol_value, evol_percent])
                    return rows
                parsed_data = parse_text_to_table(text)
                return parsed_data if parsed_data else []
        except Exception as e:
            print(f"Erreur avec tabula-py : {e}")
            return []
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(file_path)
        return [df.columns.tolist()] + df.astype(str).values.tolist()
    elif ext == ".html":
        return parse_html_table(file_path, selected_columns)

    return []

def extract_data(raw_data, title_range=None, data_range=None):
    """Extrait les titres et données selon les plages définies."""
    if not raw_data:
        return [], []
    if title_range:
        titles = []
        for row in raw_data[title_range[0]:title_range[1] + 1]:
            row_titles = row[title_range[2]:title_range[3] + 1]
            if not titles:
                titles = row_titles
            else:
                titles = [f"{t} {r}".strip() if r and t else t or r for t, r in zip(titles, row_titles)]
    else:
        titles = raw_data[0] if raw_data else []
    data_start = data_range[0] if data_range else 1
    data_end = data_range[1] + 1 if data_range else len(raw_data)
    data = [row[title_range[2]:title_range[3] + 1] for row in
            raw_data[data_start:data_end]] if title_range and data_range else raw_data[1:] if raw_data else []
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
        "selected_table": None  # Valeur par défaut
    })

def update_source_settings(source, separator, page, title_range, data_range, selected_table=None):
    """Met à jour les paramètres d’une source."""
    settings = load_settings()
    settings[source] = {
        "separator": separator,
        "page": page,
        "title_range": title_range,
        "data_range": data_range,
        "selected_table": selected_table  # Nouveau paramètre
    }
    save_settings(settings)