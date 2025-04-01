# src/parser.py
import os
import pandas as pd
import tabula
import pdfplumber
import json
import re
import csv
from datetime import datetime

# Chemin des fichiers téléchargés
actual_date = datetime.now()
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "..", "Downloads", f"{month}-{day}")
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "source_settings.json")

def get_downloaded_files():
    """Retourne une liste des fichiers téléchargés avec leur source."""
    files = {}
    for file_name in os.listdir(DOWNLOAD_DIR):
        if os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name)):
            source = file_name.split(" - ")[0]  # Extrait le préfixe (SourceX)
            files[source] = os.path.join(DOWNLOAD_DIR, file_name)
    return files

def parse_file(file_path, separator=None, page=0):
    """Parse le fichier selon son extension en séparant toutes les lignes avec le séparateur."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".json":
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Convertir le JSON en une liste de listes (format attendu par l'application)
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                headers = list(data[0].keys())
                rows = [list(item.values()) for item in data]
                return [headers] + rows
        # Si le JSON n’est pas une liste de dictionnaires, on le traite comme une structure simple
        elif isinstance(data, dict):
            # Convertir en une liste de paires clé-valeur
            return [[k, str(v)] for k, v in data.items()]
        return []  # À ajuster selon le format exact du JSON
    elif ext == ".csv":
        if separator is None:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    dialect = csv.Sniffer().sniff(f.read(1024))
                    separator = dialect.delimiter
                except csv.Error:
                    separator = ';'  # Fallback sur ';' pour ton cas

        raw_data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=separator)
            for row in reader:
                raw_data.append([cell.strip() for cell in row])
        return raw_data
    elif ext == ".pdf":
        try:
            tables = tabula.read_pdf(file_path, pages=str(page + 1), lattice=True, multiple_tables=True)
            print(f"Tables extraites par tabula-py avec lattice (page {page + 1}): {tables}")
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = []
                for row in df.values:
                    new_row = []
                    for cell in row:
                        if cell != 'nan' and ' ' in cell:
                            parts = cell.split()
                            if any(re.match(r'[+-]?\d*[.,]?\d+%?', p) for p in parts):
                                new_row.extend(parts)
                            else:
                                new_row.append(cell)
                        else:
                            new_row.append(cell)
                    result.append([x for x in new_row if x != 'nan'])
                result = [row for row in result if any(x.strip() for x in row)]
                return result

            tables = tabula.read_pdf(file_path, pages=str(page + 1), stream=True, multiple_tables=True)
            print(f"Tables extraites par tabula-py avec stream (page {page + 1}): {tables}")
            if tables and not tables[0].empty:
                df = tables[0].astype(str)
                result = []
                for row in df.values:
                    new_row = []
                    for cell in row:
                        if cell != 'nan' and ' ' in cell:
                            parts = cell.split()
                            if any(re.match(r'[+-]?\d*[.,]?\d+%?', p) for p in parts):
                                new_row.extend(parts)
                            else:
                                new_row.append(cell)
                        else:
                            new_row.append(cell)
                    result.append([x for x in new_row if x != 'nan'])
                result = [row for row in result if any(x.strip() for x in row)]
                return result

            with pdfplumber.open(file_path) as pdf:
                if page >= len(pdf.pages) or page < 0:
                    print(f"Page {page} invalide pour {file_path}")
                    return []
                pdf_page = pdf.pages[page]
                text = pdf_page.extract_text() or ""
                print(f"Texte brut extrait par pdfplumber (page {page}): {text}")

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
                    print(f"Tableau parsé manuellement : {rows}")
                    return rows

                parsed_data = parse_text_to_table(text)
                return parsed_data if parsed_data else []
        except Exception as e:
            print(f"Erreur avec tabula-py : {e}")
            return []
    elif ext in [".xls", ".xlsx"]:
        df = pd.read_excel(file_path)
        return [df.columns.tolist()] + df.astype(str).values.tolist()

    return []

def extract_data(raw_data, title_range=None, data_range=None):
    """Extrait les titres et données selon les plages définies, avec fusion des lignes de titres."""
    if not raw_data:
        return [], []
    # title_range: [start_row, end_row, start_col, end_col]
    if title_range:
        titles = []
        for row in raw_data[title_range[0]:title_range[1] + 1]:
            row_titles = row[title_range[2]:title_range[3] + 1]
            if not titles:
                titles = row_titles
            else:
                # Fusionner les lignes de titres en combinant les cellules (si non vides)
                titles = [f"{t} {r}".strip() if r and t else t or r for t, r in zip(titles, row_titles)]
    else:
        titles = raw_data[0]

    # data_range: [start_row, end_row]
    data_start = data_range[0] if data_range else 1
    data_end = data_range[1] + 1 if data_range else len(raw_data)
    data = [row[title_range[2]:title_range[3] + 1] for row in raw_data[data_start:data_end]] if title_range and data_range else raw_data[1:]
    return titles, data

def load_settings():
    """Charge les paramètres sauvegardés depuis le fichier JSON."""
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_settings(settings):
    """Sauvegarde les paramètres dans le fichier JSON."""
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=4)

def get_source_settings(source):
    """Retourne les paramètres d’une source spécifique."""
    settings = load_settings()
    return settings.get(source, {
        "separator": ";",
        "page": 0,
        "title_range": [2, 3, 0, 2],  # Ajusté pour ton cas : lignes 2-3, colonnes 0-2
        "data_range": [4, 13]         # Exemple pour les 10 premières lignes de données
    })

def update_source_settings(source, separator, page, title_range, data_range):
    """Met à jour les paramètres d’une source."""
    settings = load_settings()
    settings[source] = {
        "separator": separator,
        "page": page,
        "title_range": title_range,  # [start_row, end_row, start_col, end_col]
        "data_range": data_range
    }
    save_settings(settings)