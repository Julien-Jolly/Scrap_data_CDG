# src/utils.py
import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from src.config import SOURCE_FILE

def load_excel_data():
    """Charge les données depuis le fichier Excel."""
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
    return df

def save_to_excel(df):
    """Sauvegarde le DataFrame dans le fichier Excel."""
    with pd.ExcelWriter(SOURCE_FILE, mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name="Source sans doub", index=False)

def make_unique_titles(titles):
    """Ajoute un suffixe '_x' aux titres dupliqués pour les rendre uniques."""
    seen = {}
    unique_titles = []
    for title in titles:
        if title in seen:
            seen[title] += 1
            unique_titles.append(f"{title}_{seen[title]}")
        else:
            seen[title] = 0
            unique_titles.append(title)
    return unique_titles

def clean_column_name(name):
    """Nettoie un nom de colonne pour qu'il soit valide en SQL."""
    name = str(name)
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if name and name[0].isdigit():
        name = f"col_{name}"
    if not name:
        name = "unnamed"
    return name.lower()

def delete_existing_data_for_date(engine, table_name, extraction_date):
    """Supprime les données existantes pour une date donnée dans une table."""
    date_str = extraction_date.strftime("%Y-%m-%d")
    query = text(f"DELETE FROM {table_name} WHERE DATE(extraction_datetime) = :date")
    with engine.connect() as connection:
        try:
            connection.execute(query, {"date": date_str})
            connection.commit()
        except Exception as e:
            if "no such table" in str(e).lower():
                pass  # Table n'existe pas, rien à faire
            else:
                raise  # Propager les autres erreurs

def insert_dataframe_to_sql(df, table_name, db_path):
    """Insère un DataFrame dans une table SQL avec suppression des données existantes pour la même date."""
    clean_columns = [clean_column_name(col) for col in df.columns]
    df_clean = df.copy()
    df_clean.columns = clean_columns

    engine = create_engine(f"sqlite:///{db_path}")
    if 'extraction_datetime' in df_clean.columns:
        max_date = pd.to_datetime(df_clean['extraction_datetime']).max()
        delete_existing_data_for_date(engine, table_name, max_date)
    else:
        delete_existing_data_for_date(engine, table_name, datetime.now())

    df_clean.to_sql(table_name, engine, if_exists='append', index=False)