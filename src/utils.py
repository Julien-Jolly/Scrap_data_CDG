# src/utils.py
import pandas as pd
import re
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
import numpy as np
from src.config import SOURCE_FILE, configure_logging
import logging
import sqlite3

# Configurer le logging
logger, _ = configure_logging("utils")

def sanitize_filename(name):
    """Sanitize a string to be used as a filename by removing invalid characters and normalizing."""
    if not name or not isinstance(name, str):
        return "unnamed"

    # Convertir en chaîne et supprimer les espaces en début/fin
    name = str(name).strip()

    # Remplacer les caractères invalides par des underscores
    name = re.sub(r'[<>:"/\\|?*]', '_', name)

    # Remplacer les espaces multiples et autres caractères spéciaux par un underscore
    name = re.sub(r'\s+', '_', name)

    # Supprimer les underscores en début/fin
    name = name.strip('_')

    # Tronquer à 100 caractères pour éviter les limites du système de fichiers
    name = name[:100]

    # Si le résultat est vide après nettoyage, retourner un nom par défaut
    if not name:
        return "unnamed"

    return name

def load_excel_data():
    """Charge les données depuis le fichier Excel."""
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
    logger.debug(f"Colonnes lues depuis l'Excel : {df.columns.tolist()}")
    url_column = df.columns[2] if len(df.columns) > 2 else None
    if url_column:
        logger.debug(f"URLs lues depuis la colonne '{url_column}' : {df[url_column].tolist()}")
    else:
        logger.warning("Aucune colonne URL trouvée dans l'Excel")
    return df

def save_to_excel(df):
    """Sauvegarde le DataFrame dans le fichier Excel."""
    with pd.ExcelWriter(SOURCE_FILE, mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name="Source sans doub", index=False)

def clean_column_name(name, idx, seen=None):
    """Nettoie un nom de colonne pour qu'il soit valide en SQL et gère les doublons."""
    name = str(name).lower()
    name = re.sub(r'[^a-zA-Z0-9_%]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if name and name[0].isdigit():
        name = f"col_{name}"
    if not name:
        name = f"unnamed_{idx}"
    if seen is not None:
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
    return name

def validate_and_format_value(value):
    """Valide et formate une valeur comme texte ou nombre."""
    if pd.isna(value) or value in [None, '', '-', 'NaN']:
        return None
    if isinstance(value, str):
        clean_value = value.replace(" ", "").replace(",", ".")
        try:
            return float(clean_value)
        except ValueError:
            return value
    return value

def generate_create_table_query(unique_titles):
    """Génère une requête CREATE TABLE pour SQL Server."""
    fixed_columns = [
        ("id", "INT IDENTITY(1,1) PRIMARY KEY"),
        ("source", "NVARCHAR(255)"),
        ("datetime_extraction", "DATETIME")
    ]
    dynamic_columns = [(clean_column_name(title, idx), "NVARCHAR(255)") for idx, title in enumerate(unique_titles)]
    all_columns = fixed_columns + dynamic_columns
    column_definitions = [f"[{col_name}] {col_type}" for col_name, col_type in all_columns]
    query = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Extractions')
    BEGIN
        CREATE TABLE Extractions (
            {', '.join(column_definitions)}
        )
    END
    """
    return query

def save_create_table_query(query, date_str):
    """Sauvegarde la requête CREATE TABLE dans un fichier."""
    EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
    SQL_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "sql_scripts")
    os.makedirs(SQL_SCRIPTS_DIR, exist_ok=True)
    sql_file = os.path.join(SQL_SCRIPTS_DIR, f"create_table_extractions_{date_str}.sql")
    try:
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(query)
        logger.debug(f"Requête CREATE TABLE sauvegardée dans {sql_file}")
        return sql_file
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de la requête CREATE TABLE: {e}")
        raise

def save_csv(df, date_str):
    """Sauvegarde le DataFrame dans un fichier CSV."""
    EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    csv_file = os.path.join(EXPORTS_DIR, f"extractions_{date_str}.csv")
    try:
        df.to_csv(csv_file, index=False, sep=";", encoding="utf-8-sig")
        logger.debug(f"CSV sauvegardé dans {csv_file}")
        return csv_file
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du CSV: {e}")
        raise

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
                pass
            else:
                raise

def adjust_dataframe_to_table(df, table_name, db_path):
    """Ajuste le DataFrame pour correspondre à la structure de la table SQL."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_columns = [row[1] for row in cursor.fetchall()]
    common_columns = [col for col in df.columns if col in table_columns]
    if not common_columns:
        logger.error(f"Aucune colonne commune entre le DataFrame et la table {table_name}")
        conn.close()
        raise ValueError(f"Aucune colonne commune entre le DataFrame et la table {table_name}")
    adjusted_df = df[common_columns]
    for col in table_columns:
        if col not in adjusted_df.columns:
            adjusted_df[col] = None
    conn.close()
    logger.debug(f"Colonnes ajustées pour {table_name}: {adjusted_df.columns.tolist()}")
    return adjusted_df

def insert_dataframe_to_sql(df, table_name, db_path):
    """Insère un DataFrame dans une table SQL en mode append, après suppression des données du même jour pour la même source."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if 'source' not in df.columns or 'datetime_extraction' not in df.columns:
            logger.error(f"Le DataFrame doit contenir les colonnes 'source' et 'datetime_extraction'")
            conn.close()
            raise ValueError("Le DataFrame doit contenir les colonnes 'source' et 'datetime_extraction'")
        source_name = df['source'].iloc[0]
        extraction_dates = pd.to_datetime(df['datetime_extraction']).dt.date
        if extraction_dates.empty:
            logger.error(f"Aucune date d'extraction valide dans le DataFrame pour {source_name}")
            conn.close()
            raise ValueError("Aucune date d'extraction valide dans le DataFrame")
        extraction_date = extraction_dates.iloc[0].strftime('%Y-%m-%d')
        delete_query = """
        DELETE FROM {table_name}
        WHERE source = ? AND DATE(datetime_extraction) = ?
        """
        cursor.execute(delete_query.format(table_name=table_name), (source_name, extraction_date))
        deleted_rows = cursor.rowcount
        logger.debug(f"Supprimé {deleted_rows} lignes pour source {source_name} et date {extraction_date}")
        adjusted_df = adjust_dataframe_to_table(df, table_name, db_path)
        adjusted_df.to_sql(table_name, conn, if_exists='append', index=False)
        logger.debug(f"Insertion réussie de {len(adjusted_df)} lignes dans {table_name} pour source {source_name}")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans {table_name} pour source {source_name}: {str(e)}")
        conn.close()
        raise

def load_previous_data(source, db_path, date_str):
    """Charge les données de la veille pour une source depuis la base SQLite."""
    table_name = source.replace(" ", "_").replace("-", "_").lower()
    previous_date = (datetime.strptime(date_str, "%m-%d") - timedelta(days=1)).strftime("%m-%d")
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        query = f"""
        SELECT * FROM {table_name}
        WHERE DATE(datetime_extraction) = '{datetime.now().year}-{previous_date}'
        """
        df_previous = pd.read_sql_query(query, engine)
        return df_previous
    except:
        return pd.DataFrame()

def check_cell_changes(df_current, df_previous, source):
    """Vérifie les changements de types ou de nature des valeurs entre le jour actuel et la veille."""
    anomalies = []
    if df_previous.empty:
        anomalies.append("Aucune donnée disponible pour la veille.")
        return anomalies
    current_titles = df_current.columns.tolist()
    previous_titles = df_previous.columns.tolist()
    common_cols = [col for col in current_titles if col in previous_titles and col != 'gitlab_id']
    for col in common_cols:
        for idx in range(min(len(df_current), len(df_previous))):
            current_value = df_current.iloc[idx][col]
            previous_value = df_previous.iloc[idx][col]
            empty_values = [None, np.nan, '', '-', 'NaN']
            current_is_empty = pd.isna(current_value) or current_value in empty_values
            previous_is_empty = pd.isna(previous_value) or previous_value in empty_values
            if current_is_empty or previous_is_empty:
                continue
            try:
                if isinstance(current_value, str):
                    current_clean = current_value.replace(" ", "")
                else:
                    current_clean = current_value
                if isinstance(previous_value, str):
                    previous_clean = previous_value.replace(" ", "")
                else:
                    previous_clean = previous_value
                current_type = type(current_clean).__name__
                previous_type = type(previous_clean).__name__
                if isinstance(current_clean, np.floating):
                    current_type = 'float'
                if isinstance(previous_clean, np.floating):
                    previous_type = 'float'
                if isinstance(current_clean, np.integer):
                    current_type = 'int'
                if isinstance(previous_clean, np.integer):
                    previous_type = 'int'
                if current_type != previous_type:
                    anomalies.append(
                        f"Changement de type dans la colonne {col}, ligne {idx + 1} (actuel : {current_type}, veille : {previous_type}, valeur actuelle : {current_value}, valeur veille : {previous_value}).")
                try:
                    current_numeric = float(current_clean)
                    current_is_numeric = True
                except (ValueError, TypeError):
                    current_is_numeric = False
                try:
                    previous_numeric = float(previous_clean)
                    previous_is_numeric = True
                except (ValueError, TypeError):
                    previous_is_numeric = False
                if current_is_numeric != previous_is_numeric:
                    anomalies.append(
                        f"Changement de nature dans la colonne {col}, ligne {idx + 1} (actuel : {'numérique' if current_is_numeric else 'texte'}, veille : {'numérique' if previous_is_numeric else 'texte'}, valeur actuelle : {current_value}, valeur veille : {previous_value}).")
                if current_is_numeric and previous_is_numeric and current_numeric != previous_numeric:
                    anomalies.append(
                        f"Différence numérique dans la colonne {col}, ligne {idx + 1} (actuel : {current_numeric}, veille : {previous_numeric}).")
            except Exception as e:
                anomalies.append(f"Erreur lors de la vérification dans la colonne {col}, ligne {idx + 1} : {str(e)}.")
    return anomalies

def make_unique_titles(titles):
    """Génère des titres uniques en ajoutant des suffixes si nécessaire."""
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