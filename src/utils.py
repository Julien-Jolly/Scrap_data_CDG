# src/utils.py
import pandas as pd
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
import numpy as np
from src.config import SOURCE_FILE
import logging
import sqlite3

logger = logging.getLogger(__name__)


def sanitize_filename(name):
    """Sanitize a string to be used as a filename by removing invalid characters."""
    return re.sub(r'[\\/*?:"<>|]', "", name)

def load_excel_data():
    """Charge les données depuis le fichier Excel."""
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
    # Log des colonnes pour débogage
    logger.debug(f"Colonnes lues depuis l'Excel : {df.columns.tolist()}")
    # Trouver la colonne URL (supposition : troisième colonne, index 2)
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

def make_unique_titles(titles):
    """Ajoute un suffixe '_x' aux titres dupliqués pour les rendre uniques."""
    seen = {}
    unique_titles = []
    for idx, title in enumerate(titles):
        clean_title = clean_column_name(title, idx)
        if clean_title in seen:
            seen[clean_title] += 1
            unique_titles.append(f"{clean_title}_{seen[clean_title]}")
        else:
            seen[clean_title] = 0
            unique_titles.append(clean_title)
    return unique_titles

def clean_column_name(name, idx):
    """Nettoie un nom de colonne pour qu'il soit valide en SQL."""
    name = str(name).lower()
    name = re.sub(r'[^a-zA-Z0-9_%]', '_', name)  # Conserver le % pour distinguer Var. et Var.%
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if name and name[0].isdigit():
        name = f"col_{name}"
    if not name:
        name = f"unnamed_{idx}"
    return name

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

    # Récupérer les colonnes de la table
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_columns = [row[1] for row in cursor.fetchall()]

    # Conserver uniquement les colonnes du DataFrame qui existent dans la table
    common_columns = [col for col in df.columns if col in table_columns]
    if not common_columns:
        logger.error(f"Aucune colonne commune entre le DataFrame et la table {table_name}")
        conn.close()
        raise ValueError(f"Aucune colonne commune entre le DataFrame et la table {table_name}")

    adjusted_df = df[common_columns]

    # Ajouter les colonnes manquantes avec des valeurs NULL
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

        # Vérifier que le DataFrame contient les colonnes nécessaires
        if 'source_name' not in df.columns or 'extraction_datetime' not in df.columns:
            logger.error(f"Le DataFrame doit contenir les colonnes 'source_name' et 'extraction_datetime'")
            conn.close()
            raise ValueError("Le DataFrame doit contenir les colonnes 'source_name' et 'extraction_datetime'")

        # Récupérer la source et la date d'extraction (tronquée au jour)
        source_name = df['source_name'].iloc[0]
        extraction_dates = pd.to_datetime(df['extraction_datetime']).dt.date
        if extraction_dates.empty:
            logger.error(f"Aucune date d'extraction valide dans le DataFrame pour {source_name}")
            conn.close()
            raise ValueError("Aucune date d'extraction valide dans le DataFrame")

        extraction_date = extraction_dates.iloc[0].strftime('%Y-%m-%d')

        # Supprimer les données existantes pour la même source et le même jour
        delete_query = """
        DELETE FROM extractions
        WHERE source_name = ? AND DATE(extraction_datetime) = ?
        """
        cursor.execute(delete_query, (source_name, extraction_date))
        deleted_rows = cursor.rowcount
        logger.debug(f"Supprimé {deleted_rows} lignes pour source {source_name} et date {extraction_date}")

        # Ajuster le DataFrame à la structure de la table
        adjusted_df = adjust_dataframe_to_table(df, table_name, db_path)

        # Insérer les données en mode append
        adjusted_df.to_sql(table_name, conn, if_exists='append', index=False)

        logger.debug(
            f"Insertion réussie de {len(adjusted_df)} lignes dans {table_name} pour source {source_name} avec colonnes {adjusted_df.columns.tolist()}")
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
        WHERE DATE(extraction_datetime) = '{datetime.now().year}-{previous_date}'
        """
        df_previous = pd.read_sql_query(query, engine)
        return df_previous
    except:
        return pd.DataFrame()  # Retourner un DataFrame vide si aucune donnée ou erreur

def check_cell_changes(df_current, df_previous, source):
    """
    Vérifie les changements de types ou de nature des valeurs dans les cellules entre le jour actuel et la veille.
    Ignore les cas où une cellule est vide (NaN, None, '-') dans l'un des DataFrames.
    Gère les valeurs numériques avec espaces (ex. : '0 000' → '0000').
    Retourne une liste d'anomalies (informatif).
    """
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

            # Définir les valeurs considérées comme "vides"
            empty_values = [None, np.nan, '', '-', 'NaN']

            # Vérifier si l'une des valeurs est vide
            current_is_empty = pd.isna(current_value) or current_value in empty_values
            previous_is_empty = pd.isna(previous_value) or previous_value in empty_values

            if current_is_empty or previous_is_empty:
                continue

            try:
                # Nettoyer les valeurs pour gérer les espaces dans les nombres
                if isinstance(current_value, str):
                    current_clean = current_value.replace(" ", "")
                else:
                    current_clean = current_value

                if isinstance(previous_value, str):
                    previous_clean = previous_value.replace(" ", "")
                else:
                    previous_clean = previous_value

                # Déterminer les types
                current_type = type(current_clean).__name__
                previous_type = type(previous_clean).__name__

                # Convertir les types numpy en types Python
                if isinstance(current_clean, np.floating):
                    current_type = 'float'
                if isinstance(previous_clean, np.floating):
                    previous_type = 'float'
                if isinstance(current_clean, np.integer):
                    current_type = 'int'
                if isinstance(previous_clean, np.integer):
                    previous_type = 'int'

                # Vérifier les changements de type
                if current_type != previous_type:
                    anomalies.append(
                        f"Changement de type dans la colonne {col}, ligne {idx + 1} (actuel : {current_type}, veille : {previous_type}, valeur actuelle : {current_value}, valeur veille : {previous_value}).")

                # Vérifier les changements de nature (numérique vs texte)
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

                # Comparer les valeurs numériques nettoyées
                if current_is_numeric and previous_is_numeric and current_numeric != previous_numeric:
                    anomalies.append(
                        f"Différence numérique dans la colonne {col}, ligne {idx + 1} (actuel : {current_numeric}, veille : {previous_numeric}).")

            except Exception as e:
                anomalies.append(f"Erreur lors de la vérification dans la colonne {col}, ligne {idx + 1} : {str(e)}.")

    return anomalies