# src/utils.py
import pandas as pd
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
import numpy as np
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


def adjust_dataframe_to_table(df, engine, table_name):
    """
    Ajuste le DataFrame pour qu'il corresponde à la structure de la table existante.
    Ajoute les colonnes manquantes avec des valeurs NULL et ignore les colonnes supplémentaires.
    """
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return df  # Si la table n'existe pas, on retourne le DataFrame tel quel

    # Récupérer les colonnes de la table existante
    existing_columns = {col['name'] for col in inspector.get_columns(table_name)}
    df_columns = set(df.columns)

    # Ajouter les colonnes manquantes dans le DataFrame
    missing_in_df = existing_columns - df_columns
    for col in missing_in_df:
        df[col] = None

    # Ignorer les colonnes du DataFrame qui ne sont pas dans la table
    columns_to_keep = list(df_columns & existing_columns)
    if not columns_to_keep:
        raise ValueError(
            f"Aucune colonne commune entre le DataFrame {list(df_columns)} et la table {list(existing_columns)}."
        )
    adjusted_df = df[columns_to_keep + list(missing_in_df)]

    return adjusted_df


def insert_dataframe_to_sql(df, table_name, db_path):
    """
    Insère un DataFrame dans une table SQL avec suppression des données existantes pour la même date.
    Ajuste le DataFrame pour qu'il corresponde à la table existante sans la supprimer.
    """
    # Nettoyer les noms des colonnes
    clean_columns = [clean_column_name(col, idx) for idx, col in enumerate(df.columns)]
    df_clean = df.copy()
    df_clean.columns = clean_columns

    # Connexion à la base de données
    engine = create_engine(f"sqlite:///{db_path}")

    # Supprimer les données existantes pour la date actuelle
    if 'extraction_datetime' in df_clean.columns:
        max_date = pd.to_datetime(df_clean['extraction_datetime']).max()
        delete_existing_data_for_date(engine, table_name, max_date)
    else:
        delete_existing_data_for_date(engine, table_name, datetime.now())

    # Ajuster le DataFrame pour qu'il corresponde à la table existante
    df_clean = adjust_dataframe_to_table(df_clean, engine, table_name)

    # Insérer les données
    df_clean.to_sql(table_name, engine, if_exists='append', index=False)


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
    Retourne une liste d'anomalies (informatif).
    """
    anomalies = []

    if df_previous.empty:
        anomalies.append("Aucune donnée disponible pour la veille.")
        return anomalies

    current_titles = df_current.columns.tolist()
    previous_titles = df_previous.columns.tolist()
    common_cols = [col for col in current_titles if col in previous_titles and col != 'extraction_datetime']

    for col in common_cols:
        for idx in range(min(len(df_current), len(df_previous))):
            current_value = df_current.iloc[idx][col]
            previous_value = df_previous.iloc[idx][col]

            # Définir les valeurs considérées comme "vides"
            empty_values = [None, np.nan, '', '-', 'NaN']

            # Vérifier si l'une des valeurs est vide
            current_is_empty = pd.isna(current_value) or current_value in empty_values
            previous_is_empty = pd.isna(previous_value) or previous_value in empty_values

            # Ignorer si l'une des valeurs est vide
            if current_is_empty or previous_is_empty:
                continue

            try:
                # Déterminer les types
                current_type = type(current_value).__name__
                previous_type = type(previous_value).__name__

                # Convertir les types numpy en types Python
                if isinstance(current_value, np.floating):
                    current_type = 'float'
                if isinstance(previous_value, np.floating):
                    previous_type = 'float'
                if isinstance(current_value, np.integer):
                    current_type = 'int'
                if isinstance(previous_value, np.integer):
                    previous_type = 'int'

                # Vérifier les changements de type
                if current_type != previous_type:
                    anomalies.append(
                        f"Changement de type dans la colonne {col}, ligne {idx + 1} (actuel : {current_type}, veille : {previous_type}, valeur actuelle : {current_value}, valeur veille : {previous_value}).")

                # Vérifier les changements de nature (numérique vs texte)
                try:
                    float(current_value)
                    current_is_numeric = True
                except (ValueError, TypeError):
                    current_is_numeric = False

                try:
                    float(previous_value)
                    previous_is_numeric = True
                except (ValueError, TypeError):
                    previous_is_numeric = False

                if current_is_numeric != previous_is_numeric:
                    anomalies.append(
                        f"Changement de nature dans la colonne {col}, ligne {idx + 1} (actuel : {'numérique' if current_is_numeric else 'texte'}, veille : {'numérique' if previous_is_numeric else 'texte'}, valeur actuelle : {current_value}, valeur veille : {previous_value}).")

            except Exception as e:
                anomalies.append(f"Erreur lors de la vérification dans la colonne {col}, ligne {idx + 1} : {str(e)}.")

    return anomalies