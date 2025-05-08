# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
import json
import sqlite3
import re
from datetime import datetime
from src.parser import get_downloaded_files, parse_file
from src.utils import load_excel_data, insert_dataframe_to_sql, load_previous_data, check_cell_changes, \
    clean_column_name
from src.config import get_download_dir
import logging

# Configurer le logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# Chemin du fichier JSON pour les sélections
SELECTIONS_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "selections.json")
# Chemin pour sauvegarder la requête CREATE TABLE
CREATE_TABLE_SQL_PATH = os.path.join(os.path.dirname(__file__), "..", "create_table.sql")


def load_selections():
    """Charge les sélections depuis le fichier JSON. Retourne un dictionnaire vide si le fichier n'existe pas."""
    try:
        if os.path.exists(SELECTIONS_JSON_PATH):
            with open(SELECTIONS_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Erreur lors du chargement de {SELECTIONS_JSON_PATH}: {e}")
        return {}


def clean_column_name(name, idx):
    """Nettoie un nom de colonne pour qu'il soit valide en SQL."""
    name = str(name).lower()
    name = re.sub(r'[^a-zA-Z0-9_%]', '_', name)  # Conserver le % pour distinguer Var. et Var.%
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if name and name[0].isdigit():
        name = f"val_{name}"
    if not name:
        name = f"col_{idx}"
    return name


def generate_create_table_query(columns):
    """Génère une requête CREATE TABLE pour SQL Server basée sur les colonnes du DataFrame."""
    # Colonnes fixes avec leurs types
    fixed_columns = [
        ("id", "INT IDENTITY(1,1) PRIMARY KEY"),
        ("extraction_datetime", "DATETIME"),
        ("source_name", "NVARCHAR(255)"),
        ("date", "NVARCHAR(50)"),
        ("time", "NVARCHAR(50)"),
        ("datetime", "NVARCHAR(50)")
    ]

    # Colonnes dynamiques (toutes typées NVARCHAR(255) par défaut)
    dynamic_columns = [(col, "NVARCHAR(255)") for col in columns
                       if col not in [fc[0] for fc in fixed_columns]]

    # Combiner toutes les colonnes
    all_columns = fixed_columns + dynamic_columns

    # Générer la requête
    column_definitions = [f"[{col_name}] {col_type}" for col_name, col_type in all_columns]
    query = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'extractions')
    BEGIN
        CREATE TABLE extractions (
            {', '.join(column_definitions)}
        )
    END
    """
    return query


def save_create_table_query(query):
    """Sauvegarde la requête CREATE TABLE dans un fichier."""
    try:
        with open(CREATE_TABLE_SQL_PATH, "w", encoding="utf-8") as f:
            f.write(query)
        logger.debug(f"Requête CREATE TABLE sauvegardée dans {CREATE_TABLE_SQL_PATH}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de la requête CREATE TABLE: {e}")


def create_extractions_table():
    """Crée la table 'extractions' dans SQLite (maintien de la compatibilité)."""
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS extractions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extraction_datetime DATETIME NOT NULL,
            source_name TEXT NOT NULL,
            date TEXT,
            time TEXT,
            datetime TEXT
        )
    """)

    conn.commit()
    conn.close()


def add_dynamic_column(column_name):
    """Ajoute une colonne dynamique à la table 'extractions' dans SQLite si elle n'existe pas."""
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    existing_columns = [row[1] for row in cursor.execute("PRAGMA table_info(extractions)").fetchall()]
    if column_name not in existing_columns:
        try:
            cursor.execute(f"ALTER TABLE extractions ADD COLUMN {column_name} TEXT")
            logger.debug(f"Colonne {column_name} ajoutée à la table extractions")
            conn.commit()
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout de la colonne {column_name}: {str(e)}")
            raise
    else:
        logger.debug(f"Colonne {column_name} déjà existante dans la table extractions")
    conn.close()


def validate_and_format_value(value, is_date, is_time):
    """Valide et formate une valeur en fonction des tags is_date et is_time."""
    if pd.isna(value) or value in [None, '', '-', 'NaN']:
        return None

    try:
        if is_date and is_time:
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.isna(parsed):
                raise ValueError(f"Format datetime invalide : {value}")
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        elif is_date:
            parsed = pd.to_datetime(value, errors='coerce')
            if pd.isna(parsed):
                raise ValueError(f"Format date invalide : {value}")
            return parsed.strftime("%Y-%m-%d")
        elif is_time:
            parsed = pd.to_datetime(value, format="%H:%M:%S", errors='coerce')
            if pd.isna(parsed):
                raise ValueError(f"Format time invalide : {value}")
            return parsed.strftime("%H:%M:%S")
        else:
            if isinstance(value, str):
                clean_value = value.replace(" ", "").replace(",", ".")
                try:
                    return float(clean_value)
                except ValueError:
                    return value
            return value
    except Exception as e:
        logger.error(f"Erreur de validation/formatage : {e}")
        return None


def list_sources_section():
    """Affiche la section 'Traitement et Insertion dans la Base de Données' avec suivi des insertions et contrôles."""
    st.header("Traitement et Insertion dans la Base de Données", anchor=False)
    st.write(
        "Lancez l'insertion des données dans la base de données, suivez les succès et échecs, et consultez les anomalies de types ou de nature des valeurs.")

    # Charger les données Excel
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    # Charger les sélections
    selections_data = load_selections()
    if not selections_data:
        st.warning(
            "Aucune sélection trouvée dans selections.json. Configurez les paramètres dans 'Analyse et Extraction'.")
        return

    # Récupérer les sources paramétrées
    sources = list(selections_data.keys())
    if not sources:
        st.warning("Aucune source paramétrée trouvée dans selections.json.")
        return

    # Créer la table extractions dans SQLite
    create_extractions_table()

    # Sélectionner une date
    default_date = datetime.now().strftime("%m-%d")
    # Créer le dossier du jour actuel
    current_download_dir = get_download_dir(default_date)
    logger.debug(f"Dossier du jour actuel créé : {current_download_dir}")
    available_dates = [d for d in os.listdir(os.path.join(os.path.dirname(__file__), "..", "Downloads"))
                       if os.path.isdir(os.path.join(os.path.dirname(__file__), "..", "Downloads", d))]
    selected_date = st.selectbox(
        "Date de l'extraction",
        available_dates,
        index=available_dates.index(default_date) if default_date in available_dates else 0,
        key="date_select",
        help="Sélectionnez la date correspondant au dossier de téléchargement."
    )

    # Charger les fichiers téléchargés
    download_dir = get_download_dir(selected_date)
    logger.debug(f"Dossier de téléchargement sélectionné : {download_dir}")
    downloaded_files = get_downloaded_files(download_dir)
    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé pour la date sélectionnée.")
        return

    # Vérifier les fichiers manquants pour les sources paramétrées
    missing_files = []
    for source in sources:
        if source not in downloaded_files:
            missing_files.append(source)
        elif not downloaded_files[source]:
            missing_files.append(source)

    if missing_files:
        st.warning("Les sources suivantes n'ont pas de fichiers pour la date sélectionnée :")
        for mf in missing_files:
            st.write(f"- {mf}")

    # Sélectionner une source
    selected_source = st.selectbox("Sélectionner une source", ["Toutes les sources"] + sources, key="source_select")

    # Bouton pour lancer l'insertion
    if st.button("Lancer l'insertion dans la base de données", key="insert_button"):
        # Créer deux colonnes pour les tableaux
        col1, col2 = st.columns(2)

        # Tableau de suivi (gauche)
        with col1:
            st.subheader("Suivi des insertions")
            status_placeholder = st.empty()
            status_data = []

        # Tableau des erreurs (droite)
        with col2:
            st.subheader("Erreurs d'insertion")
            errors_placeholder = st.empty()
            errors_data = []

        # Tableau des anomalies
        anomalies_data = []

        # Afficher les DataFrames pour toutes les sources
        st.write("### DataFrames avant insertion")
        dataframes = {}

        # Lancer l'insertion
        with st.spinner("Insertion en cours..."):
            if selected_source == "Toutes les sources":
                sources_to_process = sources
            else:
                sources_to_process = [selected_source]

            for source in sources_to_process:
                source_name = source
                try:
                    status_data.append({"Source": source_name, "Statut": "En cours"})
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                    # Vérifier les sélections
                    selections = selections_data.get(source, [])
                    if not selections:
                        raise Exception("Aucune sélection configurée pour cette source.")

                    # Charger les fichiers
                    file_paths = downloaded_files.get(source, [])
                    if not file_paths:
                        raise Exception("Aucun fichier trouvé pour cette source.")

                    # Utiliser le fichier le plus récent
                    file_path = max(file_paths, key=os.path.getmtime)
                    logger.debug(f"Source {source_name}: Fichier sélectionné : {file_path}")

                    # Récupérer la date de modification
                    extraction_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))

                    # Parser le fichier
                    raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                    if not raw_data:
                        raise Exception("Aucune donnée extraite.")

                    # Valider les sélections
                    max_rows = len(raw_data)
                    max_cols = max(len(row) for row in raw_data) if raw_data else 0
                    valid_selections = []
                    for sel in selections:
                        try:
                            if (sel["title_row"] >= max_rows or sel["title_col"] >= max_cols or
                                    sel["data_col"] >= max_cols or sel["data_row_start"] >= max_rows or
                                    sel["data_row_end"] >= max_rows or sel["data_row_start"] > sel["data_row_end"]):
                                anomalies_data.append({
                                    "Source": source_name,
                                    "Anomalie": f"Combinaison {sel['combination']} invalide : indices hors limites (lignes max: {max_rows}, colonnes max: {max_cols})"
                                })
                                continue
                            valid_selections.append(sel)
                        except KeyError as e:
                            anomalies_data.append({
                                "Source": source_name,
                                "Anomalie": f"Combinaison {sel['combination']} invalide : clé manquante {str(e)}"
                            })

                    if not valid_selections:
                        raise Exception("Aucune sélection valide pour cette source.")

                    # Extraire les données
                    extracted_data = []
                    column_mapping = {}
                    for sel in valid_selections:
                        try:
                            title_row = sel["title_row"]
                            title_col = sel["title_col"]
                            is_date = sel["is_date"]
                            is_time = sel["is_time"]
                            data_col = sel["data_col"]
                            data_row_start = sel["data_row_start"]
                            data_row_end = sel["data_row_end"]

                            # Récupérer le titre dynamiquement
                            title_value = raw_data[title_row][title_col] if raw_data else "Non disponible"
                            logger.debug(
                                f"Source {source_name}, combinaison {sel['combination']}: Titre = {title_value}")

                            # Nettoyer le titre pour la colonne
                            if is_date and is_time:
                                column_name = "datetime"
                            elif is_date:
                                column_name = "date"
                            elif is_time:
                                column_name = "time"
                            else:
                                column_name = clean_column_name(title_value, sel["combination"])
                                add_dynamic_column(column_name)

                            column_mapping[sel["combination"]] = column_name

                            # Extraire les données
                            data_values = []
                            for row_idx in range(data_row_start, data_row_end + 1):
                                try:
                                    value = raw_data[row_idx][data_col] if row_idx < len(raw_data) and data_col < len(
                                        raw_data[row_idx]) else None
                                    formatted_value = validate_and_format_value(value, is_date, is_time)
                                    data_values.append(formatted_value)
                                except IndexError:
                                    data_values.append(None)

                            extracted_data.append({
                                "column": column_name,
                                "values": data_values
                            })
                        except Exception as e:
                            anomalies_data.append({
                                "Source": source_name,
                                "Anomalie": f"Erreur lors de l'extraction pour la combinaison {sel['combination']} : {str(e)}"
                            })

                    # Créer le DataFrame
                    max_rows = max(len(data["values"]) for data in extracted_data) if extracted_data else 0
                    if max_rows == 0:
                        raise Exception("Aucune donnée extraite pour cette source.")

                    df_data = {
                        "extraction_datetime": [extraction_datetime] * max_rows,
                        "source_name": [source_name] * max_rows
                    }
                    for data in extracted_data:
                        column = data["column"]
                        values = data["values"]
                        if len(values) < max_rows:
                            values.extend([None] * (max_rows - len(values)))
                        df_data[column] = values

                    df_current = pd.DataFrame(df_data)
                    logger.debug(f"Source {source_name}: DataFrame créé avec colonnes {df_current.columns.tolist()}")

                    # Générer et sauvegarder la requête CREATE TABLE
                    create_table_query = generate_create_table_query(df_current.columns.tolist())
                    save_create_table_query(create_table_query)
                    st.info(f"Requête CREATE TABLE générée et sauvegardée dans {CREATE_TABLE_SQL_PATH}")

                    # Afficher le DataFrame
                    dataframes[source_name] = df_current
                    st.write(f"**DataFrame pour {source_name}**")
                    st.dataframe(df_current, use_container_width=True)

                    # Charger les données de la veille
                    df_previous = load_previous_data("extractions", "database.db", selected_date)

                    # Vérifier les anomalies
                    cell_anomalies = check_cell_changes(df_current, df_previous, source_name)
                    for anomaly in cell_anomalies:
                        anomalies_data.append({"Source": source_name, "Anomalie": anomaly})

                    # Insérer dans la base SQLite
                    try:
                        insert_dataframe_to_sql(df_current, "extractions", "database.db")
                        logger.debug(f"Source {source_name}: Insertion réussie")
                        status_data[-1]["Statut"] = "Succès"
                    except Exception as e:
                        raise Exception(f"Erreur lors de l'insertion : {str(e)}")

                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                except Exception as e:
                    status_data[-1]["Statut"] = "Échec"
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)
                    errors_data.append({"Source": source_name, "Erreur": str(e)})
                    errors_placeholder.dataframe(pd.DataFrame(errors_data), use_container_width=True)
                    anomalies_data.append({"Source": source_name, "Anomalie": f"Erreur lors du traitement : {str(e)}"})
                    logger.error(f"Source {source_name}: Erreur : {str(e)}")

        # Afficher le tableau des anomalies
        if anomalies_data:
            st.subheader("Tableau des anomalies détectées")
            df_anomalies = pd.DataFrame(anomalies_data)
            st.dataframe(df_anomalies, use_container_width=True)
        else:
            st.info("Aucune anomalie détectée.")

        if not errors_data:
            st.success("Insertion terminée pour toutes les sources !")
        else:
            st.warning("Certaines insertions ont échoué. Consultez les détails à droite.")

    # Afficher le DataFrame pour la source sélectionnée
    if selected_source != "Toutes les sources":
        st.write(f"### DataFrame pour {selected_source}")
        source_name = selected_source
        try:
            selections = selections_data.get(selected_source, [])
            if not selections:
                st.warning(f"Aucune sélection configurée pour {source_name}.")
                return

            file_paths = downloaded_files.get(selected_source, [])
            if not file_paths:
                st.error(f"Aucun fichier trouvé pour {source_name}.")
                return

            file_path = max(file_paths, key=os.path.getmtime)
            extraction_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))
            raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
            if not raw_data:
                st.error(f"Aucune donnée extraite pour {source_name}.")
                return

            # Valider les sélections
            max_rows = len(raw_data)
            max_cols = max(len(row) for row in raw_data) if raw_data else 0
            valid_selections = []
            for sel in selections:
                try:
                    if (sel["title_row"] >= max_rows or sel["title_col"] >= max_cols or
                            sel["data_col"] >= max_cols or sel["data_row_start"] >= max_rows or
                            sel["data_row_end"] >= max_rows or sel["data_row_start"] > sel["data_row_end"]):
                        st.warning(
                            f"Combinaison {sel['combination']} invalide : indices hors limites (lignes max: {max_rows}, colonnes max: {max_cols})")
                        continue
                    valid_selections.append(sel)
                except KeyError as e:
                    st.warning(f"Combinaison {sel['combination']} invalide : clé manquante {str(e)}")

            if not valid_selections:
                st.error(f"Aucune sélection valide pour {source_name}.")
                return

            extracted_data = []
            column_mapping = {}
            for sel in valid_selections:
                try:
                    title_row = sel["title_row"]
                    title_col = sel["title_col"]
                    is_date = sel["is_date"]
                    is_time = sel["is_time"]
                    data_col = sel["data_col"]
                    data_row_start = sel["data_row_start"]
                    data_row_end = sel["data_row_end"]

                    title_value = raw_data[title_row][title_col] if raw_data else "Non disponible"
                    if is_date and is_time:
                        column_name = "datetime"
                    elif is_date:
                        column_name = "date"
                    elif is_time:
                        column_name = "time"
                    else:
                        column_name = clean_column_name(title_value, sel["combination"])
                        add_dynamic_column(column_name)

                    column_mapping[sel["combination"]] = column_name

                    data_values = []
                    for row_idx in range(data_row_start, data_row_end + 1):
                        try:
                            value = raw_data[row_idx][data_col] if row_idx < len(raw_data) and data_col < len(
                                raw_data[row_idx]) else None
                            formatted_value = validate_and_format_value(value, is_date, is_time)
                            data_values.append(formatted_value)
                        except IndexError:
                            data_values.append(None)

                    extracted_data.append({
                        "column": column_name,
                        "values": data_values
                    })
                except Exception as e:
                    st.warning(f"Erreur lors de l'extraction pour la combinaison {sel['combination']} : {str(e)}")

            max_rows = max(len(data["values"]) for data in extracted_data) if extracted_data else 0
            if max_rows == 0:
                st.error(f"Aucune donnée extraite pour {source_name}.")
                return

            df_data = {
                "extraction_datetime": [extraction_datetime] * max_rows,
                "source_name": [source_name] * max_rows
            }
            for data in extracted_data:
                column = data["column"]
                values = data["values"]
                if len(values) < max_rows:
                    values.extend([None] * (max_rows - len(values)))
                df_data[column] = values

            df = pd.DataFrame(df_data)
            # Générer et sauvegarder la requête CREATE TABLE pour cette source
            create_table_query = generate_create_table_query(df.columns.tolist())
            save_create_table_query(create_table_query)
            st.info(f"Requête CREATE TABLE générée et sauvegardée dans {CREATE_TABLE_SQL_PATH}")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Erreur lors de l'extraction pour {source_name} : {str(e)}")


if __name__ == "__main__":
    list_sources_section()