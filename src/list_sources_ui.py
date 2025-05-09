# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
import json
import re
from datetime import datetime
from src.parser import get_downloaded_files, parse_file
from src.utils import load_excel_data
from src.config import get_download_dir
import logging

# Configurer le logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# Chemins pour les exports
EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "exports")
SQL_SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "sql_scripts")


def ensure_directories():
    """Crée les dossiers exports et sql_scripts s'ils n'existent pas."""
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    os.makedirs(SQL_SCRIPTS_DIR, exist_ok=True)


def load_settings():
    """Charge les paramètres depuis source_settings.json."""
    settings_path = os.path.join(os.path.dirname(__file__), "..", "source_settings.json")
    try:
        if os.path.exists(settings_path):
            with open(settings_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Erreur lors du chargement de {settings_path}: {e}")
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


def infer_sql_type(value):
    """Infère le type SQL basé sur la valeur."""
    if pd.isna(value) or value in [None, '', '-', 'NaN']:
        return "NVARCHAR(255)"

    try:
        float(str(value).replace(",", "."))
        return "FLOAT"
    except (ValueError, TypeError):
        return "NVARCHAR(255)"


def generate_create_table_query(unique_titles):
    """Génère une requête CREATE TABLE pour SQL Server avec tous les titres uniques."""
    fixed_columns = [
        ("id", "INT IDENTITY(1,1) PRIMARY KEY"),
        ("source", "NVARCHAR(255)"),
        ("datetime_extraction", "DATETIME")
    ]

    # Colonnes dynamiques basées sur les titres uniques
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


def save_create_table_query(query):
    """Sauvegarde la requête CREATE TABLE dans un fichier."""
    ensure_directories()
    sql_file = os.path.join(SQL_SCRIPTS_DIR, "create_table_extractions.sql")
    try:
        with open(sql_file, "w", encoding="utf-8") as f:
            f.write(query)
        logger.debug(f"Requête CREATE TABLE sauvegardée dans {sql_file}")
        return sql_file
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de la requête CREATE TABLE: {e}")
        raise


def save_csv(df, selected_date):
    """Sauvegarde le DataFrame dans un fichier CSV."""
    ensure_directories()
    csv_file = os.path.join(EXPORTS_DIR, f"extractions_{selected_date}.csv")
    try:
        df.to_csv(csv_file, index=False, sep=";", encoding="utf-8-sig")
        logger.debug(f"CSV sauvegardé dans {csv_file}")
        return csv_file
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du CSV: {e}")
        raise


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


def list_sources_section():
    """Affiche la section 'Traitement et Insertion dans la Base de Données'."""
    st.header("Traitement et Insertion dans la Base de Données", anchor=False)
    st.write(
        "Préparez les données pour l'importation dans SQL Server Management Studio (SSMS) en générant un fichier CSV et un script SQL de création de table."
    )

    # Charger les données Excel
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    # Charger les paramètres
    settings_data = load_settings()
    if not settings_data:
        st.warning("Aucun paramètre trouvé dans source_settings.json. Configurez les paramètres dans 'Analyse et Extraction'.")
        return

    # Récupérer les sources paramétrées
    sources = list(settings_data.keys())
    if not sources:
        st.warning("Aucune source paramétrée trouvée dans source_settings.json.")
        return

    # Sélectionner une date
    default_date = datetime.now().strftime("%m-%d")
    downloads_base = os.path.join(os.path.dirname(__file__), "..", "Downloads")
    available_dates = sorted([
        d for d in os.listdir(downloads_base)
        if os.path.isdir(os.path.join(downloads_base, d)) and re.match(r"\d{2}-\d{2}", d)
    ])
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
        if source not in downloaded_files or not downloaded_files[source]:
            missing_files.append(source)

    if missing_files:
        st.warning("Les sources suivantes n'ont pas de fichiers pour la date sélectionnée :")
        for mf in missing_files:
            st.write(f"- {mf}")

    # Sélectionner une source
    selected_source = st.selectbox("Sélectionner une source", ["Toutes les sources"] + sources, key="source_select")

    # Bouton pour générer les fichiers
    if st.button("Générer CSV et script SQL", key="generate_button"):
        # Créer deux colonnes pour les tableaux
        col1, col2 = st.columns(2)

        # Tableau de suivi (gauche)
        with col1:
            st.subheader("Suivi de la génération")
            status_placeholder = st.empty()
            status_data = []

        # Tableau des erreurs (droite)
        with col2:
            st.subheader("Erreurs de génération")
            errors_placeholder = st.empty()
            errors_data = []

        # Tableau des anomalies
        anomalies_data = []

        # Collecter tous les titres uniques
        unique_titles = set()
        for source in sources:
            source_settings = settings_data.get(source, {}).get("tables", {})
            for table_name, table_settings in source_settings.items():
                for idx, comb in enumerate(table_settings.get("combinations", [])):
                    if not comb.get("ignore_titles", False):
                        # Les titres seront extraits des fichiers plus tard
                        pass
                    else:
                        title = f"Titre_{comb['data_col'] + 1}"
                        unique_titles.add(title)

        # Afficher le DataFrame global
        st.write("### DataFrame global")
        global_data = []

        # Lancer la génération
        with st.spinner("Génération en cours..."):
            if selected_source == "Toutes les sources":
                sources_to_process = sources
            else:
                sources_to_process = [selected_source]

            for source in sources_to_process:
                source_name = source
                try:
                    status_data.append({"Source": source_name, "Statut": "En cours"})
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                    # Vérifier les paramètres
                    source_settings = settings_data.get(source, {}).get("tables", {})
                    if not source_settings:
                        raise Exception("Aucun paramètre configuré pour cette source.")

                    # Charger les fichiers
                    file_paths = downloaded_files.get(source, [])
                    if not file_paths:
                        raise Exception("Aucun fichier trouvé pour cette source.")

                    # Traiter chaque tableau
                    for table_name, table_settings in source_settings.items():
                        # Trouver le fichier correspondant
                        file_path = next((fp for fp in file_paths if os.path.basename(fp) == table_name), None)
                        if not file_path:
                            anomalies_data.append({
                                "Source": source_name,
                                "Anomalie": f"Fichier pour le tableau {table_name} non trouvé."
                            })
                            continue

                        # Récupérer la date de modification
                        extraction_datetime = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime(
                            "%Y-%m-%d %H:%M:%S")

                        # Parser le fichier
                        raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                        if not raw_data:
                            raise Exception(f"Aucune donnée extraite pour {table_name}.")

                        # Valider les combinaisons
                        max_rows = len(raw_data)
                        max_cols = max(len(row) for row in raw_data) if raw_data else 0
                        table_data = {"source": f"{source_name}-{table_name.replace('.html', '')}",
                                      "datetime_extraction": extraction_datetime}

                        for idx, comb in enumerate(table_settings.get("combinations", [])):
                            try:
                                if (comb["title_row"] >= max_rows or comb["title_col"] >= max_cols or
                                        comb["data_col"] >= max_cols or comb["data_row_start"] >= max_rows or
                                        comb["data_row_end"] >= max_rows or
                                        comb["data_row_start"] > comb["data_row_end"]):
                                    anomalies_data.append({
                                        "Source": source_name,
                                        "Anomalie": f"Combinaison {idx + 1} de {table_name} invalide : indices hors limites (lignes max: {max_rows}, colonnes max: {max_cols})"
                                    })
                                    continue

                                title_row = comb["title_row"]
                                title_col = comb["title_col"]
                                data_col = comb["data_col"]
                                data_row_start = comb["data_row_start"]
                                data_row_end = comb["data_row_end"]
                                ignore_titles = comb["ignore_titles"]

                                # Récupérer le titre
                                if not ignore_titles:
                                    title_value = raw_data[title_row][title_col] if raw_data else f"Col_{idx + 1}"
                                else:
                                    title_value = f"Titre_{data_col + 1}"

                                column_name = clean_column_name(title_value, idx)
                                unique_titles.add(column_name)

                                # Extraire les données
                                data_values = []
                                for row_idx in range(data_row_start, data_row_end + 1):
                                    try:
                                        value = raw_data[row_idx][data_col] if row_idx < len(
                                            raw_data) and data_col < len(raw_data[row_idx]) else None
                                        formatted_value = validate_and_format_value(value)
                                        data_values.append(formatted_value)
                                    except IndexError:
                                        data_values.append(None)

                                # Ajouter la première valeur non nulle (simplification : une valeur par combinaison)
                                for value in data_values:
                                    if value is not None:
                                        table_data[column_name] = value
                                        break
                            except Exception as e:
                                anomalies_data.append({
                                    "Source": source_name,
                                    "Anomalie": f"Erreur lors de l'extraction pour la combinaison {idx + 1} de {table_name} : {str(e)}"
                                })

                        global_data.append(table_data)

                    status_data[-1]["Statut"] = "Succès"
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                except Exception as e:
                    status_data[-1]["Statut"] = "Échec"
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)
                    errors_data.append({"Source": source_name, "Erreur": str(e)})
                    errors_placeholder.dataframe(pd.DataFrame(errors_data), use_container_width=True)
                    anomalies_data.append({"Source": source_name, "Anomalie": f"Erreur lors du traitement : {str(e)}"})
                    logger.error(f"Source {source_name}: Erreur : {str(e)}")

            # Créer le DataFrame global
            if global_data:
                df_global = pd.DataFrame(global_data)
                for title in unique_titles:
                    if title not in df_global.columns:
                        df_global[title] = None
                df_global = df_global[["source", "datetime_extraction"] + sorted(unique_titles)]

                # Générer et sauvegarder la requête CREATE TABLE
                create_table_query = generate_create_table_query(unique_titles)
                sql_file = save_create_table_query(create_table_query)

                # Sauvegarder le CSV
                csv_file = save_csv(df_global, selected_date)

                # Afficher le DataFrame
                st.dataframe(df_global, use_container_width=True)
                st.info(f"CSV généré : {csv_file}")
                st.info(f"Script SQL généré : {sql_file}")
            else:
                st.error("Aucune donnée extraite pour aucune source.")

        # Afficher le tableau des anomalies
        if anomalies_data:
            st.subheader("Tableau des anomalies détectées")
            df_anomalies = pd.DataFrame(anomalies_data)
            st.dataframe(df_anomalies, use_container_width=True)
        else:
            st.info("Aucune anomalie détectée.")

        if not errors_data:
            st.success("Génération terminée pour toutes les sources !")
        else:
            st.warning("Certaines générations ont échoué. Consultez les détails à droite.")

    # Afficher le DataFrame pour la source sélectionnée
    if selected_source != "Toutes les sources":
        st.write(f"### DataFrame pour {selected_source}")
        source_name = selected_source
        try:
            source_settings = settings_data.get(source_name, {}).get("tables", {})
            if not source_settings:
                st.warning(f"Aucun paramètre configuré pour {source_name}.")
                return

            file_paths = downloaded_files.get(source_name, [])
            if not file_paths:
                st.error(f"Aucun fichier trouvé pour {source_name}.")
                return

            local_data = []
            local_titles = set()
            for table_name, table_settings in source_settings.items():
                file_path = next((fp for fp in file_paths if os.path.basename(fp) == table_name), None)
                if not file_path:
                    st.warning(f"Fichier pour le tableau {table_name} non trouvé.")
                    continue

                extraction_datetime = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                if not raw_data:
                    st.error(f"Aucune donnée extraite pour {table_name}.")
                    continue

                max_rows = len(raw_data)
                max_cols = max(len(row) for row in raw_data) if raw_data else 0
                table_data = {"source": f"{source_name}-{table_name.replace('.html', '')}",
                              "datetime_extraction": extraction_datetime}

                for idx, comb in enumerate(table_settings.get("combinations", [])):
                    try:
                        if (comb["title_row"] >= max_rows or comb["title_col"] >= max_cols or
                                comb["data_col"] >= max_cols or comb["data_row_start"] >= max_rows or
                                comb["data_row_end"] >= max_rows or
                                comb["data_row_start"] > comb["data_row_end"]):
                            st.warning(
                                f"Combinaison {idx + 1} de {table_name} invalide : indices hors limites (lignes max: {max_rows}, colonnes max: {max_cols})")
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

                        column_name = clean_column_name(title_value, idx)
                        local_titles.add(column_name)

                        data_values = []
                        for row_idx in range(data_row_start, data_row_end + 1):
                            try:
                                value = raw_data[row_idx][data_col] if row_idx < len(raw_data) and data_col < len(
                                    raw_data[row_idx]) else None
                                formatted_value = validate_and_format_value(value)
                                data_values.append(formatted_value)
                            except IndexError:
                                data_values.append(None)

                        for value in data_values:
                            if value is not None:
                                table_data[column_name] = value
                                break
                    except Exception as e:
                        st.warning(
                            f"Erreur lors de l'extraction pour la combinaison {idx + 1} de {table_name} : {str(e)}")

                local_data.append(table_data)

            if local_data:
                df_local = pd.DataFrame(local_data)
                for title in local_titles:
                    if title not in df_local.columns:
                        df_local[title] = None
                df_local = df_local[["source", "datetime_extraction"] + sorted(local_titles)]
                st.dataframe(df_local, use_container_width=True)
            else:
                st.error(f"Aucune donnée extraite pour {source_name}.")
        except Exception as e:
            st.error(f"Erreur lors de l'extraction pour {source_name} : {str(e)}")


if __name__ == "__main__":
    list_sources_section()