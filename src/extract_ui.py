# src/extract_ui.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings, update_source_settings, \
    load_settings
from src.utils import load_excel_data, make_unique_titles
from src.config import get_download_dir
import sqlite3
import logging

# Configurer le logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def create_table_if_not_exists(source, columns):
    """Crée une table SQLite pour la source si elle n'existe pas."""
    try:
        conn = sqlite3.connect("scrap_data.db")
        cursor = conn.cursor()

        # Créer un nom de table valide (remplacer les espaces et caractères spéciaux)
        table_name = source.replace(" ", "_").replace("-", "_")

        # Créer une requête pour créer la table
        columns_def = ", ".join([f'"{col}" TEXT' for col in columns])
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {columns_def},
            extraction_date TEXT
        )
        """
        cursor.execute(create_query)
        conn.commit()
        logger.debug(f"Tableau {table_name} créé ou déjà existant avec colonnes: {columns}")
    except Exception as e:
        logger.error(f"Erreur lors de la création de la table {table_name}: {e}")
        raise
    finally:
        conn.close()


def insert_data_to_db(source, df):
    """Insère les données du DataFrame dans la table correspondante."""
    try:
        conn = sqlite3.connect("scrap_data.db")
        cursor = conn.cursor()

        table_name = source.replace(" ", "_").replace("-", "_")
        columns = df.columns.tolist()
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join([f'"{col}"' for col in columns])

        # Ajouter la colonne extraction_date
        columns_str += ", extraction_date"
        placeholders += ", ?"

        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        extraction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for _, row in df.iterrows():
            values = tuple(row) + (extraction_date,)
            cursor.execute(insert_query, values)

        conn.commit()
        logger.debug(f"Données insérées dans {table_name}: {df.to_dict()}")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans {table_name}: {e}")
        raise
    finally:
        conn.close()


def extract_section():
    """Affiche la section 'Analyse et Extraction des Données' avec sélection de date et tableau des sources non paramétrées."""
    st.header("Analyse et Extraction des Données")

    # Charger les données Excel
    df = load_excel_data()
    columns = df.columns.tolist()

    # Récupérer toutes les sources depuis le fichier Excel
    all_sources = df[columns[0]].unique().tolist()

    if not all_sources:
        st.warning("Aucune source trouvée dans le fichier Excel.")
        return

    # Sélectionner une source
    selected_source = st.selectbox("Sélectionner une source", all_sources)

    # Récupérer le nom de la source (colonne 7)
    source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(
        df[df[columns[0]] == selected_source]) > 0 else selected_source

    # Sélecteur de date
    st.write(f"**Nom de la source :** {source_name}")
    default_date = datetime.now().strftime("%m-%d")
    available_dates = [d for d in os.listdir(os.path.join(os.path.dirname(__file__), "..", "Downloads"))
                       if os.path.isdir(os.path.join(os.path.dirname(__file__), "..", "Downloads", d))]
    selected_date = st.selectbox("Date de l'extraction", available_dates,
                                 index=available_dates.index(default_date) if default_date in available_dates else 0)

    # Charger les fichiers pour la date sélectionnée
    download_dir = get_download_dir(selected_date)
    downloaded_files = get_downloaded_files(download_dir)

    # Vérifier si la source sélectionnée a des fichiers pour la date
    if selected_source not in downloaded_files:
        st.warning(f"Aucun fichier trouvé pour la source {source_name} à la date {selected_date}.")
        return

    file_paths = downloaded_files[selected_source]
    previous_source = st.session_state.get("last_source")
    st.session_state["last_source"] = selected_source

    # Charger les paramètres de la source
    settings = get_source_settings(selected_source)
    default_separator = settings.get("separator", ";")
    default_page = settings.get("page", 0)
    default_title_range = settings.get("title_range", [0, 0, 0, 5])
    default_data_range = settings.get("data_range", [1, 10])
    default_selected_table = settings.get("selected_table")
    default_ignore_titles = settings.get("ignore_titles", False)

    default_title_start_row = default_title_range[0]
    default_title_end_row = default_title_range[1]
    default_title_col_start = default_title_range[2]
    default_title_col_end = default_title_range[3]

    if "temp_data_row_start" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_start"] = default_data_range[0]
    if "temp_data_row_end" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_end"] = default_data_range[1]

    # Charger les tableaux
    cache_key = f"{selected_source}_{selected_date}_{default_page}"
    if ("raw_tables" not in st.session_state or
            st.session_state.get("last_cache_key") != cache_key):
        raw_tables = {}
        for file_path in file_paths:
            try:
                table_data = parse_file(file_path, default_separator, default_page, selected_columns=None)
                if table_data:
                    table_name = os.path.basename(file_path)
                    raw_tables[table_name] = table_data
            except Exception as e:
                st.error(f"Erreur de parsing pour {file_path}: {e}")
        st.session_state["raw_tables"] = raw_tables
        st.session_state["last_cache_key"] = cache_key
    else:
        raw_tables = st.session_state["raw_tables"]

    col1, col2 = st.columns([1, 1])

    with col1:
        st.write("### Contenu brut")
        if not raw_tables:
            st.warning(f"Aucun contenu extrait. Vérifiez les fichiers ou les paramètres.")
            raw_data = []  # Initialiser raw_data pour éviter UnboundLocalError
        else:
            selected_table = st.selectbox(
                "Sélectionner un tableau",
                list(raw_tables.keys()),
                index=list(raw_tables.keys()).index(
                    default_selected_table) if default_selected_table in raw_tables else 0
            )
            raw_data = raw_tables[selected_table]
            if not raw_data:
                st.warning(f"Aucun contenu pour {selected_table}.")
            elif len(raw_data) <= 1:
                st.warning(f"Seuls les en-têtes sont extraits pour {selected_table}.")
            else:
                df_raw = pd.DataFrame(raw_data)
                st.dataframe(df_raw, use_container_width=True, height=400)

    with col2:
        st.write("### Paramétrage de l'extraction")
        separator = st.text_input("Séparateur (pour CSV)", value=default_separator)
        page_to_extract = st.number_input("Page à extraire (PDF uniquement)", min_value=0, value=default_page, step=1)

        st.write("#### Plage des titres")
        raw_data_defined = 'raw_data' in locals() and raw_data is not None and len(raw_data) > 0
        max_rows = len(raw_data) if raw_data_defined else 1
        max_cols = max(len(row) for row in raw_data) if raw_data_defined else 1

        # Option pour ignorer les titres
        ignore_titles = st.checkbox("Ignorer les titres (utiliser Titre 1, Titre 2, ...)", value=default_ignore_titles)

        if not ignore_titles:
            title_row_start = st.number_input("Ligne début titres", min_value=0, max_value=max_rows - 1,
                                              value=min(default_title_start_row, max_rows - 1))
            default_title_end_row_adjusted = max(title_row_start, default_title_end_row)
            title_row_end = st.number_input("Ligne fin titres", min_value=title_row_start, max_value=max_rows - 1,
                                            value=min(default_title_end_row_adjusted, max_rows - 1))
        else:
            title_row_start, title_row_end = 0, 0  # Valeurs par défaut inutilisées
            st.info(
                "Les titres seront générés automatiquement (Titre 1, Titre 2, ...) en fonction du nombre de colonnes des données.")

        # Champs pour les colonnes, toujours visibles
        title_col_start = st.number_input("Colonne début", min_value=0, max_value=max_cols - 1,
                                          value=min(default_title_col_start, max_cols - 1))
        default_title_col_end_adjusted = max(title_col_start, default_title_col_end)
        title_col_end = st.number_input("Colonne fin", min_value=title_col_start, max_value=max_cols - 1,
                                        value=min(default_title_col_end_adjusted, max_cols - 1))

        st.write("#### Plage des données")
        data_row_start = st.number_input("Ligne début données", min_value=0, max_value=max_rows - 1,
                                         value=min(st.session_state["temp_data_row_start"], max_rows - 1),
                                         key="data_row_start_input")
        st.session_state["temp_data_row_start"] = data_row_start

        default_data_row_end = max(data_row_start, st.session_state["temp_data_row_end"])
        data_row_end = st.number_input("Ligne fin données", min_value=data_row_start, max_value=max_rows - 1,
                                       value=min(default_data_row_end, max_rows - 1),
                                       key="data_row_end_input")
        st.session_state["temp_data_row_end"] = data_row_end

        if st.button("Mettre à jour le contenu"):
            raw_tables = {}
            for file_path in file_paths:
                try:
                    table_data = parse_file(file_path, separator, page_to_extract, selected_columns=None)
                    if table_data:
                        table_name = os.path.basename(file_path)
                        raw_tables[table_name] = table_data
                except Exception as e:
                    st.error(f"Erreur de parsing pour {file_path}: {e}")
            st.session_state["raw_tables"] = raw_tables
            st.session_state["last_cache_key"] = f"{selected_source}_{selected_date}_{page_to_extract}"
            st.session_state["last_page"] = page_to_extract
            st.success("Contenu mis à jour avec succès.")
            st.rerun()

        if st.button("Appliquer et Sauvegarder"):
            title_range = [title_row_start, title_row_end, title_col_start, title_col_end]
            data_range = [st.session_state["temp_data_row_start"], st.session_state["temp_data_row_end"]]
            update_source_settings(selected_source, separator, page_to_extract, title_range, data_range, selected_table,
                                   ignore_titles)

            if raw_data:
                titles, data = extract_data(raw_data, title_range, data_range, ignore_titles)
                if not data:
                    st.error("Aucune donnée extraite. Vérifiez la plage des données.")
                else:
                    data_col_count = max(len(row) for row in data) if data else 0
                    if ignore_titles:
                        # Générer des titres automatiques
                        titles = [f"Titre {i + 1}" for i in range(data_col_count)]
                    if len(titles) != data_col_count:
                        st.error(
                            f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes). Ajustez les plages ou activez 'Ignorer les titres'.")
                    else:
                        unique_titles = make_unique_titles(titles)
                        try:
                            df_extracted = pd.DataFrame(data, columns=unique_titles)
                            st.write("### Titres extraits")
                            st.dataframe(pd.DataFrame([unique_titles]), use_container_width=True)
                            st.write("### Données extraites")
                            st.dataframe(df_extracted, use_container_width=True)
                            st.success(f"Paramètres sauvegardés pour {selected_source}.")
                            # Stocker les données extraites dans st.session_state
                            st.session_state["extracted_data"] = df_extracted
                        except ValueError as e:
                            st.error(f"Erreur lors de la création du DataFrame : {e}. Vérifiez les données extraites.")
                        except Exception as e:
                            st.error(f"Erreur inattendue : {e}. Contactez le support technique.")
            else:
                st.error(f"Aucune donnée extraite pour la page {page_to_extract}. Vérifiez le fichier.")

        # Bouton pour insérer en base de données
        if "extracted_data" in st.session_state and st.button("Insérer en base de données"):
            df_extracted = st.session_state["extracted_data"]
            try:
                create_table_if_not_exists(selected_source, df_extracted.columns)
                insert_data_to_db(selected_source, df_extracted)
                st.success(f"Données insérées avec succès dans la table pour {selected_source}.")
            except Exception as e:
                st.error(f"Erreur lors de l'insertion en base de données : {e}")

    # Tableau des sources non paramétrées
    st.write("### Sources non paramétrées")
    settings = load_settings()
    parametrized_sources = list(settings.keys())
    non_parametrized_sources = [source for source in all_sources if source not in parametrized_sources]

    if not non_parametrized_sources:
        st.info("Toutes les sources sont paramétrées.")
    else:
        non_parametrized_data = df[df[columns[0]].isin(non_parametrized_sources)][[columns[0], columns[1], columns[5]]]
        non_parametrized_data.columns = ["Source", "Type d'extraction", "Commentaires"]
        st.dataframe(non_parametrized_data, use_container_width=True)