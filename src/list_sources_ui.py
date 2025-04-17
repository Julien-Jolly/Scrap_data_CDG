# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings
from src.utils import load_excel_data, make_unique_titles, insert_dataframe_to_sql
from src.config import DOWNLOAD_DIR

def list_sources_section():
    """Affiche la section 'Liste des Sources et DataFrames' avec un bouton pour mettre à jour la BDD."""
    st.header("Liste des Sources et DataFrames")
    st.write("Visualisez les DataFrames extraits et mettez à jour la base de données.")

    # Charger les fichiers téléchargés
    downloaded_files = get_downloaded_files()
    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé.")
        return

    # Charger les données Excel
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    # Sélectionner une source
    sources = list(downloaded_files.keys())
    selected_source = st.selectbox("Sélectionner une source", ["Toutes les sources"] + sources)

    # Bouton pour mettre à jour la BDD
    if st.button("Mettre à jour la base de données"):
        with st.spinner("Mise à jour de la base de données en cours..."):
            if selected_source == "Toutes les sources":
                sources_to_process = sources
            else:
                sources_to_process = [selected_source]
            update_database(sources_to_process, downloaded_files, df_excel, columns)
        st.success("Mise à jour de la base de données terminée !")

    # Afficher le DataFrame pour la source sélectionnée
    if selected_source != "Toutes les sources":
        st.write(f"### DataFrame pour {selected_source}")
        source_name = df_excel[df_excel[columns[0]] == selected_source][columns[7]].iloc[0] if len(df_excel[df_excel[columns[0]] == selected_source]) > 0 else selected_source
        settings = get_source_settings(selected_source)
        separator = settings.get("separator", ";")
        page = settings.get("page", 0)
        title_range = settings.get("title_range", [0, 0, 0, 5])
        data_range = settings.get("data_range", [1, 10])
        selected_table = settings.get("selected_table", None)

        if not selected_table:
            st.warning(f"Aucun tableau sélectionné pour {source_name}. Configurez les paramètres dans 'Analyse et Extraction'.")
            return

        file_paths = downloaded_files[selected_source]
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            st.error(f"Fichier {selected_table} introuvable pour {source_name}.")
            return

        try:
            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                st.error(f"Aucune donnée extraite pour {selected_table}.")
                return

            titles, data = extract_data(raw_data, title_range, data_range)
            if not titles or not data:
                st.warning(f"Aucune donnée ou titre extrait pour {selected_table}.")
                return

            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                st.error(f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes).")
                return

            unique_titles = make_unique_titles(titles)
            # Ajouter extraction_datetime à chaque ligne
            data_with_datetime = [[datetime.now()] + row for row in data]
            unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
            df = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Erreur lors de l'extraction pour {source_name} : {e}")

def update_database(sources, downloaded_files, df_excel, columns, db_path="database.db"):
    """Met à jour la base de données pour les sources sélectionnées."""
    for source in sources:
        try:
            st.write(f"Traitement de la source : {source}")
            source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(df_excel[df_excel[columns[0]] == source]) > 0 else source
            settings = get_source_settings(source)
            separator = settings.get("separator", ";")
            page = settings.get("page", 0)
            title_range = settings.get("title_range", [0, 0, 0, 5])
            data_range = settings.get("data_range", [1, 10])
            selected_table = settings.get("selected_table", None)

            if not selected_table:
                st.warning(f"Aucun tableau sélectionné pour {source_name}. Ignoré.")
                continue

            file_paths = downloaded_files[source]
            file_path = None
            for path in file_paths:
                if os.path.basename(path) == selected_table:
                    file_path = path
                    break

            if not file_path:
                st.error(f"Fichier {selected_table} introuvable pour {source_name}. Ignoré.")
                continue

            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                st.error(f"Aucune donnée extraite pour {selected_table}. Ignoré.")
                continue

            titles, data = extract_data(raw_data, title_range, data_range)
            if not titles or not data:
                st.warning(f"Aucune donnée ou titre extrait pour {selected_table}. Ignoré.")
                continue

            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                st.error(f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes) pour {source}. Ignoré.")
                continue

            unique_titles = make_unique_titles(titles)
            # Ajouter extraction_datetime à chaque ligne
            data_with_datetime = [[datetime.now()] + row for row in data]
            unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
            df = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

            table_name = source.replace(" ", "_").replace("-", "_").lower()
            try:
                insert_dataframe_to_sql(df, table_name, db_path)
                st.success(f"DataFrame pour {source_name} inséré dans la table {table_name} avec extraction_datetime.")
            except Exception as e:
                st.error(f"Erreur lors de l'insertion de {source_name} dans la BDD : {e}")
        except Exception as e:
            st.error(f"Erreur lors du traitement de {source_name} : {e}")