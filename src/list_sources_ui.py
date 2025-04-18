# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
import time
import json
from datetime import datetime
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings
from src.utils import load_excel_data, make_unique_titles, insert_dataframe_to_sql
from src.config import DOWNLOAD_DIR

def list_sources_section():
    """Affiche la section 'Traitement et Insertion dans la Base de Données' avec suivi des insertions."""
    st.header("Traitement et Insertion dans la Base de Données")
    st.write("Lancez l'insertion des données dans la base de données et suivez les succès et échecs.")

    # Charger les fichiers téléchargés
    downloaded_files = get_downloaded_files()
    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé. Veuillez lancer le téléchargement dans 'Téléchargement des fichiers'.")
        return

    # Charger les données Excel
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    # Vérifier les fichiers manquants
    sources = list(downloaded_files.keys())
    missing_files = []
    for source in sources:
        settings = get_source_settings(source)
        selected_table = settings.get("selected_table")
        file_paths = downloaded_files.get(source, [])
        if selected_table and not any(os.path.basename(path) == selected_table for path in file_paths):
            # Chercher le fichier le plus récent comme alternative
            if file_paths:
                selected_table = max(file_paths, key=os.path.getmtime)
                settings["selected_table"] = os.path.basename(selected_table)
                with open("C:/Users/Julien/PycharmProjects/PythonProject/Scrap_data_CDG/source_settings.json", "r+") as f:
                    all_settings = json.load(f)
                    all_settings[source] = settings
                    f.seek(0)
                    json.dump(all_settings, f, indent=4)
                    f.truncate()
            else:
                missing_files.append(f"{source}: {selected_table}")

    if missing_files:
        st.warning("Les fichiers suivants sont manquants. Relancez le téléchargement :")
        for mf in missing_files:
            st.write(f"- {mf}")

    # Sélectionner une source
    selected_source = st.selectbox("Sélectionner une source", ["Toutes les sources"] + sources)

    # Bouton pour lancer l'insertion
    if st.button("Lancer l'insertion dans la base de données"):
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

        # Lancer l'insertion
        with st.spinner("Insertion en cours..."):
            if selected_source == "Toutes les sources":
                sources_to_process = sources
            else:
                sources_to_process = [selected_source]

            for source in sources_to_process:
                source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(df_excel[df_excel[columns[0]] == source]) > 0 else source
                try:
                    status_data.append({"Source": source_name, "Statut": "En cours"})
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                    settings = get_source_settings(source)
                    separator = settings.get("separator", ";")
                    page = settings.get("page", 0)
                    title_range = settings.get("title_range", [0, 0, 0, 5])
                    data_range = settings.get("data_range", [1, 10])
                    selected_table = settings.get("selected_table", None)

                    if not selected_table:
                        raise Exception("Aucun tableau sélectionné. Configurez les paramètres dans 'Analyse et Extraction'.")

                    file_paths = downloaded_files.get(source, [])
                    file_path = None
                    for path in file_paths:
                        if os.path.basename(path) == selected_table:
                            file_path = path
                            break

                    if not file_path:
                        raise Exception("Fichier introuvable. Essayez de relancer le téléchargement.")

                    # Récupérer la date de téléchargement (date de modification du fichier)
                    download_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))

                    raw_data = parse_file(file_path, separator, page, selected_columns=None)
                    if not raw_data:
                        raise Exception("Aucune donnée extraite.")

                    titles, data = extract_data(raw_data, title_range, data_range)
                    if not titles or not data:
                        raise Exception("Aucune donnée ou titre extrait.")

                    data_col_count = max(len(row) for row in data) if data else 0
                    if len(titles) != data_col_count:
                        raise Exception(f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes).")

                    unique_titles = make_unique_titles(titles)
                    data_with_datetime = []
                    for row in data:
                        # Utiliser la date de téléchargement pour extraction_datetime
                        data_with_datetime.append([download_datetime] + row)
                        time.sleep(0.001)  # Conserver le léger décalage pour éviter des doublons exacts
                    unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
                    df = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

                    table_name = source.replace(" ", "_").replace("-", "_").lower()
                    insert_dataframe_to_sql(df, table_name, "database.db")

                    status_data[-1]["Statut"] = "Succès"
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)

                except Exception as e:
                    status_data[-1]["Statut"] = "Échec"
                    status_placeholder.dataframe(pd.DataFrame(status_data), use_container_width=True)
                    errors_data.append({"Source": source_name, "Erreur": str(e)})
                    errors_placeholder.dataframe(pd.DataFrame(errors_data), use_container_width=True)

        if not errors_data:
            st.success("Insertion terminée pour toutes les sources !")
        else:
            st.warning("Certaines insertions ont échoué. Consultez les détails à droite.")

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

        file_paths = downloaded_files.get(selected_source, [])
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            st.error(f"Fichier {selected_table} introuvable pour {source_name}. Essayez de relancer le téléchargement dans 'Téléchargement des fichiers'.")
            return

        try:
            # Récupérer la date de téléchargement (date de modification du fichier)
            download_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))

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
                st.error(f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes). Ajustez les paramètres dans 'Analyse et Extraction'.")
                return

            unique_titles = make_unique_titles(titles)
            data_with_datetime = []
            for row in data:
                # Utiliser la date de téléchargement pour extraction_datetime
                data_with_datetime.append([download_datetime] + row)
                time.sleep(0.001)
            unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
            df = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Erreur lors de l'extraction pour {source_name} : {e}")