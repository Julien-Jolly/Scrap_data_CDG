# src/extract_ui.py
import streamlit as st
import pandas as pd
import os
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings, update_source_settings
from src.utils import load_excel_data, make_unique_titles

def extract_section():
    """Affiche la section 'Analyse et Extraction'."""
    st.header("Analyse et Extraction des Données")

    downloaded_files = get_downloaded_files()

    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé.")
        return

    selected_source = st.selectbox("Sélectionner une source", list(downloaded_files.keys()))
    file_paths = downloaded_files[selected_source]

    previous_source = st.session_state.get("last_source")
    st.session_state["last_source"] = selected_source

    df = load_excel_data()
    columns = df.columns.tolist()
    source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(df[df[columns[0]] == selected_source]) > 0 else selected_source

    settings = get_source_settings(selected_source)
    default_separator = settings.get("separator", ";")
    default_page = settings.get("page", 0)
    default_title_range = settings.get("title_range", [0, 0, 0, 5])
    default_data_range = settings.get("data_range", [1, 10])
    default_selected_table = settings.get("selected_table")  # Charger la table sauvegardée

    default_title_start_row = default_title_range[0]
    default_title_end_row = default_title_range[1]
    default_title_col_start = default_title_range[2]
    default_title_col_end = default_title_range[3]

    if "temp_data_row_start" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_start"] = default_data_range[0]
    if "temp_data_row_end" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_end"] = default_data_range[1]

    # Charger tous les tableaux pour la source
    if ("raw_tables" not in st.session_state or
            previous_source != selected_source or
            st.session_state.get("last_page") != default_page):
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
        st.session_state["last_page"] = default_page
    else:
        raw_tables = st.session_state["raw_tables"]

    col1, col2 = st.columns([1, 1])

    with col1:
        st.write("### Contenu brut")
        st.write(f"**Nom de la source :** {source_name}")
        if not raw_tables:
            st.warning(f"Aucun contenu extrait. Vérifiez les fichiers ou les paramètres.")
        else:
            # Sélection du tableau avec valeur par défaut
            selected_table = st.selectbox(
                "Sélectionner un tableau",
                list(raw_tables.keys()),
                index=list(raw_tables.keys()).index(default_selected_table) if default_selected_table in raw_tables else 0
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

        # Paramètres des plages
        st.write("#### Plage des titres")
        raw_data_defined = raw_data is not None and len(raw_data) > 0
        max_rows = len(raw_data) if raw_data_defined else 1
        max_cols = max(len(row) for row in raw_data) if raw_data_defined else 1

        title_row_start = st.number_input("Ligne début titres", min_value=0, max_value=max_rows - 1,
                                          value=min(default_title_start_row, max_rows - 1))
        default_title_end_row_adjusted = max(title_row_start, default_title_end_row)
        title_row_end = st.number_input("Ligne fin titres", min_value=title_row_start, max_value=max_rows - 1,
                                        value=min(default_title_end_row_adjusted, max_rows - 1))
        title_col_start = st.number_input("Colonne début titres", min_value=0, max_value=max_cols - 1,
                                          value=min(default_title_col_start, max_cols - 1))
        default_title_col_end_adjusted = max(title_col_start, default_title_col_end)
        title_col_end = st.number_input("Colonne fin titres", min_value=title_col_start, max_value=max_cols - 1,
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
            st.session_state["last_page"] = page_to_extract
            st.session_state["last_source"] = selected_source
            st.success("Contenu mis à jour avec succès.")
            st.rerun()

        if st.button("Appliquer et Sauvegarder"):
            title_range = [title_row_start, title_row_end, title_col_start, title_col_end]
            data_range = [st.session_state["temp_data_row_start"], st.session_state["temp_data_row_end"]]
            update_source_settings(selected_source, separator, page_to_extract, title_range, data_range, selected_table)

            if raw_data:
                titles, data = extract_data(raw_data, title_range, data_range)

                # Vérifier si les données et titres sont valides
                if not titles:
                    st.error("Aucun titre extrait. Vérifiez la plage des titres.")
                elif not data:
                    st.error("Aucune donnée extraite. Vérifiez la plage des données.")
                else:
                    # Vérifier la compatibilité entre titres et données
                    data_col_count = max(len(row) for row in data) if data else 0
                    if len(titles) != data_col_count:
                        st.error(
                            f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes). Ajustez les plages.")
                    else:
                        # Rendre les titres uniques
                        unique_titles = make_unique_titles(titles)
                        try:
                            # Créer le DataFrame
                            df_extracted = pd.DataFrame(data, columns=unique_titles)
                            st.write("### Titres extraits")
                            st.dataframe(pd.DataFrame([unique_titles]), use_container_width=True)
                            st.write("### Données extraites")
                            st.dataframe(df_extracted, use_container_width=True)
                            st.success(f"Paramètres sauvegardés pour {selected_source}.")
                        except ValueError as e:
                            st.error(f"Erreur lors de la création du DataFrame : {e}. Vérifiez les données extraites.")
                        except Exception as e:
                            st.error(f"Erreur inattendue : {e}. Contactez le support technique.")
            else:
                st.error(f"Aucune donnée extraite pour la page {page_to_extract}. Vérifiez le fichier.")