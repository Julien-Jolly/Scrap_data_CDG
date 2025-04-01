# src/extract_ui.py
import streamlit as st
import pandas as pd
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings, update_source_settings
from src.utils import load_excel_data

def extract_section():
    """Affiche la section 'Analyse et Extraction'."""
    st.header("Analyse et Extraction des Données")

    downloaded_files = get_downloaded_files()

    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé.")
        return

    selected_source = st.selectbox("Sélectionner une source", list(downloaded_files.keys()))
    file_path = downloaded_files[selected_source]

    previous_source = st.session_state.get("last_source")
    st.session_state["last_source"] = selected_source

    df = load_excel_data()
    columns = df.columns.tolist()
    source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(df[df[columns[0]] == selected_source]) > 0 else selected_source

    settings = get_source_settings(selected_source)
    default_separator = settings["separator"]
    default_page = settings.get("page", 0)
    default_title_range = settings["title_range"]
    default_data_range = settings["data_range"]

    default_title_start_row = default_title_range[0] if len(default_title_range) > 3 else default_title_range[0]
    default_title_end_row = default_title_range[1] if len(default_title_range) > 3 else default_title_range[0]
    default_title_col_start = default_title_range[2] if len(default_title_range) > 3 else 0
    default_title_col_end = default_title_range[3] if len(default_title_range) > 3 else default_title_range[2]

    if "temp_data_row_start" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_start"] = default_data_range[0]
    if "temp_data_row_end" not in st.session_state or previous_source != selected_source:
        st.session_state["temp_data_row_end"] = default_data_range[1]

    if ("raw_data" not in st.session_state or
            previous_source != selected_source or
            st.session_state.get("last_page") != default_page):
        try:
            raw_data = parse_file(file_path, default_separator, default_page)
            st.session_state["raw_data"] = raw_data
            st.session_state["last_page"] = default_page
        except pd.errors.ParserError as e:
            st.error(f"Erreur de parsing avec le séparateur '{default_separator}': {e}. Modifiez-le ci-dessous.")
            raw_data = []
    else:
        raw_data = st.session_state["raw_data"]

    col1, col2 = st.columns([1, 1])

    with col1:
        st.write("### Contenu brut")
        st.write(f"**Nom de la source :** {source_name}")
        if not raw_data:
            st.warning(f"Aucun contenu extrait. Vérifiez le fichier ou le séparateur.")
        else:
            df_raw = pd.DataFrame(raw_data)
            st.dataframe(df_raw, use_container_width=True, height=400)

    with col2:
        st.write("### Paramétrage de l'extraction")
        separator = st.text_input("Séparateur", value=default_separator)
        page_to_extract = st.number_input("Page à extraire (PDF uniquement)", min_value=0, value=default_page, step=1)

        if page_to_extract != st.session_state.get("last_page"):
            try:
                raw_data = parse_file(file_path, default_separator, page_to_extract)
                st.session_state["raw_data"] = raw_data
                st.session_state["last_page"] = page_to_extract
                st.session_state["last_source"] = selected_source
            except pd.errors.ParserError as e:
                st.error(f"Erreur de parsing avec le séparateur '{default_separator}': {e}. Ajustez-le ci-dessous.")
                raw_data = []

        if st.button("Mettre à jour le contenu"):
            try:
                raw_data = parse_file(file_path, separator, page_to_extract)
                st.session_state["raw_data"] = raw_data
                st.session_state["last_page"] = page_to_extract
                st.session_state["last_source"] = selected_source
                st.success("Contenu mis à jour avec succès.")
                st.rerun()
            except pd.errors.ParserError as e:
                st.error(f"Erreur avec le séparateur '{separator}': {e}. Essayez un autre (ex. ';', '\\t').")

        st.write("#### Plage des titres")
        max_rows = len(raw_data) if raw_data else 1
        max_cols = max(len(row) for row in raw_data) if raw_data else 1

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

        if st.button("Appliquer et Sauvegarder"):
            title_range = [title_row_start, title_row_end, title_col_start, title_col_end]
            data_range = [st.session_state["temp_data_row_start"], st.session_state["temp_data_row_end"]]
            update_source_settings(selected_source, separator, page_to_extract, title_range, data_range)

            if raw_data:
                titles, data = extract_data(raw_data, title_range, data_range)
                if len(titles) == len(data[0]):
                    st.write("### Titres extraits")
                    st.dataframe(pd.DataFrame([titles]), use_container_width=True)
                    st.write("### Données extraites")
                    st.dataframe(pd.DataFrame(data, columns=titles), use_container_width=True)
                    st.success(f"Paramètres sauvegardés pour {selected_source}.")
                else:
                    st.error("Les titres et les données ont des longueurs différentes. Ajustez les plages.")
            else:
                st.error(f"Aucune donnée extraite pour la page {page_to_extract}.")