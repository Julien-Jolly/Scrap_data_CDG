# src/extract_ui.py
import streamlit as st
import pandas as pd
import os
import re
from datetime import datetime
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings, update_source_settings, \
    load_settings
from src.utils import load_excel_data, make_unique_titles
from src.config import get_download_dir
import json
import logging

# Configurer le logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s', force=True)
logger = logging.getLogger(__name__)


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
        logger.warning("Aucune source trouvée dans matrice_sources.xlsx")
        return

    # Sélectionner une source
    with st.container():
        st.markdown("#### Sélection de la source et de la date")
        selected_source = st.selectbox("Sélectionner une source", all_sources)
        logger.debug(f"Source sélectionnée : {selected_source}")

        # Récupérer le nom de la source (colonne 7)
        source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(
            df[df[columns[0]] == selected_source]) > 0 else selected_source

        # Sélecteur de date
        st.write(f"**Nom de la source :** {source_name}")
        default_date = datetime.now().strftime("%m-%d")
        downloads_base = os.path.join(os.path.dirname(__file__), "..", "Downloads")
        available_dates = sorted([
            d for d in os.listdir(downloads_base)
            if os.path.isdir(os.path.join(downloads_base, d)) and re.match(r"\d{2}-\d{2}", d)
        ])
        if not available_dates:
            st.error("Aucun dossier de téléchargement trouvé dans Downloads.")
            logger.error("Aucun dossier de téléchargement trouvé dans Downloads")
            return

        selected_date = st.selectbox(
            "Date de l'extraction",
            available_dates,
            index=available_dates.index(default_date) if default_date in available_dates else 0,
            key="date_selectbox"
        )
        logger.debug(f"Date sélectionnée dans l'interface : {selected_date}")

    # Valider le format de selected_date
    if not selected_date or not re.match(r"\d{2}-\d{2}", selected_date):
        st.error(f"Format de date invalide : {selected_date}. Attendu : MM-DD (ex. 05-07).")
        logger.error(f"Format de date invalide : {selected_date}")
        return

    # Forcer la réinitialisation du cache si la date ou la source change
    cache_key = f"{selected_source}_{selected_date}"
    logger.debug(f"Clé de cache calculée : {cache_key}")
    if (st.session_state.get("last_selected_date") != selected_date or
            st.session_state.get("last_source") != selected_source or
            st.session_state.get("last_cache_key") != cache_key):
        logger.debug(
            f"Réinitialisation du cache : date ({st.session_state.get('last_selected_date')} -> {selected_date}), "
            f"source ({st.session_state.get('last_source')} -> {selected_source}), "
            f"cache_key ({st.session_state.get('last_cache_key')} -> {cache_key})")
        st.session_state["raw_tables"] = {}
        st.session_state["last_cache_key"] = cache_key
        st.session_state["last_selected_date"] = selected_date
        st.session_state["last_source"] = selected_source

    # Charger les fichiers pour la date sélectionnée
    download_dir = get_download_dir(selected_date)
    logger.debug(f"Répertoire de téléchargement calculé : {download_dir}")
    if not os.path.exists(download_dir):
        st.warning(f"Aucun fichier trouvé pour la date {selected_date}. Le répertoire {download_dir} n'existe pas.")
        logger.warning(f"Le répertoire {download_dir} n'existe pas")
        return

    logger.debug(f"Contenu du répertoire {download_dir} : {os.listdir(download_dir)}")
    downloaded_files = get_downloaded_files(download_dir)
    logger.debug(
        f"Fichiers téléchargés pour source '{selected_source}' à la date {selected_date} : {downloaded_files.get(selected_source, [])}")

    # Vérifier si la source sélectionnée a des fichiers pour la date
    if selected_source not in downloaded_files or not downloaded_files[selected_source]:
        st.warning(f"Aucun fichier trouvé pour la source {source_name} à la date {selected_date}.")
        logger.warning(f"Aucun fichier pour {selected_source} à {selected_date}")
        return

    file_paths = downloaded_files[selected_source]
    logger.debug(f"Chemins des fichiers pour '{selected_source}' ({len(file_paths)} fichiers) : {file_paths}")

    # Charger les paramètres de la source
    settings = get_source_settings(selected_source)
    table_settings = settings.get("tables", {})
    default_selected_table = list(table_settings.keys())[0] if table_settings else None
    default_combinations = table_settings.get(default_selected_table, {}).get("combinations", [
        {
            "ignore_titles": False,
            "title_row": 0,
            "title_col": 0,
            "data_col": 0,
            "data_row_start": 1,
            "data_row_end": 10
        }
    ])

    # Charger les tableaux
    if "raw_tables" not in st.session_state or not st.session_state["raw_tables"]:
        logger.debug(f"Chargement des tableaux pour {selected_source} à la date {selected_date}")
        raw_tables = {}
        for file_path in file_paths:
            try:
                table_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                if table_data:
                    table_name = os.path.basename(file_path)
                    raw_tables[table_name] = table_data
                    logger.debug(f"Tableau chargé : {table_name} avec {len(table_data)} lignes")
                else:
                    logger.debug(f"Aucune donnée pour {file_path}")
            except Exception as e:
                st.error(f"Erreur de parsing pour {file_path}: {e}")
                logger.error(f"Erreur de parsing pour {file_path}: {e}")
        st.session_state["raw_tables"] = raw_tables
    else:
        raw_tables = st.session_state["raw_tables"]
        logger.debug(
            f"Utilisation des tableaux depuis le cache pour {selected_source} à la date {selected_date}: {list(raw_tables.keys())}")

    # Contenu brut
    st.divider()
    with st.container():
        st.markdown("### Contenu brut")
        if not raw_tables:
            st.warning(
                f"Aucun contenu extrait pour {selected_source} à la date {selected_date}. Vérifiez les fichiers.")
            logger.debug("Aucun tableau disponible dans raw_tables")
            raw_data = []
        else:
            table_options = list(raw_tables.keys())
            default_index = table_options.index(
                default_selected_table) if default_selected_table in table_options else 0
            selected_table = st.selectbox(
                "Sélectionner un tableau",
                table_options,
                index=default_index,
                key="table_selectbox"
            )
            raw_data = raw_tables.get(selected_table, [])
            logger.debug(f"Tableau sélectionné : {selected_table}, Données : {raw_data[:2] if raw_data else 'Vide'}")
            if not raw_data:
                st.warning(f"Aucun contenu pour {selected_table} à la date {selected_date}.")
            elif len(raw_data) <= 1:
                st.warning(f"Seuls les en-têtes sont extraits pour {selected_table} à la date {selected_date}.")
            else:
                df_raw = pd.DataFrame(raw_data)
                st.dataframe(df_raw, use_container_width=True, height=400)

    # Paramétrage des combinaisons
    st.divider()
    with st.container():
        st.markdown("### Paramétrage des combinaisons")
        st.info(
            "Configurez les combinaisons de titres et de données pour extraire des portions spécifiques du tableau. Chaque combinaison peut être dépliée/repliée pour plus de clarté.")
        raw_data_defined = bool(raw_data and len(raw_data) > 0)
        max_rows = len(raw_data) if raw_data_defined else 1
        max_cols = max(len(row) for row in raw_data) if raw_data_defined else 1

        # Charger les combinaisons spécifiques au tableau sélectionné
        table_combinations = table_settings.get(selected_table, {}).get("combinations", [
            {
                "ignore_titles": False,
                "title_row": 0,
                "title_col": 0,
                "data_col": 0,
                "data_row_start": 1,
                "data_row_end": 10
            }
        ])

        num_combinations = st.number_input(
            "Nombre de combinaisons Titre/Données",
            min_value=1,
            max_value=10,
            value=len(table_combinations),
            step=1,
            key="num_combinations",
            help="Définissez le nombre de combinaisons à configurer pour ce tableau."
        )

        # Stocker les paramètres des combinaisons
        combinations = []
        for i in range(num_combinations):
            with st.expander(f"Combinaison {i + 1}", expanded=(i == 0)):
                default_comb = table_combinations[i] if i < len(table_combinations) else table_combinations[0]

                # Section Titre
                with st.container(border=True):
                    st.markdown("**Titre**")
                    st.markdown(
                        "Sélectionnez la colonne et la ligne du titre, ou ignorez pour utiliser des titres par défaut.")
                    ignore_titles = st.checkbox(
                        "Ignorer les titres (utiliser Titre 1, Titre 2, ...)",
                        value=default_comb.get("ignore_titles", False),
                        key=f"ignore_titles_{i}"
                    )

                    if not ignore_titles:
                        title_col = st.number_input(
                            "Colonne du titre",
                            min_value=0,
                            max_value=max_cols - 1,
                            value=min(default_comb.get("title_col", 0), max_cols - 1),
                            key=f"title_col_{i}",
                            help="Colonne où se trouve le titre (commence à 0)."
                        )
                        title_row = st.number_input(
                            "Ligne du titre",
                            min_value=0,
                            max_value=max_rows - 1,
                            value=min(default_comb.get("title_row", 0), max_rows - 1),
                            key=f"title_row_{i}",
                            help="Ligne où se trouve le titre (commence à 0)."
                        )
                    else:
                        title_row, title_col = 0, 0
                        st.info(
                            "Les titres seront générés automatiquement (Titre 1, Titre 2, ...) en fonction du nombre de colonnes des données.")

                # Section Plage des données
                with st.container(border=True):
                    st.markdown("**Plage des données**")
                    st.markdown("Définissez la colonne et les lignes de début/fin pour les données à extraire.")
                    data_col = st.number_input(
                        "Colonne des données",
                        min_value=0,
                        max_value=max_cols - 1,
                        value=min(default_comb.get("data_col", 0), max_cols - 1),
                        key=f"data_col_{i}",
                        help="Colonne contenant les données (commence à 0)."
                    )
                    data_row_start = st.number_input(
                        "Ligne début données",
                        min_value=0,
                        max_value=max_rows - 1,
                        value=min(default_comb.get("data_row_start", 1), max_rows - 1),
                        key=f"data_row_start_{i}",
                        help="Ligne où les données commencent (commence à 0)."
                    )
                    data_row_end = st.number_input(
                        "Ligne fin données",
                        min_value=data_row_start,
                        max_value=max_rows - 1,
                        value=max(data_row_start, min(default_comb.get("data_row_end", 10), max_rows - 1)),
                        key=f"data_row_end_{i}",
                        help="Ligne où les données se terminent (inclus, commence à 0)."
                    )

                combinations.append({
                    "ignore_titles": ignore_titles,
                    "title_row": title_row,
                    "title_col": title_col,
                    "data_col": data_col,
                    "data_row_start": data_row_start,
                    "data_row_end": data_row_end
                })

    # Bouton Appliquer et Sauvegarder
    st.divider()
    with st.container():
        if st.button("Appliquer et Sauvegarder"):
            # Sauvegarder les paramètres pour le tableau sélectionné
            settings = {
                "tables": {
                    selected_table: {
                        "combinations": combinations
                    }
                }
            }
            update_source_settings(selected_source, settings)

            if raw_data:
                extracted_dfs = []
                for i, comb in enumerate(combinations):
                    ignore_titles = comb["ignore_titles"]
                    title_row = comb["title_row"]
                    title_col = comb["title_col"]
                    data_col = comb["data_col"]
                    data_row_start = comb["data_row_start"]
                    data_row_end = comb["data_row_end"]

                    # Extraire le titre
                    if not ignore_titles:
                        try:
                            titles = [raw_data[title_row][title_col]]
                        except IndexError:
                            st.error(
                                f"Combinaison {i + 1}: Titre non trouvé à la ligne {title_row}, colonne {title_col}.")
                            continue
                    else:
                        titles = [f"Titre {data_col + 1}"]

                    # Extraire les données
                    try:
                        data = [
                            [raw_data[row][data_col]]
                            for row in range(data_row_start, min(data_row_end + 1, max_rows))
                            if row < len(raw_data) and data_col < len(raw_data[row])
                        ]
                    except IndexError:
                        st.error(f"Combinaison {i + 1}: Données non trouvées pour la plage spécifiée.")
                        continue

                    if not data:
                        st.error(f"Combinaison {i + 1}: Aucune donnée extraite. Vérifiez la plage des données.")
                        continue

                    # Vérifier la correspondance titres/données
                    data_col_count = 1  # Une seule colonne extraite
                    if len(titles) != data_col_count:
                        st.error(
                            f"Combinaison {i + 1}: Les titres ({len(titles)}) ne correspondent pas aux données ({data_col_count} colonnes)."
                        )
                        continue

                    unique_titles = make_unique_titles(titles)
                    try:
                        df_extracted = pd.DataFrame(data, columns=unique_titles)
                        extracted_dfs.append(df_extracted)
                        st.markdown(f"### Données extraites - Combinaison {i + 1}")
                        st.dataframe(df_extracted, use_container_width=True)
                    except ValueError as e:
                        st.error(f"Combinaison {i + 1}: Erreur lors de la création du DataFrame : {e}.")
                        continue

                if extracted_dfs:
                    # Combiner les DataFrames si nécessaire
                    combined_df = pd.concat(extracted_dfs, axis=1)
                    st.markdown("### Données combinées")
                    st.dataframe(combined_df, use_container_width=True)
                    st.session_state["extracted_data"] = combined_df
                    st.success(f"Paramètres sauvegardés pour {selected_source} - {selected_table}.")

                    # Afficher le JSON
                    all_settings = load_settings()
                    settings_json = json.dumps(all_settings.get(selected_source, {}), indent=4)
                    st.markdown("### Paramètres sauvegardés (JSON)")
                    st.code(settings_json, language="json")
                else:
                    st.error("Aucune donnée extraite pour aucune combinaison.")
            else:
                st.error(f"Aucune donnée disponible pour {selected_table}.")

    # Tableau des sources non paramétrées
    st.divider()
    with st.container():
        st.markdown("### Sources non paramétrées")
        settings = load_settings()
        parametrized_sources = list(settings.keys())
        non_parametrized_sources = [source for source in all_sources if source not in parametrized_sources]

        if not non_parametrized_sources:
            st.info("Toutes les sources sont paramétrées.")
        else:
            non_parametrized_data = df[df[columns[0]].isin(non_parametrized_sources)][
                [columns[0], columns[1], columns[5]]]
            non_parametrized_data.columns = ["Source", "Type d'extraction", "Commentaires"]
            st.dataframe(non_parametrized_data, use_container_width=True)