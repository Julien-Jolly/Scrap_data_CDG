# src/extract_ui.py
import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
from src.parser import get_downloaded_files, parse_file
from src.utils import load_excel_data
from src.config import get_download_dir
import logging

# Configurer le logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# Chemin du fichier JSON pour sauvegarder les sélections
SELECTIONS_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "selections.json")

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

def save_selections(selections_data):
    """Sauvegarde les sélections dans le fichier JSON."""
    try:
        with open(SELECTIONS_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(selections_data, f, indent=2)
        logger.debug(f"Sélections sauvegardées dans {SELECTIONS_JSON_PATH}")
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde de {SELECTIONS_JSON_PATH}: {e}")
        st.error(f"Erreur lors de la sauvegarde des sélections : {e}")

def extract_section():
    """Affiche la section 'Analyse et Extraction des Données' avec sélection de date, tableau des sources et paramétrage multiple."""
    st.header("Analyse et Extraction des Données", anchor=False)

    # Charger les données Excel
    df = load_excel_data()
    columns = df.columns.tolist()

    # Vérifier si la colonne 10 existe
    if len(columns) < 11:  # Colonne 10 correspond à l'index 10 (0-based)
        st.error("Le fichier Excel ne contient pas la colonne 10. Vérifiez sa structure.")
        return

    # Récupérer toutes les sources depuis le fichier Excel
    all_sources = df[columns[0]].unique().tolist()

    if not all_sources:
        st.warning("Aucune source trouvée dans le fichier Excel.")
        return

    # Section Source
    with st.container():
        st.subheader("Sélection de la Source", anchor=False)
        selected_source = st.selectbox(
            "Sélectionner une source",
            all_sources,
            key="source_select",
            help="Choisissez une source dans la liste extraite du fichier Excel."
        )

        # Récupérer le nom de la source (colonne 7) et la colonne 10
        source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(
            df[df[columns[0]] == selected_source]) > 0 else selected_source
        column_10_value = df[df[columns[0]] == selected_source][columns[10]].iloc[0] if len(
            df[df[columns[0]] == selected_source]) > 0 else "Non défini"

        # Afficher le nom de la source et la colonne 10 avec style
        st.markdown(f"**Nom de la source** : {source_name}")
        st.markdown(f"**Information** : {column_10_value}")

    # Sélecteur de date
    with st.container():
        st.subheader("Date de l'Extraction", anchor=False)
        default_date = datetime.now().strftime("%m-%d")
        available_dates = [d for d in os.listdir(os.path.join(os.path.dirname(__file__), "..", "Downloads"))
                           if os.path.isdir(os.path.join(os.path.dirname(__file__), "..", "Downloads", d))]
        selected_date = st.selectbox(
            "Date de l'extraction",
            available_dates,
            index=available_dates.index(default_date) if default_date in available_dates else 0,
            key="date_select",
            help="Sélectionnez la date correspondant au dossier de téléchargement."
        )

    # Charger les fichiers pour la date sélectionnée
    download_dir = get_download_dir(selected_date)
    downloaded_files = get_downloaded_files(download_dir)

    # Vérifier si la source sélectionnée a des fichiers pour la date
    if selected_source not in downloaded_files:
        st.warning(f"Aucun fichier trouvé pour la source {source_name} à la date {selected_date}.")
        return

    file_paths = downloaded_files[selected_source]

    # Charger les tableaux
    cache_key = f"{selected_source}_{selected_date}"
    if ("raw_tables" not in st.session_state or
            st.session_state.get("last_cache_key") != cache_key):
        raw_tables = {}
        for file_path in file_paths:
            try:
                table_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                if table_data:
                    table_name = os.path.basename(file_path)
                    raw_tables[table_name] = table_data
            except Exception as e:
                st.error(f"Erreur de parsing pour {file_path}: {e}")
        st.session_state["raw_tables"] = raw_tables
        st.session_state["last_cache_key"] = cache_key
    else:
        raw_tables = st.session_state["raw_tables"]

    # Afficher le contenu brut sur toute la largeur
    with st.container():
        st.subheader("Contenu Brut", anchor=False)
        if not raw_tables:
            st.warning(f"Aucun contenu extrait. Vérifiez les fichiers.")
            raw_data = []  # Initialiser raw_data pour éviter UnboundLocalError
        else:
            selected_table = st.selectbox(
                "Sélectionner un tableau",
                list(raw_tables.keys()),
                key="table_select",
                help="Choisissez un fichier parmi les fichiers téléchargés pour cette source."
            )
            raw_data = raw_tables[selected_table]
            if not raw_data:
                st.warning(f"Aucun contenu pour {selected_table}.")
            elif len(raw_data) <= 1:
                st.warning(f"Seuls les en-têtes sont extraits pour {selected_table}.")
            else:
                df_raw = pd.DataFrame(raw_data)
                st.dataframe(df_raw, use_container_width=True, height=400)

    # Paramétrage sous le contenu brut
    with st.container():
        st.subheader("Paramétrage de l'Extraction", anchor=False)
        raw_data_defined = 'raw_data' in locals() and raw_data is not None and len(raw_data) > 0
        max_rows = len(raw_data) if raw_data_defined else 1
        max_cols = max(len(row) for row in raw_data) if raw_data_defined else 1

        # Charger les sélections existantes pour la source (indépendant de la date)
        selections_data = load_selections()
        saved_selections = selections_data.get(selected_source, [])

        # Détecter un changement de source
        if "last_source" not in st.session_state or st.session_state.last_source != selected_source:
            # Réinitialiser num_combinations et selections pour la nouvelle source
            st.session_state.num_combinations = len(saved_selections) if saved_selections else 1
            st.session_state.selections = saved_selections if saved_selections else []
            st.session_state.last_source = selected_source

        # Compteur pour le nombre de combinaisons titre + plage de données
        num_combinations = st.number_input(
            "Nombre de combinaisons titre + plage de données",
            min_value=1,
            value=st.session_state.num_combinations,
            step=1,
            key="num_combinations",
            help="Indiquez combien de champs (titre + données) vous souhaitez extraire."
        )

        # Formulaire pour chaque combinaison
        selections = []
        for i in range(num_combinations):
            st.markdown(f"**Combinaison {i + 1}**")
            with st.expander(f"Configuration de la Combinaison {i + 1}", expanded=True):
                # Charger les valeurs par défaut depuis les sélections sauvegardées
                default_values = next((s for s in saved_selections if s["combination"] == i + 1), {})

                # Sélection du titre
                st.markdown("**Titre**")
                cols_title = st.columns([1, 1])
                with cols_title[0]:
                    title_row = st.number_input(
                        "Ligne du titre",
                        min_value=0,
                        max_value=max_rows - 1,
                        value=default_values.get("title_row", 0),
                        key=f"title_row_{i}",
                        help="Indiquez la ligne où se trouve le titre."
                    )
                with cols_title[1]:
                    title_col = st.number_input(
                        "Colonne du titre",
                        min_value=0,
                        max_value=max_cols - 1,
                        value=default_values.get("title_col", 0),
                        key=f"title_col_{i}",
                        help="Indiquez la colonne où se trouve le titre."
                    )

                # Tags Date et Time
                st.markdown("**Type de champ**")
                cols_tags = st.columns([1, 1])
                with cols_tags[0]:
                    is_date = st.checkbox(
                        "Date",
                        value=default_values.get("is_date", False),
                        key=f"is_date_{i}",
                        help="Cochez si ce champ représente une date."
                    )
                with cols_tags[1]:
                    is_time = st.checkbox(
                        "Time",
                        value=default_values.get("is_time", False),
                        key=f"is_time_{i}",
                        help="Cochez si ce champ représente une heure."
                    )

                # Sélection de la plage de données
                st.markdown("**Plage de données**")
                data_col = st.number_input(
                    "Colonne des données",
                    min_value=0,
                    max_value=max_cols - 1,
                    value=default_values.get("data_col", 0),
                    key=f"data_col_{i}",
                    help="Indiquez la colonne où se trouvent les données."
                )
                cols_data = st.columns([1, 1])
                with cols_data[0]:
                    data_row_start = st.number_input(
                        "Ligne début données",
                        min_value=0,
                        max_value=max_rows - 1,
                        value=default_values.get("data_row_start", 1),
                        key=f"data_row_start_{i}",
                        help="Indiquez la première ligne des données."
                    )
                with cols_data[1]:
                    data_row_end = st.number_input(
                        "Ligne fin données",
                        min_value=data_row_start,
                        max_value=max_rows - 1,
                        value=default_values.get("data_row_end", min(data_row_start + 9, max_rows - 1)),
                        key=f"data_row_end_{i}",
                        help="Indiquez la dernière ligne des données."
                    )

                # Stocker la sélection
                selections.append({
                    "combination": i + 1,
                    "title_row": title_row,
                    "title_col": title_col,
                    "is_date": is_date,
                    "is_time": is_time,
                    "data_col": data_col,
                    "data_row_start": data_row_start,
                    "data_row_end": data_row_end
                })

        # Bouton pour sauvegarder les sélections
        if st.button("Sauvegarder les sélections", key="save_selections"):
            # Mettre à jour les sélections dans selections_data (indépendant de la date)
            selections_data[selected_source] = selections
            # Sauvegarder dans le fichier JSON
            save_selections(selections_data)
            # Mettre à jour st.session_state.selections
            st.session_state.selections = selections
            st.success("Sélections sauvegardées avec succès.")

    # Afficher les données extraites
    with st.container():
        st.subheader("Données Extraites", anchor=False)
        if st.session_state.selections:
            extracted_data = []
            for sel in st.session_state.selections:
                try:
                    # Extraire la valeur du titre
                    title_value = raw_data[sel["title_row"]][sel["title_col"]] if raw_data_defined else "Non disponible"
                    # Déterminer le titre à afficher
                    if sel["is_date"] and sel["is_time"]:
                        title_display = "DateTime"
                    elif sel["is_date"]:
                        title_display = "Date"
                    elif sel["is_time"]:
                        title_display = "Time"
                    else:
                        title_display = title_value

                    # Extraire les données
                    data_values = []
                    for row_idx in range(sel["data_row_start"], sel["data_row_end"] + 1):
                        try:
                            value = raw_data[row_idx][sel["data_col"]] if raw_data_defined else "Non disponible"
                            data_values.append(value)
                        except IndexError:
                            data_values.append("Erreur : indice hors limites")

                    extracted_data.append({
                        "Titre": title_display,
                        "Données": data_values
                    })
                except IndexError:
                    extracted_data.append({
                        "Titre": "Erreur : indice hors limites",
                        "Données": ["Erreur : impossible d'extraire les données"]
                    })

            # Afficher les données extraites
            for data in extracted_data:
                st.markdown(f"**{data['Titre']}**")
                if data["Données"]:
                    df_data = pd.DataFrame(data["Données"], columns=[data["Titre"]])
                    st.dataframe(df_data, use_container_width=True, height=200)
                else:
                    st.warning("Aucune donnée extraite pour ce champ.")
        else:
            st.info("Aucune donnée extraite. Sauvegardez les sélections pour voir les résultats.")

if __name__ == "__main__":
    extract_section()