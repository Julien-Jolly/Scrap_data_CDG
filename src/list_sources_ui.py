# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings
from src.utils import load_excel_data, make_unique_titles


def list_sources_section():
    """Affiche la section 'Liste des Sources et DataFrames'."""
    st.header("Liste des Sources et DataFrames")

    # Charger les sources disponibles
    downloaded_files = get_downloaded_files()

    if not downloaded_files:
        st.warning("Aucun fichier téléchargé trouvé.")
        return

    # Charger les informations des sources depuis le fichier Excel
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    # Liste des sources
    sources = list(downloaded_files.keys())

    if not sources:
        st.warning("Aucune source configurée trouvée.")
        return

    st.write(f"**Nombre total de sources :** {len(sources)}")

    # Parcourir chaque source
    for source in sources:
        st.subheader(f"Source : {source}")

        # Récupérer le nom de la source depuis le fichier Excel
        source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(
            df_excel[df_excel[columns[0]] == source]) > 0 else source
        st.write(f"**Nom complet :** {source_name}")

        # Charger les paramètres de la source
        settings = get_source_settings(source)
        separator = settings.get("separator", ";")
        page = settings.get("page", 0)
        title_range = settings.get("title_range", [0, 0, 0, 5])
        data_range = settings.get("data_range", [1, 10])
        selected_table = settings.get("selected_table", None)

        # Vérifier si un tableau est sélectionné
        if not selected_table:
            st.warning(
                f"Aucun tableau sélectionné pour {source}. Configurez les paramètres dans 'Analyse et Extraction'.")
            continue

        # Trouver le fichier correspondant au tableau sélectionné
        file_paths = downloaded_files[source]
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            st.error(f"Le fichier {selected_table} n'a pas été trouvé pour la source {source}.")
            continue

        # Parser le fichier avec les paramètres sauvegardés
        try:
            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                st.error(f"Aucune donnée extraite pour {selected_table}. Vérifiez le fichier ou les paramètres.")
                continue

            # Extraire les titres et données selon les plages définies
            titles, data = extract_data(raw_data, title_range, data_range)

            if not titles or not data:
                st.warning(
                    f"Aucune donnée ou titre extrait pour {selected_table}. Ajustez les plages dans 'Analyse et Extraction'.")
                continue

            # Vérifier la compatibilité entre titres et données
            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                st.error(
                    f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes). Ajustez les plages dans 'Analyse et Extraction'.")
                continue

            # Rendre les titres uniques
            unique_titles = make_unique_titles(titles)

            # Créer et afficher le DataFrame
            try:
                df_extracted = pd.DataFrame(data, columns=unique_titles)
                st.write("### DataFrame paramétré")
                st.dataframe(df_extracted, use_container_width=True, height=200)
                st.success(f"DataFrame généré avec succès pour {source}.")
            except ValueError as e:
                st.error(
                    f"Erreur lors de la création du DataFrame pour {source} : {e}. Vérifiez les données extraites.")
            except Exception as e:
                st.error(f"Erreur inattendue pour {source} : {e}. Contactez le support technique.")

        except Exception as e:
            st.error(f"Erreur lors de l'extraction des données pour {file_path} : {e}")