# src/list_sources_ui.py
import streamlit as st
import pandas as pd
import os
import re
from datetime import datetime
from src.parser import get_downloaded_files, parse_file, get_source_settings, extract_data_from_combinations
from src.utils import load_excel_data, save_csv, generate_create_table_query, save_create_table_query
from src.config import get_download_dir, configure_logging

# Configurer le logging
logger, summary_logger = configure_logging("list_sources_ui")

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
    settings_data = get_source_settings(None)
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
        global_data = []
        unique_titles = set()

        # Lancer la génération
        with st.spinner("Génération en cours..."):
            if selected_source == "Toutes les sources":
                sources_to_process = sources
            else:
                sources_to_process = [selected_source]

            for source in sources_to_process:
                source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(
                    df_excel[df_excel[columns[0]] == source]) > 0 else source
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
                        file_path = next((fp for fp in file_paths if os.path.basename(fp) == table_name), None)
                        if not file_path:
                            anomalies_data.append({
                                "Source": source_name,
                                "Anomalie": f"Fichier pour le tableau {table_name} non trouvé."
                            })
                            continue

                        # Parser le fichier
                        raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                        if not raw_data:
                            raise Exception(f"Aucune donnée extraite pour {table_name}.")

                        # Extraire les données avec extract_data_from_combinations
                        table_data, table_titles = extract_data_from_combinations(
                            raw_data, table_settings.get("combinations", []), source_name, table_name
                        )
                        global_data.extend(table_data)
                        unique_titles.update(table_titles)

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

                # Afficher le DataFrame
                st.subheader("DataFrame global")
                st.dataframe(df_global, use_container_width=True)

                # Générer et sauvegarder le CSV
                try:
                    csv_file = save_csv(df_global, selected_date)
                    st.success(f"CSV généré : {csv_file}")
                    with open(csv_file, "rb") as f:
                        st.download_button("Télécharger le CSV", f, file_name=os.path.basename(csv_file))
                except Exception as e:
                    st.error(f"Erreur lors de la génération du CSV : {str(e)}")
                    logger.error(f"Erreur lors de la génération du CSV : {str(e)}")

                # Générer et sauvegarder la requête CREATE TABLE
                try:
                    create_table_query = generate_create_table_query(unique_titles)
                    sql_file = save_create_table_query(create_table_query, selected_date)
                    st.success(f"Script SQL généré : {sql_file}")
                    with open(sql_file, "r") as f:
                        st.download_button("Télécharger le script SQL", f, file_name=os.path.basename(sql_file))
                except Exception as e:
                    st.error(f"Erreur lors de la génération du script SQL : {str(e)}")
                    logger.error(f"Erreur lors de la génération du script SQL : {str(e)}")
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
        st.subheader(f"DataFrame pour {selected_source}")
        source_name = df_excel[df_excel[columns[0]] == selected_source][columns[7]].iloc[0] if len(
            df_excel[df_excel[columns[0]] == selected_source]) > 0 else selected_source
        try:
            source_settings = settings_data.get(selected_source, {}).get("tables", {})
            if not source_settings:
                st.warning(f"Aucun paramètre configuré pour {source_name}.")
                return

            file_paths = downloaded_files.get(selected_source, [])
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

                raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                if not raw_data:
                    st.error(f"Aucune donnée extraite pour {table_name}.")
                    continue

                table_data, table_titles = extract_data_from_combinations(
                    raw_data, table_settings.get("combinations", []), source_name, table_name
                )
                local_data.extend(table_data)
                local_titles.update(table_titles)

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
            logger.error(f"Erreur lors de l'extraction pour {source_name} : {str(e)}")

if __name__ == "__main__":
    list_sources_section()