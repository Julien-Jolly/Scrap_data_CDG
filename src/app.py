# src/app.py
import streamlit as st
import pandas as pd
import concurrent.futures
import queue
import time
from src.downloader import get_sources, download_files
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings, update_source_settings

# Chemin du fichier Excel (identique à celui dans downloader.py)
SOURCE_FILE = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"

# Fonction pour exécuter une fonction synchrone dans un thread séparé
def run_in_thread(func, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(func, *args, **kwargs)
        return future.result()

# Fonction pour charger les données du fichier Excel
def load_excel_data():
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
    return df

# Fonction pour sauvegarder les modifications dans le fichier Excel
def save_to_excel(df):
    with pd.ExcelWriter(SOURCE_FILE, mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name="Source sans doub", index=False)

def main():
    # Configuration de la page
    st.set_page_config(page_title="Téléchargement de Fichiers", layout="wide")

    # Sidebar
    st.sidebar.title("Menu")
    option = st.sidebar.selectbox("Choisir une action", [
        "Téléchargement des fichiers",
        "Gestion des Sources",
        "Analyse et Extraction"
    ])

    # Corps de la page
    st.title("Téléchargement de Fichiers")

    if option == "Téléchargement des fichiers":
        st.header("Liste des sources à télécharger")

        # Récupérer la liste des sources
        sources = get_sources()
        if not sources:
            st.warning("Aucune source trouvée dans le fichier.")
            return

        # Créer deux colonnes pour séparer le tableau des statuts (gauche) et le rapport (droite)
        col1, col2 = st.columns([1, 1])

        # Colonne 1 (gauche) : Tableau des sources avec statuts dynamiques
        with col1:
            st.write("### Statut des téléchargements :")
            # Initialiser un DataFrame pour les statuts
            status_df = pd.DataFrame({
                "Source": sources,
                "Statut": ["⏳ En attente" for _ in sources]
            })
            # Créer un conteneur pour le tableau des statuts
            status_container = st.empty()
            # Afficher le tableau initial
            status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)

        # Colonne 2 (droite) : Espace pour le rapport final
        with col2:
            st.write("### Rapport de Téléchargement :")
            # Créer un conteneur pour le rapport final
            report_container = st.empty()
            # Initialement, afficher un message d'attente
            report_container.write("### Rapport final\nEn attente de la fin des téléchargements...")

            # Bouton pour lancer le téléchargement
            if st.button("Lancer le téléchargement"):
                with st.spinner("Téléchargement en cours..."):
                    # Créer une file d'attente pour les mises à jour de statut
                    status_queue = queue.Queue()

                    # Lancer le téléchargement dans un thread séparé
                    def run_download():
                        return download_files(status_queue)

                    # Démarrer le thread de téléchargement
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(run_download)

                        # Consommer les mises à jour de statut depuis la file d'attente
                        while True:
                            try:
                                # Attendre une mise à jour (timeout pour éviter de bloquer indéfiniment)
                                update = status_queue.get(timeout=0.1)
                                if update[0] == "DONE":
                                    break
                                source, status = update
                                status_df.loc[status_df["Source"] == source, "Statut"] = status
                                status_container.dataframe(status_df, hide_index=True, use_container_width=True,
                                                           height=300)
                            except queue.Empty:
                                # Si la file est vide, vérifier si le thread est terminé
                                if future.done():
                                    break
                                time.sleep(0.1)  # Attendre un peu avant de réessayer

                        # Récupérer le résultat final
                        successes, total, errors = future.result()

                    # Afficher le rapport final à droite
                    report_container.empty()  # Vider le conteneur
                    with report_container:
                        st.write("### Rapport final")
                        st.write(f"**Fichiers téléchargés avec succès : {successes}/{total}**")
                        if errors:
                            st.write("**Liste des fichiers en erreur :**")
                            # Créer un DataFrame pour les erreurs
                            errors_df = pd.DataFrame(errors, columns=["Source", "Motif"])
                            st.dataframe(errors_df, hide_index=True, use_container_width=True)
                        else:
                            st.success("Tous les fichiers ont été téléchargés avec succès !")

    elif option == "Gestion des Sources":
        st.header("Gestion des Sources")
        st.write("Modifiez les informations ci-dessous pour adapter la logique d'extraction des fichiers.")

        # Charger les données du fichier Excel
        df = load_excel_data()
        columns = df.columns.tolist()

        # Créer un DataFrame avec les colonnes demandées
        edit_df = df[[columns[0], columns[1], columns[2], columns[3]]].copy()
        edit_df.columns = ["Source", "Type d'extraction", "URL", "XPath reformaté"]

        # Rendre la colonne "Source" non éditable
        disabled_columns = {"Source": st.column_config.Column(disabled=True)}

        # Afficher le tableau éditable
        edited_df = st.data_editor(
            edit_df,
            hide_index=True,
            column_config=disabled_columns,
            use_container_width=True,
            height=400,
            num_rows="fixed"  # Empêche l'ajout ou la suppression de lignes
        )

        # Bouton pour sauvegarder les modifications
        if st.button("Sauvegarder les modifications"):
            # Mettre à jour le DataFrame original avec les modifications
            df[columns[1]] = edited_df["Type d'extraction"]
            df[columns[2]] = edited_df["URL"]
            df[columns[3]] = edited_df["XPath reformaté"]

            # Sauvegarder dans le fichier Excel
            save_to_excel(df)
            st.success("Les modifications ont été sauvegardées avec succès dans le fichier Excel !")






    elif option == "Analyse et Extraction":

        st.header("Analyse et Extraction des Données")

        downloaded_files = get_downloaded_files()

        if not downloaded_files:
            st.warning("Aucun fichier téléchargé trouvé.")

            return

        selected_source = st.selectbox("Sélectionner une source", list(downloaded_files.keys()))

        file_path = downloaded_files[selected_source]

        settings = get_source_settings(selected_source)

        default_separator = settings["separator"]

        default_page = settings.get("page", 0)

        default_title_range = settings["title_range"]

        default_data_range = settings["data_range"]

        # Rafraîchir si la source ou la page change

        if ("raw_data" not in st.session_state or

                st.session_state.get("last_source") != selected_source or

                st.session_state.get("last_page") != default_page):

            raw_data = parse_file(file_path, default_separator, default_page)

            st.session_state["raw_data"] = raw_data

            st.session_state["last_source"] = selected_source

            st.session_state["last_page"] = default_page

        else:

            raw_data = st.session_state["raw_data"]

        col1, col2 = st.columns([1, 1])

        with col1:

            st.write("### Contenu brut")

            page_to_extract = st.number_input("Page à extraire (PDF uniquement)", min_value=0, value=default_page,
                                              step=1)

            if page_to_extract != st.session_state.get("last_page"):
                raw_data = parse_file(file_path, default_separator, page_to_extract)

                st.session_state["raw_data"] = raw_data

                st.session_state["last_page"] = page_to_extract

                st.session_state["last_source"] = selected_source

            if not raw_data:

                st.warning(f"Aucun contenu extrait pour la page {page_to_extract}. Essayez une autre page.")

            else:

                df_raw = pd.DataFrame(raw_data)

                st.dataframe(df_raw, use_container_width=True, height=400)

        with col2:

            st.write("### Paramétrage de l'extraction")

            separator = st.text_input("Séparateur", value=default_separator)

            if st.button("Mettre à jour le contenu"):
                raw_data = parse_file(file_path, separator, page_to_extract)

                st.session_state["raw_data"] = raw_data

                st.session_state["last_page"] = page_to_extract

                st.session_state["last_source"] = selected_source

                st.rerun()

            st.write("#### Plage des titres")

            max_rows = len(raw_data) if raw_data else 1

            max_cols = len(raw_data[0]) if raw_data and raw_data[0] else 1

            title_row = st.number_input("Ligne des titres", min_value=0, max_value=max_rows - 1,

                                        value=min(default_title_range[0], max_rows - 1))

            title_col_start = st.number_input("Colonne début titres", min_value=0, max_value=max_cols - 1,

                                              value=min(default_title_range[1], max_cols - 1))

            title_col_end = st.number_input("Colonne fin titres", min_value=0, max_value=max_cols - 1,

                                            value=min(default_title_range[2], max_cols - 1))

            st.write("#### Plage des données")

            data_row_start = st.number_input("Ligne début données", min_value=0, max_value=max_rows - 1,

                                             value=min(default_data_range[0], max_rows - 1))

            data_row_end = st.number_input("Ligne fin données", min_value=0, max_value=max_rows - 1,

                                           value=min(default_data_range[1], max_rows - 1))

            if st.button("Appliquer et Sauvegarder"):

                title_range = [title_row, title_col_start, title_col_end]

                data_range = [data_row_start, data_row_end]

                update_source_settings(selected_source, separator, page_to_extract, title_range, data_range)

                if raw_data:

                    titles = raw_data[title_row][title_col_start:title_col_end + 1]

                    data = [row[title_col_start:title_col_end + 1] for row in raw_data[data_row_start:data_row_end + 1]]

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

if __name__ == "__main__":
    main()