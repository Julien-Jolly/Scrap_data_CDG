# src/app.py
import streamlit as st
import pandas as pd
import concurrent.futures
import queue
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from src.downloader import get_sources, download_files, simple_dl, driver_dl
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

            # Créer une ligne pour les deux boutons côte à côte
            button_col1, button_col2, _ = st.columns([1, 1, 3])

            with button_col1:
                # Bouton pour lancer le téléchargement
                if button_col1.button("Lancer le téléchargement"):
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

                        # Stocker les erreurs dans st.session_state pour une relance éventuelle
                        st.session_state["download_errors"] = errors

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

            with button_col2:
                # Bouton pour relancer les téléchargements en erreur
                # Afficher uniquement si des erreurs existent et que le rapport est affiché
                if "download_errors" in st.session_state and st.session_state["download_errors"]:
                    if button_col2.button("Relancer les téléchargements en erreur"):
                        with st.spinner("Relance des téléchargements en erreur..."):
                            # Créer une file d'attente pour les mises à jour de statut
                            status_queue = queue.Queue()

                            # Recharger le fichier Excel pour prendre en compte les modifications
                            df_updated = load_excel_data()
                            columns = df_updated.columns.tolist()

                            # Filtrer les sources en erreur
                            error_sources = [error[0] for error in st.session_state["download_errors"]]
                            df_to_retry = df_updated[df_updated[columns[0]].isin(error_sources)]

                            # Réinitialiser le DataFrame des statuts pour les sources en erreur
                            status_df = pd.DataFrame({
                                "Source": error_sources,
                                "Statut": ["⏳ En attente" for _ in error_sources]
                            })
                            status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)

                            # Lancer le téléchargement uniquement pour les sources en erreur
                            def run_retry_download():
                                errors = []
                                successes = 0
                                total = len(df_to_retry)

                                # Configurer Selenium avec Chrome
                                options = webdriver.ChromeOptions()
                                options.add_argument("--headless")
                                options.add_argument("--disable-gpu")
                                options.add_argument("--disable-blink-features=AutomationControlled")
                                service = Service("C:/chromedriver/chromedriver.exe")
                                driver = webdriver.Chrome(service=service, options=options)

                                try:
                                    for index, row in df_to_retry.iterrows():
                                        source = row[columns[0]]
                                        status_queue.put((source, "⏳ En cours"))

                                        if row[columns[1]] == "1":
                                            success, error = simple_dl(row)
                                            if success:
                                                successes += 1
                                                status_queue.put((source, "✅ Succès"))
                                            else:
                                                errors.append((source, error))
                                                status_queue.put((source, "❌ Échec"))
                                        elif row[columns[1]] == "2":
                                            success, error = driver_dl(row, driver)
                                            if success:
                                                successes += 1
                                                status_queue.put((source, "✅ Succès"))
                                            else:
                                                errors.append((source, error))
                                                status_queue.put((source, "❌ Échec"))
                                finally:
                                    driver.quit()

                                status_queue.put(("DONE", None))
                                return successes, total, errors

                            # Démarrer le thread de téléchargement
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                future = executor.submit(run_retry_download)

                                # Consommer les mises à jour de statut
                                while True:
                                    try:
                                        update = status_queue.get(timeout=0.1)
                                        if update[0] == "DONE":
                                            break
                                        source, status = update
                                        status_df.loc[status_df["Source"] == source, "Statut"] = status
                                        status_container.dataframe(status_df, hide_index=True, use_container_width=True,
                                                                   height=300)
                                    except queue.Empty:
                                        if future.done():
                                            break
                                        time.sleep(0.1)

                                # Récupérer le résultat final
                                successes, total, errors = future.result()

                            # Mettre à jour les erreurs dans st.session_state
                            st.session_state["download_errors"] = errors

                            # Mettre à jour le rapport final
                            report_container.empty()
                            with report_container:
                                st.write("### Rapport final (relance)")
                                st.write(f"**Fichiers téléchargés avec succès : {successes}/{total}**")
                                if errors:
                                    st.write("**Liste des fichiers en erreur :**")
                                    errors_df = pd.DataFrame(errors, columns=["Source", "Motif"])
                                    st.dataframe(errors_df, hide_index=True, use_container_width=True)
                                else:
                                    st.success("Tous les fichiers en erreur ont été téléchargés avec succès !")

        # Ajouter la section "Gestion des Sources" sous les éléments existants
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

        # Mettre à jour last_source dès que la source change
        # Stocker l'ancienne source pour détecter un changement
        previous_source = st.session_state.get("last_source")

        # Mettre à jour last_source
        st.session_state["last_source"] = selected_source

        # Charger les données du fichier Excel pour récupérer le nom de la source (colonne 8)
        df = load_excel_data()
        columns = df.columns.tolist()
        # La 8ème colonne (index 7) contient le nom de la source
        source_name = df[df[columns[0]] == selected_source][columns[7]].iloc[0] if len(
            df[df[columns[0]] == selected_source]) > 0 else selected_source

        settings = get_source_settings(selected_source)

        default_separator = settings["separator"]

        default_page = settings.get("page", 0)

        default_title_range = settings["title_range"]  # [start_row, end_row, start_col, end_col]

        default_data_range = settings["data_range"]  # [start_row, end_row]

        # Extraire les valeurs par défaut
        default_title_start_row = default_title_range[0] if len(default_title_range) > 3 else default_title_range[0]
        default_title_end_row = default_title_range[1] if len(default_title_range) > 3 else default_title_range[0]
        default_title_col_start = default_title_range[2] if len(default_title_range) > 3 else 0
        default_title_col_end = default_title_range[3] if len(default_title_range) > 3 else default_title_range[2]

        # Initialiser ou réinitialiser les valeurs temporaires dans st.session_state
        # Si les clés n'existent pas ou si la source a changé, utiliser les valeurs par défaut
        if "temp_data_row_start" not in st.session_state or previous_source != selected_source:
            st.session_state["temp_data_row_start"] = default_data_range[0]
        if "temp_data_row_end" not in st.session_state or previous_source != selected_source:
            st.session_state["temp_data_row_end"] = default_data_range[1]

        # Charger les données brutes uniquement si la source ou la page change
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
            # Afficher le nom de la source (colonne 8)
            st.write(f"**Nom de la source :** {source_name}")

            if not raw_data:
                st.warning(f"Aucun contenu extrait. Vérifiez le fichier ou le séparateur.")
            else:
                df_raw = pd.DataFrame(raw_data)
                st.dataframe(df_raw, use_container_width=True, height=400)

        with col2:
            st.write("### Paramétrage de l'extraction")
            separator = st.text_input("Séparateur", value=default_separator)

            # Déplacer "Page à extraire (PDF uniquement)" sous "Séparateur"
            page_to_extract = st.number_input("Page à extraire (PDF uniquement)", min_value=0, value=default_page,
                                              step=1)
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
            # Utiliser st.session_state pour stocker les valeurs temporaires
            data_row_start = st.number_input("Ligne début données", min_value=0, max_value=max_rows - 1,
                                            value=min(st.session_state["temp_data_row_start"], max_rows - 1),
                                            key="data_row_start_input")
            # Mettre à jour la valeur temporaire dans st.session_state
            st.session_state["temp_data_row_start"] = data_row_start

            # Ajuster la valeur par défaut de data_row_end pour qu'elle soit cohérente avec data_row_start
            default_data_row_end = max(data_row_start, st.session_state["temp_data_row_end"])
            data_row_end = st.number_input("Ligne fin données", min_value=data_row_start, max_value=max_rows - 1,
                                           value=min(default_data_row_end, max_rows - 1),
                                           key="data_row_end_input")
            # Mettre à jour la valeur temporaire dans st.session_state
            st.session_state["temp_data_row_end"] = data_row_end

            if st.button("Appliquer et Sauvegarder"):
                title_range = [title_row_start, title_row_end, title_col_start, title_col_end]
                # Utiliser les valeurs temporaires stockées dans st.session_state
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

if __name__ == "__main__":
    main()