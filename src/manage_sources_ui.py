# src/manage_sources_ui.py
import streamlit as st
import pandas as pd
import queue
import time
import concurrent.futures
from src.downloader import download_files
from src.utils import load_excel_data, save_to_excel
from src.config import SOURCE_FILE

def manage_sources_section(sources):
    """Affiche la section 'Gestion des Sources' avec suivi des téléchargements et rapport des erreurs."""
    st.header("Gestion des Sources et Téléchargements")
    st.write("Visualisez et modifiez les sources, lancez les téléchargements et consultez les erreurs.")

    # Charger le DataFrame Excel
    df = load_excel_data()
    columns = df.columns.tolist()

    if len(columns) < 7:
        st.error("Le fichier Excel ne contient pas assez de colonnes. Vérifiez sa structure.")
        return

    # Créer un DataFrame éditable avec une colonne Statut dynamique
    edit_df = df[[columns[0], columns[1], columns[2], columns[3], columns[4], columns[5], columns[6]]].copy()
    edit_df.columns = ["Source", "Type d'extraction", "URL", "XPath reformaté", "Donnée", "Commentaires", "Statut Excel"]
    edit_df.insert(0, "Statut", ["⏳ En attente" for _ in range(len(edit_df))])

    # Configuration des colonnes (désactiver Source)
    disabled_columns = {"Source": st.column_config.Column(disabled=True)}

    # Afficher le DataFrame éditable
    st.subheader("Sources")
    data_container = st.empty()
    edited_df = data_container.data_editor(
        edit_df,
        hide_index=True,
        column_config=disabled_columns,
        use_container_width=True,
        height=400,
        num_rows="fixed"
    )

    # Bouton pour sauvegarder les modifications
    if st.button("Sauvegarder les modifications"):
        df[columns[1]] = edited_df["Type d'extraction"]
        df[columns[2]] = edited_df["URL"]
        df[columns[3]] = edited_df["XPath reformaté"]
        df[columns[4]] = edited_df["Donnée"]
        df[columns[5]] = edited_df["Commentaires"]
        df[columns[6]] = edited_df["Statut Excel"]
        save_to_excel(df)
        st.success("Modifications sauvegardées dans le fichier Excel !")

    # Section pour les téléchargements
    st.subheader("Téléchargements")
    report_container = st.empty()
    error_container = st.empty()

    # Boutons pour lancer et relancer les téléchargements
    button_col1, button_col2, _ = st.columns([1, 1, 3])

    with button_col1:
        if st.button("Lancer le téléchargement"):
            with st.spinner("Téléchargement en cours..."):
                status_queue = queue.Queue()
                errors = run_download_with_status(sources, data_container, status_queue, report_container, edited_df, error_container)
                st.session_state["download_errors"] = errors

    with button_col2:
        if "download_errors" in st.session_state and st.session_state["download_errors"]:
            if st.button("Relancer les téléchargements en erreur"):
                with st.spinner("Relance des téléchargements en erreur..."):
                    error_df = pd.DataFrame(st.session_state["download_errors"], columns=["Source", "Motif"])
                    error_df.insert(0, "Statut", ["⏳ En attente" for _ in range(len(error_df))])
                    status_queue = queue.Queue()
                    errors = run_error_download_with_status(error_df, status_queue, report_container, error_container)
                    st.session_state["download_errors"] = errors

def run_download_with_status(sources, data_container, status_queue, report_container, edit_df, error_container):
    """Exécute le téléchargement et met à jour les statuts dans le DataFrame principal."""
    status_df = edit_df.copy()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(download_files, sources, status_queue)

        while True:
            try:
                update = status_queue.get(timeout=0.1)
                if update[0] == "DONE":
                    break
                source, status = update
                status_df.loc[status_df["Source"] == source, "Statut"] = status
                data_container.data_editor(
                    status_df,
                    hide_index=True,
                    column_config={"Source": st.column_config.Column(disabled=True)},
                    use_container_width=True,
                    height=400,
                    num_rows="fixed"
                )
            except queue.Empty:
                if future.done():
                    break
                time.sleep(0.1)

        successes, total, errors = future.result()

    # Afficher le rapport des téléchargements
    report_container.empty()
    error_container.empty()
    with report_container:
        st.write("### Rapport final")
        st.write(f"**Fichiers téléchargés avec succès : {successes}/{total}**")
    with error_container:
        if errors:
            st.write("**Sources en erreur :**")
            errors_df = pd.DataFrame(errors, columns=["Source", "Motif"])
            errors_df.insert(0, "Statut", ["❌ Échec" for _ in range(len(errors_df))])
            st.dataframe(errors_df, hide_index=True, use_container_width=True)
        else:
            st.success("Tous les fichiers ont été téléchargés avec succès !")

    return errors

def run_error_download_with_status(error_df, status_queue, report_container, error_container):
    """Exécute la relance des téléchargements pour les sources en erreur et met à jour les statuts dans le tableau des erreurs."""
    error_sources = error_df["Source"].tolist()
    status_error_df = error_df.copy()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(download_files, error_sources, status_queue)

        while True:
            try:
                update = status_queue.get(timeout=0.1)
                if update[0] == "DONE":
                    break
                source, status = update
                status_error_df.loc[status_error_df["Source"] == source, "Statut"] = status
                error_container.dataframe(
                    status_error_df,
                    hide_index=True,
                    use_container_width=True
                )
            except queue.Empty:
                if future.done():
                    break
                time.sleep(0.1)

        successes, total, errors = future.result()

    # Afficher le rapport final de la relance
    report_container.empty()
    error_container.empty()
    with report_container:
        st.write("### Rapport final (relance)")
        st.write(f"**Fichiers téléchargés avec succès : {successes}/{total}**")
    with error_container:
        if errors:
            st.write("**Sources toujours en erreur :**")
            errors_df = pd.DataFrame(errors, columns=["Source", "Motif"])
            errors_df.insert(0, "Statut", ["❌ Échec" for _ in range(len(errors_df))])
            st.dataframe(errors_df, hide_index=True, use_container_width=True)
        else:
            st.success("Tous les fichiers en erreur ont été téléchargés avec succès !")

    return errors