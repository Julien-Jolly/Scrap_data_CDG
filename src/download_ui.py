# src/download_ui.py
import streamlit as st
import pandas as pd
import queue
import time
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from src.downloader import download_files, simple_dl, driver_dl, scrape_html_table_dl, scrape_html_table_with_captcha_dl, scrape_articles_dl, api_historical_data_dl
from src.utils import load_excel_data
from webdriver_manager.chrome import ChromeDriverManager



def run_download_with_status(sources, status_container, status_queue, report_container, retry=False):
    """Exécute le téléchargement et met à jour l'interface avec les statuts."""
    status_df = pd.DataFrame({
        "Source": sources,
        "Statut": ["⏳ En attente" for _ in sources]
    })
    status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(download_files if not retry else run_retry_download, sources, status_queue)

        while True:
            try:
                update = status_queue.get(timeout=0.1)
                if update[0] == "DONE":
                    break
                source, status = update
                status_df.loc[status_df["Source"] == source, "Statut"] = status
                status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)
            except queue.Empty:
                if future.done():
                    break
                time.sleep(0.1)

        successes, total, errors = future.result()

    report_container.empty()
    with report_container:
        st.write(f"### Rapport final{' (relance)' if retry else ''}")
        st.write(f"**Fichiers téléchargés avec succès : {successes}/{total}**")
        if errors:
            st.write("**Liste des fichiers en erreur :**")
            errors_df = pd.DataFrame(errors, columns=["Source", "Motif"])
            st.dataframe(errors_df, hide_index=True, use_container_width=True)
        else:
            st.success("Tous les fichiers ont été téléchargés avec succès !")
    return errors

def run_retry_download(sources, status_queue):
    """Relance les téléchargements pour les sources en erreur."""
    errors = []
    successes = 0
    total = len(sources)

    df_updated = load_excel_data()
    columns = df_updated.columns.tolist()
    df_to_retry = df_updated[df_updated[columns[0]].isin(sources)]

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        for index, row in df_to_retry.iterrows():
            source = row[columns[0]]
            extraction_type = row[columns[1]]
            status_queue.put((source, "⏳ En cours"))

            if extraction_type == "1":
                success, error = simple_dl(row, columns)
                if success:
                    successes += 1
                    status_queue.put((source, "✅ Succès"))
                else:
                    errors.append((source, error))
                    status_queue.put((source, "❌ Échec"))
            elif extraction_type == "2":
                success, error = driver_dl(row, columns, driver)
                if success:
                    successes += 1
                    status_queue.put((source, "✅ Succès"))
                else:
                    errors.append((source, error))
                    status_queue.put((source, "❌ Échec"))
            elif extraction_type == "3":
                success, error = scrape_html_table_dl(row, columns, driver)
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

def download_section(sources):
    st.header("Liste des sources à télécharger")

    if not sources:
        st.warning("Aucune source trouvée dans le fichier.")
        return

    col1, col2 = st.columns([1, 1])

    with col1:
        st.write("### Statut des téléchargements :")
        status_container = st.empty()
        status_df = pd.DataFrame({
            "Source": sources,
            "Statut": ["⏳ En attente" for _ in sources]
        })
        status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)

    with col2:
        st.write("### Rapport de Téléchargement :")
        report_container = st.empty()
        report_container.write("### Rapport final\nEn attente de la fin des téléchargements...")

        button_col1, button_col2, _ = st.columns([1, 1, 3])

        with button_col1:
            if button_col1.button("Lancer le téléchargement"):
                with st.spinner("Téléchargement en cours..."):
                    status_queue = queue.Queue()
                    errors = run_download_with_status(sources, status_container, status_queue, report_container)
                    st.session_state["download_errors"] = errors

        with button_col2:
            if "download_errors" in st.session_state and st.session_state["download_errors"]:
                if button_col2.button("Relancer les téléchargements en erreur"):
                    error_sources = [error[0] for error in st.session_state["download_errors"]]
                    status_df = pd.DataFrame({
                        "Source": error_sources,
                        "Statut": ["⏳ En attente" for _ in error_sources]
                    })
                    status_container.dataframe(status_df, hide_index=True, use_container_width=True, height=300)
                    with st.spinner("Relance des téléchargements en erreur..."):
                        status_queue = queue.Queue()
                        errors = run_download_with_status(error_sources, status_container, status_queue, report_container, retry=True)
                        st.session_state["download_errors"] = errors