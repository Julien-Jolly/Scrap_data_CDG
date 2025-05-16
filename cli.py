# src/cli.py
import argparse
import pandas as pd
import os
import queue
import threading
from datetime import datetime
from src.downloader import download_files, get_sources
from src.parser import get_downloaded_files, parse_file, get_source_settings, extract_data_from_combinations
from src.utils import load_excel_data, insert_dataframe_to_sql, check_cell_changes, load_previous_data, save_csv, generate_create_table_query, save_create_table_query
from src.config import get_download_dir, configure_logging

# Configurer le logging
logger, summary_logger = configure_logging("cli")

def run_downloads(sources, status_queue, date_str):
    """Exécute les téléchargements avec jusqu'à deux relances."""
    downloaded_sources = []
    download_errors = []
    max_retries = 2

    def run_download(sources_to_download, result_container):
        try:
            successes, total, errors = download_files(sources_to_download, status_queue, date_str)
            result_container.extend([(successes, total, errors)])
            return successes, total, errors
        except Exception as e:
            logger.error(f"Erreur critique lors du téléchargement : {str(e)}")
            print(f"Erreur critique lors du téléchargement : {str(e)}")
            result_container.extend([(0, len(sources_to_download), [(s, str(e)) for s in sources_to_download])])
            return 0, len(sources_to_download), [(s, str(e)) for s in sources_to_download]

    print("--- Tentative initiale ---")
    sources_to_download = sources.copy()
    source_status = {source: False for source in sources}
    result_container = []  # Store results from run_download
    download_thread = threading.Thread(target=run_download, args=(sources_to_download, result_container))
    download_thread.start()

    errors = []  # Initialize errors for the loop
    while download_thread.is_alive() or not status_queue.empty():
        try:
            source, status = status_queue.get(timeout=1)
            if source == "DONE":
                break
            if status == "⏳ En cours":
                print(f"--- {source} ---")
                print("Téléchargement : ⏳ En cours")
                source_status[source] = True
            elif status == "✅ Succès":
                if source_status.get(source, False):
                    print("Téléchargement : ✅ Succès")
                    print()
                downloaded_sources.append({"Source": source})
            elif status == "❌ Échec":
                if source_status.get(source, False):
                    error_msg = next((err[1] for err in errors if err[0] == source), "Erreur inconnue")
                    print(f"Téléchargement : ❌ Échec ({error_msg})")
                    print()
                download_errors.append({"Source": source, "Erreur": error_msg})
            elif status == "🚫 Ignoré":
                if source_status.get(source, False):
                    print(f"Téléchargement : 🚫 Ignoré")
                    print()
                download_errors.append({"Source": source, "Erreur": "Type d'extraction invalide"})
        except queue.Empty:
            continue

    download_thread.join()

    # Retrieve results after thread completion
    if result_container:
        successes, total, errors = result_container[0]
    else:
        errors = [(source, "Téléchargement non effectué") for source in sources_to_download]

    # Update download_errors with final errors
    for source in sources_to_download:
        if source not in [d["Source"] for d in downloaded_sources]:
            error_msg = next((err[1] for err in errors if err[0] == source), "Erreur inconnue")
            if not any(e["Source"] == source for e in download_errors):
                download_errors.append({"Source": source, "Erreur": error_msg})

    # Retry logic
    failed_sources = [error["Source"] for error in download_errors]
    retry_count = 0
    while failed_sources and retry_count < max_retries:
        retry_count += 1
        print(f"\n--- Tentative de relance {retry_count}/{max_retries} pour {len(failed_sources)} sources ---")
        logger.info(f"Tentative de relance {retry_count}/{max_retries} pour {len(failed_sources)} sources")
        new_downloaded = []
        new_errors = []
        sources_to_retry = failed_sources.copy()
        source_status = {source: False for source in sources_to_retry}
        result_container = []  # Reset for retry
        status_queue = queue.Queue()
        download_thread = threading.Thread(target=run_download, args=(sources_to_retry, result_container))
        download_thread.start()

        errors = []  # Reset errors for retry loop
        while download_thread.is_alive() or not status_queue.empty():
            try:
                source, status = status_queue.get(timeout=1)
                if source == "DONE":
                    break
                if status == "⏳ En cours":
                    print(f"--- {source} ---")
                    print(f"Téléchargement (Tentative {retry_count}) : ⏳ En cours")
                    source_status[source] = True
                elif status == "✅ Succès":
                    if source_status.get(source, False):
                        print(f"Téléchargement (Tentative {retry_count}) : ✅ Succès")
                        print()
                    new_downloaded.append({"Source": source})
                elif status == "❌ Échec":
                    if source_status.get(source, False):
                        error_msg = next((err[1] for err in errors if err[0] == source), "Erreur inconnue")
                        print(f"Téléchargement (Tentative {retry_count}) : ❌ Échec ({error_msg})")
                        print()
                    new_errors.append({"Source": source, "Erreur": error_msg})
                elif status == "🚫 Ignoré":
                    if source_status.get(source, False):
                        print(f"Téléchargement (Tentative {retry_count}) : 🚫 Ignoré")
                        print()
                    new_errors.append({"Source": source, "Erreur": "Type d'extraction invalide"})
            except queue.Empty:
                continue

        download_thread.join()

        # Retrieve retry results
        if result_container:
            successes, total, errors = result_container[0]
        else:
            errors = [(source, "Téléchargement non effectué") for source in sources_to_retry]

        # Update new_errors with final errors
        for source in sources_to_retry:
            if source not in [d["Source"] for d in new_downloaded]:
                error_msg = next((err[1] for err in errors if err[0] == source), "Erreur inconnue")
                if not any(e["Source"] == source for e in new_errors):
                    new_errors.append({"Source": source, "Erreur": error_msg})

        downloaded_sources.extend(new_downloaded)
        failed_sources = [error["Source"] for error in new_errors]
        download_errors = [error for error in download_errors if error["Source"] not in [d["Source"] for d in new_downloaded]]
        download_errors.extend(new_errors)
        print(f"\nRésumé tentative {retry_count} : {len(new_downloaded)}/{len(sources_to_retry)} sources téléchargées avec succès")

    return downloaded_sources, download_errors

def process_and_insert(db_path, downloaded_sources, download_errors, date_str):
    """Traite les fichiers, génère un CSV et un script SQL, et insère dans la BDD."""
    downloaded_files = get_downloaded_files(get_download_dir(date_str))
    if not downloaded_files:
        logger.error("Aucun fichier téléchargé trouvé.")
        print("Erreur : Aucun fichier téléchargé trouvé.")
        return
    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()
    processed_sources = []
    inserted_sources = []
    insert_errors = []
    sources_with_anomalies = []
    global_data = []
    unique_titles = set()
    settings_data = get_source_settings(None)
    if not settings_data:
        logger.error("Aucun paramètre trouvé dans source_settings.json.")
        print("Erreur : Aucun paramètre trouvé dans source_settings.json.")
        return
    for source_dict in downloaded_sources:
        source = source_dict["Source"]
        source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(
            df_excel[df_excel[columns[0]] == source]) > 0 else source
        print(f"--- {source_name} ---")
        logger.info(f"--- {source_name} ---")
        print(f"Traitement     : ⏳ En cours")
        source_settings = settings_data.get(source, {}).get("tables", {})
        if not source_settings:
            msg = f"Aucun paramètre configuré pour {source}. Ignoré."
            logger.warning(msg)
            print(f"Traitement     : ⚠️ Avertissement ({msg})")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({msg})")
            insert_errors.append({"Source": source_name, "Erreur": msg})
            print()
            continue
        file_paths = downloaded_files.get(source, [])
        if not file_paths:
            msg = f"Aucun fichier trouvé pour {source}. Ignoré."
            logger.error(msg)
            print(f"Traitement     : ❌ Échec ({msg})")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({msg})")
            insert_errors.append({"Source": source_name, "Erreur": msg})
            print()
            continue
        try:
            for table_name, table_settings in source_settings.items():
                file_path = next((fp for fp in file_paths if os.path.basename(fp) == table_name), None)
                if not file_path:
                    msg = f"Fichier {table_name} introuvable pour {source}. Ignoré."
                    logger.error(msg)
                    print(f"Traitement     : ❌ Échec ({msg})")
                    print(f"Insertion      : 🚫 Non effectué")
                    print(f"Résultat       : ❌ Échec ({msg})")
                    insert_errors.append({"Source": source_name, "Erreur": msg})
                    print()
                    continue
                extraction_datetime = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
                raw_data = parse_file(file_path, separator=";", page=0, selected_columns=None)
                if not raw_data:
                    msg = f"Aucune donnée extraite pour {table_name}. Ignoré."
                    logger.error(msg)
                    print(f"Traitement     : ❌ Échec ({msg})")
                    print(f"Insertion      : 🚫 Non effectué")
                    print(f"Résultat       : ❌ Échec ({msg})")
                    insert_errors.append({"Source": source_name, "Erreur": msg})
                    print()
                    continue
                table_data, table_titles = extract_data_from_combinations(raw_data, table_settings.get("combinations", []), source_name, table_name)
                global_data.extend(table_data)
                unique_titles.update(table_titles)
            processed_sources.append({"Source": source_name})
            print(f"Traitement     : ✅ Succès")
            print(f"Insertion      : ⏳ En cours")
            table_name = source.replace(" ", "_").replace("-", "_").lower()
            df_current = pd.DataFrame(table_data)
            if not df_current.empty:
                try:
                    insert_dataframe_to_sql(df_current, table_name, db_path)
                    print(f"Insertion      : ✅ Succès")
                    inserted_sources.append({"Source": source_name})
                except Exception as e:
                    msg = f"Erreur lors de l'insertion de {source} dans la BDD : {str(e)}"
                    logger.error(msg)
                    print(f"Insertion      : ❌ Échec ({str(e)[:50]}...)")
                    print(f"Résultat       : ❌ Échec ({str(e)[:50]}...)")
                    insert_errors.append({"Source": source_name, "Erreur": str(e)})
                    print()
                    continue
                df_previous = load_previous_data(source, db_path, date_str)
                cell_anomalies = check_cell_changes(df_current, df_previous, source_name)
                if cell_anomalies:
                    for anomaly in cell_anomalies:
                        logger.warning(anomaly)
                        sources_with_anomalies.append({"Source": source_name, "Anomalie": anomaly})
                    print(f"Résultat       : ✅ Succès (avec anomalies)")
                else:
                    print(f"Résultat       : ✅ Succès")
            else:
                print(f"Insertion      : 🚫 Non effectué (aucune donnée)")
                print(f"Résultat       : ❌ Échec (aucune donnée)")
                insert_errors.append({"Source": source_name, "Erreur": "Aucune donnée extraite"})
            print()
        except Exception as e:
            msg = f"Erreur lors du traitement de {source} : {str(e)}"
            logger.error(msg)
            print(f"Traitement     : ❌ Échec ({str(e)[:50]}...)")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({str(e)[:50]}...)")
            insert_errors.append({"Source": source_name, "Erreur": str(e)})
            print()
    if global_data:
        df_global = pd.DataFrame(global_data)
        for title in unique_titles:
            if title not in df_global.columns:
                df_global[title] = None
        df_global = df_global[["source", "datetime_extraction"] + sorted(unique_titles)]
        try:
            csv_file = save_csv(df_global, date_str)
            print(f"CSV généré : {csv_file}")
            logger.info(f"CSV généré : {csv_file}")
        except Exception as e:
            logger.error(f"Erreur lors de la génération du CSV : {str(e)}")
            print(f"Erreur lors de la génération du CSV : {str(e)}")
        try:
            create_table_query = generate_create_table_query(unique_titles)
            sql_file = save_create_table_query(create_table_query, date_str)
            print(f"Script SQL généré : {sql_file}")
            logger.info(f"Script SQL généré : {sql_file}")
        except Exception as e:
            logger.error(f"Erreur lors de la génération du script SQL : {str(e)}")
            print(f"Erreur lors de la génération du script SQL : {str(e)}")
    print(f"\nRésumé : {len(inserted_sources)}/{len(downloaded_files)} sources insérées avec succès")
    if insert_errors:
        print("Sources en erreur :")
        for error in insert_errors:
            print(f"- {error['Source']} ({error['Erreur']})")
    if sources_with_anomalies:
        print("Sources avec anomalies :")
        for anomaly in sources_with_anomalies:
            print(f"- {anomaly['Source']} ({anomaly['Anomalie']})")
    summary_logger.info("Résumé de l'exécution CLI\n")
    summary_logger.info("Sources téléchargées :\n" + "\n".join([d["Source"] for d in downloaded_sources]) or "Aucun élément")
    summary_logger.info("\nSources non téléchargées en erreur :\n" + "\n".join([f"{e['Source']} ({e['Erreur']})" for e in download_errors]) or "Aucun élément")
    summary_logger.info("\nSources traitées :\n" + "\n".join([p["Source"] for p in processed_sources]) or "Aucun élément")
    summary_logger.info("\nSources insérées :\n" + "\n".join([i["Source"] for i in inserted_sources]) or "Aucun élément")
    summary_logger.info("\nSources avec anomalies :\n" + "\n".join([f"{a['Source']} ({a['Anomalie']})" for a in sources_with_anomalies]) or "Aucun élément")
    summary_logger.info("\nSources en erreur :\n" + "\n".join([f"{e['Source']} ({e['Erreur']})" for e in insert_errors]) or "Aucun élément")

def download_and_process(args):
    """Télécharge, traite et insère les sources dans la BDD."""
    logger.info(f"--- Nouvelle exécution CLI démarrée à {datetime.now()} ---")
    summary_logger.info(f"--- Nouvelle exécution CLI démarrée à {datetime.now()} ---")
    print("=== Phase de téléchargement ===")
    logger.info("Etape 1 : Téléchargement des fichiers...")
    sources = get_sources()
    if not sources:
        logger.error("Aucune source trouvée")
        print("Erreur : Aucune source trouvée")
        return
    status_queue = queue.Queue()
    date_str = datetime.now().strftime("%m-%d")
    downloaded_sources, download_errors = run_downloads(sources, status_queue, date_str)
    print(f"\nRésumé final de la phase de téléchargement : {len(downloaded_sources)}/{len(sources)} sources téléchargées avec succès")
    if download_errors:
        print("Sources en erreur après toutes les tentatives :")
        for error in download_errors:
            print(f"- {error['Source']} ({error['Erreur']})")
    else:
        print("Sources en erreur après toutes les tentatives : Aucune")
    print("\n=== Phase de traitement et insertion ===")
    logger.info("Etape 2 : Traitement et insertion dans la BDD...")
    process_and_insert(args.db_path, downloaded_sources, download_errors, date_str)

def process_only(args):
    """Traite les fichiers existants et insère dans la BDD."""
    logger.info(f"--- Nouvelle exécution CLI démarrée à {datetime.now()} ---")
    summary_logger.info(f"--- Nouvelle exécution CLI démarrée à {datetime.now()} ---")
    print("=== Phase de traitement et insertion ===")
    logger.info("Traitement des fichiers existants et insertion dans la BDD...")
    date_str = datetime.now().strftime("%m-%d")
    downloaded_sources = [{"Source": source} for source in get_downloaded_files(get_download_dir(date_str)).keys()]
    download_errors = []
    process_and_insert(args.db_path, downloaded_sources, download_errors, date_str)

def main():
    parser = argparse.ArgumentParser(description="CLI pour le traitement des sources et l'insertion dans une BDD SQLite.")
    subparsers = parser.add_subparsers(dest="command")
    parser_download = subparsers.add_parser("download_and_process", help="Télécharge, traite et insère dans la BDD")
    parser_download.add_argument("--db_path", default="database.db", help="Chemin vers la base de données SQLite")
    parser_process = subparsers.add_parser("process_only", help="Traite les fichiers existants et insère dans la BDD")
    parser_process.add_argument("--db_path", default="database.db", help="Chemin vers la base de données SQLite")
    args = parser.parse_args()
    if args.command == "download_and_process":
        download_and_process(args)
    elif args.command == "process_only":
        process_only(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()