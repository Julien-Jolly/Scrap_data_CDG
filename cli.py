# cli.py
import argparse
import pandas as pd
import os
import logging
import queue
import time
from datetime import datetime
from src.downloader import download_files, get_sources
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings
from src.utils import load_excel_data, make_unique_titles, insert_dataframe_to_sql, check_cell_changes, \
    load_previous_data
from src.config import DEST_PATH, get_download_dir

# Configurer la journalisation principale
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"cli_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurer un logger pour le résumé
summary_log_file = os.path.join(log_dir, f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
summary_logger = logging.getLogger('summary')
summary_logger.setLevel(logging.INFO)
summary_handler = logging.FileHandler(summary_log_file)
summary_handler.setFormatter(logging.Formatter('%(message)s'))
summary_logger.addHandler(summary_handler)


def download_and_process(args):
    """Télécharge, traite et insère les sources dans la BDD."""
    logger.info("Étape 1 : Téléchargement des fichiers...")
    status_queue = queue.Queue()
    sources = get_sources()
    downloaded_sources = []
    download_errors = []
    successes, total, errors = download_files(sources, status_queue)
    logger.info(f"Téléchargements terminés : {successes}/{total} réussis")

    # Collecter les résultats du téléchargement
    for source in sources:
        if any(error[0] == source for error in errors):
            error_msg = next(error[1] for error in errors if error[0] == source)
            download_errors.append({"Source": source, "Erreur": error_msg})
        else:
            downloaded_sources.append({"Source": source})

    logger.info("Étape 2 : Traitement et insertion dans la BDD...")
    process_and_insert(args.db_path, downloaded_sources, download_errors)


def process_only(args):
    """Traite les fichiers existants et insère dans la BDD."""
    logger.info("Traitement des fichiers existants et insertion dans la BDD...")
    downloaded_sources = [{"Source": source} for source in
                          get_downloaded_files(get_download_dir(datetime.now().strftime("%m-%d"))).keys()]
    download_errors = []
    process_and_insert(args.db_path, downloaded_sources, download_errors)


def process_and_insert(db_path, downloaded_sources, download_errors):
    """Traite les fichiers et insère les DataFrames dans la BDD."""
    date_str = datetime.now().strftime("%m-%d")
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

    for source in downloaded_files:
        source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(
            df_excel[df_excel[columns[0]] == source]) > 0 else source
        logger.info(f"Traitement de la source : {source}")
        print(f"Traitement de la source : {source}")
        settings = get_source_settings(source)
        separator = settings.get("separator", ";")
        page = settings.get("page", 0)
        title_range = settings.get("title_range", [0, 0, 0, 5])
        data_range = settings.get("data_range", [1, 10])
        selected_table = settings.get("selected_table", None)

        if not selected_table:
            logger.warning(f"Aucun tableau sélectionné pour {source}. Ignoré.")
            print(f"Avertissement : Aucun tableau sélectionné pour {source}. Ignoré.")
            insert_errors.append({"Source": source_name, "Erreur": "Aucun tableau sélectionné."})
            continue

        file_paths = downloaded_files[source]
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            logger.error(f"Fichier {selected_table} introuvable pour {source}. Ignoré.")
            print(f"Erreur : Fichier {selected_table} introuvable pour {source}. Ignoré.")
            insert_errors.append({"Source": source_name, "Erreur": f"Fichier {selected_table} introuvable."})
            continue

        try:
            # Récupérer la date de téléchargement (date de modification du fichier)
            download_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))

            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                logger.error(f"Aucune donnée extraite pour {selected_table}. Ignoré.")
                print(f"Erreur : Aucune donnée extraite pour {selected_table}. Ignoré.")
                insert_errors.append({"Source": source_name, "Erreur": "Aucune donnée extraite."})
                continue

            titles, data = extract_data(raw_data, title_range, data_range)
            if not titles or not data:
                logger.warning(f"Aucune donnée ou titre extrait pour {selected_table}. Ignoré.")
                print(f"Avertissement : Aucune donnée ou titre extrait pour {selected_table}. Ignoré.")
                insert_errors.append({"Source": source_name, "Erreur": "Aucune donnée ou titre extrait."})
                continue

            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                logger.error(
                    f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes) pour {source}. Ignoré.")
                print(
                    f"Erreur : Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes) pour {source}. Ignoré.")
                insert_errors.append({"Source": source_name,
                                      "Erreur": f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes)."})
                continue

            unique_titles = make_unique_titles(titles)
            data_with_datetime = []
            for row in data:
                data_with_datetime.append([download_datetime] + row)
                time.sleep(0.001)  # Conserver le léger décalage pour éviter des doublons exacts
            unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
            df_current = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

            # Ajouter à la liste des sources traitées
            processed_sources.append({"Source": source_name})

            # Charger les données de la veille
            df_previous = load_previous_data(source, db_path, date_str)

            # Vérifier les changements de types ou de nature
            cell_anomalies = check_cell_changes(df_current, df_previous, source_name)
            if cell_anomalies:
                logger.warning(f"Anomalies détectées pour {source_name} :")
                print(f"Anomalies détectées pour {source_name} :")
                for anomaly in cell_anomalies:
                    logger.warning(anomaly)
                    print(f"- {anomaly}")

            # Insérer les données dans la base
            table_name = source.replace(" ", "_").replace("-", "_").lower()
            try:
                insert_dataframe_to_sql(df_current, table_name, db_path)
                logger.info(f"DataFrame pour {source} inséré dans la table {table_name} avec extraction_datetime.")
                print(f"Succès : DataFrame pour {source} inséré dans la table {table_name} avec extraction_datetime.")
                inserted_sources.append({"Source": source_name})
            except Exception as e:
                logger.error(f"Erreur lors de l'insertion de {source} dans la BDD : {e}")
                print(f"Erreur lors de l'insertion de {source} dans la BDD : {e}")
                insert_errors.append({"Source": source_name, "Erreur": str(e)})

        except Exception as e:
            logger.error(f"Erreur lors du traitement de {source} : {e}")
            print(f"Erreur lors du traitement de {source} : {e}")
            insert_errors.append({"Source": source_name, "Erreur": str(e)})

    # Générer le fichier de log de résumé
    summary_logger.info("Résumé de l'exécution CLI\n")

    # Sources téléchargées
    summary_logger.info("Sources téléchargées :")
    if downloaded_sources:
        df_downloaded = pd.DataFrame(downloaded_sources)
        summary_logger.info(df_downloaded.to_string(index=False))
    else:
        summary_logger.info("Aucune source téléchargée.")
    summary_logger.info("\n")

    # Sources non téléchargées en erreur
    summary_logger.info("Sources non téléchargées en erreur :")
    if download_errors:
        df_download_errors = pd.DataFrame(download_errors)
        summary_logger.info(df_download_errors.to_string(index=False))
    else:
        summary_logger.info("Aucune erreur de téléchargement.")
    summary_logger.info("\n")

    # Sources traitées
    summary_logger.info("Sources traitées :")
    if processed_sources:
        df_processed = pd.DataFrame(processed_sources)
        summary_logger.info(df_processed.to_string(index=False))
    else:
        summary_logger.info("Aucune source traitée.")
    summary_logger.info("\n")

    # Sources insérées dans la BDD
    summary_logger.info("Sources traitées insérées dans la BDD :")
    if inserted_sources:
        df_inserted = pd.DataFrame(inserted_sources)
        summary_logger.info(df_inserted.to_string(index=False))
    else:
        summary_logger.info("Aucune source insérée.")
    summary_logger.info("\n")

    # Sources non insérées en erreur
    summary_logger.info("Sources traitées non insérées en erreur :")
    if insert_errors:
        df_insert_errors = pd.DataFrame(insert_errors)
        summary_logger.info(df_insert_errors.to_string(index=False))
    else:
        summary_logger.info("Aucune erreur d'insertion.")
    summary_logger.info("\n")


def main():
    parser = argparse.ArgumentParser(
        description="CLI pour le traitement des sources et l'insertion dans une BDD SQLite.")
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