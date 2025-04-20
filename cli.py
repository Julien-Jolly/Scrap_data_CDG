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

# Formatter pour le fichier (détaillé)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Configurer le handler pour le fichier uniquement
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(file_formatter)

# Configurer le logger principal (pas de console handler)
logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler]
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
    print("=== Phase de téléchargement ===")
    logger.info("Etape 1 : Téléchargement des fichiers...")
    status_queue = queue.Queue()
    sources = get_sources()
    downloaded_sources = []
    download_errors = []
    successes, total, errors = download_files(sources, status_queue)

    # Collecter les résultats du téléchargement
    for source in sources:
        print(f"--- {source} ---")
        if any(error[0] == source for error in errors):
            error_msg = next(error[1] for error in errors if error[0] == source)
            print(f"Téléchargement : ❌ Échec ({error_msg})")
            logger.error(f"Téléchargement de {source} échoué : {error_msg}")
            download_errors.append({"Source": source, "Erreur": error_msg})
        else:
            print(f"Téléchargement : ✅ Succès")
            logger.info(f"Téléchargement de {source} réussi")
            downloaded_sources.append({"Source": source})
        print()  # Ajouter une ligne vide entre les blocs

    logger.info(f"Téléchargements terminés : {successes}/{total} réussis")
    print(f"\nRésumé : {successes}/{total} sources téléchargées avec succès")
    if download_errors:
        print("Sources en erreur :")
        for error in download_errors:
            print(f"- {error['Source']} ({error['Erreur']})")
    else:
        print("Sources en erreur : Aucune")

    print("\n=== Phase de traitement et insertion ===")
    logger.info("Etape 2 : Traitement et insertion dans la BDD...")
    process_and_insert(args.db_path, downloaded_sources, download_errors)

def process_only(args):
    """Traite les fichiers existants et insère dans la BDD."""
    print("=== Phase de traitement et insertion ===")
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
    sources_with_anomalies = []

    for source in downloaded_files:
        source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(
            df_excel[df_excel[columns[0]] == source]) > 0 else source
        print(f"--- {source_name} ---")
        logger.info(f"--- {source_name} ---")

        print(f"Traitement     : ⏳ En cours")
        settings = get_source_settings(source)
        separator = settings.get("separator", ";")
        page = settings.get("page", 0)
        title_range = settings.get("title_range", [0, 0, 0, 5])
        data_range = settings.get("data_range", [1, 10])
        selected_table = settings.get("selected_table", None)

        if not selected_table:
            msg = f"Aucun tableau sélectionné pour {source}. Ignoré."
            logger.warning(msg)
            print(f"Traitement     : ⚠️ Avertissement ({msg})")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({msg})")
            insert_errors.append({"Source": source_name, "Erreur": msg})
            print()  # Ajouter une ligne vide entre les blocs
            continue

        file_paths = downloaded_files[source]
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            msg = f"Fichier {selected_table} introuvable pour {source}. Ignoré."
            logger.error(msg)
            print(f"Traitement     : ❌ Échec ({msg})")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({msg})")
            insert_errors.append({"Source": source_name, "Erreur": msg})
            print()  # Ajouter une ligne vide entre les blocs
            continue

        try:
            download_datetime = datetime.fromtimestamp(os.path.getmtime(file_path))

            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                msg = f"Aucune donnée extraite pour {selected_table}. Ignoré."
                logger.error(msg)
                print(f"Traitement     : ❌ Échec ({msg})")
                print(f"Insertion      : 🚫 Non effectué")
                print(f"Résultat       : ❌ Échec ({msg})")
                insert_errors.append({"Source": source_name, "Erreur": msg})
                print()  # Ajouter une ligne vide entre les blocs
                continue

            titles, data = extract_data(raw_data, title_range, data_range)
            if not titles or not data:
                msg = f"Aucune donnée ou titre extrait pour {selected_table}. Ignoré."
                logger.warning(msg)
                print(f"Traitement     : ⚠️ Avertissement ({msg})")
                print(f"Insertion      : 🚫 Non effectué")
                print(f"Résultat       : ❌ Échec ({msg})")
                insert_errors.append({"Source": source_name, "Erreur": msg})
                print()  # Ajouter une ligne vide entre les blocs
                continue

            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                msg = f"Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes)."
                logger.error(msg)
                print(f"Traitement     : ❌ Échec ({msg})")
                print(f"Insertion      : 🚫 Non effectué")
                print(f"Résultat       : ❌ Échec ({msg})")
                insert_errors.append({"Source": source_name, "Erreur": msg})
                print()  # Ajouter une ligne vide entre les blocs
                continue

            unique_titles = make_unique_titles(titles)
            data_with_datetime = []
            for row in data:
                data_with_datetime.append([download_datetime] + row)
                time.sleep(0.001)
            unique_titles_with_datetime = ['extraction_datetime'] + unique_titles
            df_current = pd.DataFrame(data_with_datetime, columns=unique_titles_with_datetime)

            processed_sources.append({"Source": source_name})
            print(f"Traitement     : ✅ Succès")

            df_previous = load_previous_data(source, db_path, date_str)
            cell_anomalies = check_cell_changes(df_current, df_previous, source_name)
            anomalies_detected = bool(cell_anomalies)

            print(f"Insertion      : ⏳ En cours")
            table_name = source.replace(" ", "_").replace("-", "_").lower()
            try:
                insert_dataframe_to_sql(df_current, table_name, db_path)
                logger.info(f"DataFrame pour {source} inséré dans la table {table_name} avec extraction_datetime.")
                print(f"Insertion      : ✅ Succès")
                inserted_sources.append({"Source": source_name})
                if anomalies_detected:
                    # Capturer la première anomalie comme raison principale
                    anomaly_reason = cell_anomalies[0] if cell_anomalies else "Raison non spécifiée"
                    logger.warning(f"Anomalie pour {source_name} : {anomaly_reason}")
                    for anomaly in cell_anomalies:
                        logger.warning(anomaly)
                    sources_with_anomalies.append({"Source": source_name, "Anomalie": anomaly_reason})
            except Exception as e:
                msg = f"Erreur lors de l'insertion de {source} dans la BDD : {str(e)}"
                logger.error(msg)
                print(f"Insertion      : ❌ Échec ({str(e)[:50]}...)")
                print(f"Résultat       : ❌ Échec ({str(e)[:50]}...)")
                insert_errors.append({"Source": source_name, "Erreur": str(e)})
                print()  # Ajouter une ligne vide entre les blocs
                continue

            result_msg = "✅ Succès" if not anomalies_detected else f"✅ Succès (avec anomalie : {anomaly_reason[:50]})"
            print(f"Résultat       : {result_msg}")
            print()  # Ajouter une ligne vide entre les blocs

        except Exception as e:
            msg = f"Erreur lors du traitement de {source} : {str(e)}"
            logger.error(msg)
            print(f"Traitement     : ❌ Échec ({str(e)[:50]}...)")
            print(f"Insertion      : 🚫 Non effectué")
            print(f"Résultat       : ❌ Échec ({str(e)[:50]}...)")
            insert_errors.append({"Source": source_name, "Erreur": str(e)})
            print()  # Ajouter une ligne vide entre les blocs

    print(f"\nRésumé : {len(inserted_sources)}/{len(downloaded_files)} sources insérées avec succès")
    if insert_errors:
        print("Sources en erreur :")
        for error in insert_errors:
            print(f"- {error['Source']} ({error['Erreur']})")
    else:
        print("Sources en erreur : Aucune")
    if sources_with_anomalies:
        print("Sources avec anomalies :")
        for anomaly in sources_with_anomalies:
            print(f"- {anomaly['Source']} ({anomaly['Anomalie']})")
    else:
        print("Sources avec anomalies : Aucune")

    # Nettoyer les noms des sources pour éviter les problèmes d'alignement
    def clean_source_name(source):
        return source.strip()

    # Fonction pour formater les DataFrames manuellement avec alignement à gauche
    def format_dataframe(data, columns):
        if not data:
            return "Aucun élément"
        lines = ["\t".join(columns)]  # En-tête
        for item in data:
            line = "\t".join(clean_source_name(str(item.get(col, ""))) for col in columns)
            lines.append(line)
        return "\n".join(lines)

    # Générer le fichier de log de résumé avec alignement à gauche
    summary_logger.info("Résumé de l'exécution CLI\n")
    summary_logger.info("Sources téléchargées :")
    if downloaded_sources:
        summary_logger.info(format_dataframe(downloaded_sources, ["Source"]))
    else:
        summary_logger.info("Aucun élément")
    summary_logger.info("\n")

    summary_logger.info("Sources non téléchargées en erreur :")
    if download_errors:
        summary_logger.info(format_dataframe(download_errors, ["Source", "Erreur"]))
    else:
        summary_logger.info("Aucun élément")
    summary_logger.info("\n")

    summary_logger.info("Sources traitées :")
    if processed_sources:
        summary_logger.info(format_dataframe(processed_sources, ["Source"]))
    else:
        summary_logger.info("Aucun élément")
    summary_logger.info("\n")

    summary_logger.info("Sources traitées insérées dans la BDD :")
    if inserted_sources:
        summary_logger.info(format_dataframe(inserted_sources, ["Source"]))
    else:
        summary_logger.info("Aucun élément")
    summary_logger.info("\n")

    summary_logger.info("Sources traitées avec anomalies :")
    if sources_with_anomalies:
        summary_logger.info(format_dataframe(sources_with_anomalies, ["Source", "Anomalie"]))
    else:
        summary_logger.info("Aucun élément")
    summary_logger.info("\n")

    summary_logger.info("Sources traitées non insérées en erreur :")
    if insert_errors:
        summary_logger.info(format_dataframe(insert_errors, ["Source", "Erreur"]))
    else:
        summary_logger.info("Aucun élément")
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