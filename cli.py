# cli.py
import argparse
import pandas as pd
import os
from src.downloader import download_files, get_sources
from src.parser import get_downloaded_files, parse_file, extract_data, get_source_settings
from src.utils import load_excel_data, make_unique_titles, insert_dataframe_to_sql
from src.config import SOURCE_FILE

def download_and_process(args):
    """Télécharge, traite et insère les sources dans la BDD."""
    print("Étape 1 : Téléchargement des fichiers...")
    sources = get_sources()
    download_files(sources)
    print("Téléchargement terminé.")

    print("Étape 2 : Traitement et insertion dans la BDD...")
    process_and_insert(args.db_path)

def process_only(args):
    """Traite les fichiers existants et insère dans la BDD."""
    print("Traitement des fichiers existants et insertion dans la BDD...")
    process_and_insert(args.db_path)

def process_and_insert(db_path):
    """Traite les fichiers et insère les DataFrames dans la BDD."""
    downloaded_files = get_downloaded_files()
    if not downloaded_files:
        print("Erreur : Aucun fichier téléchargé trouvé.")
        return

    df_excel = load_excel_data()
    columns = df_excel.columns.tolist()

    for source in downloaded_files:
        print(f"Traitement de la source : {source}")
        source_name = df_excel[df_excel[columns[0]] == source][columns[7]].iloc[0] if len(df_excel[df_excel[columns[0]] == source]) > 0 else source
        settings = get_source_settings(source)
        separator = settings.get("separator", ";")
        page = settings.get("page", 0)
        title_range = settings.get("title_range", [0, 0, 0, 5])
        data_range = settings.get("data_range", [1, 10])
        selected_table = settings.get("selected_table", None)

        if not selected_table:
            print(f"Avertissement : Aucun tableau sélectionné pour {source}. Ignoré.")
            continue

        file_paths = downloaded_files[source]
        file_path = None
        for path in file_paths:
            if os.path.basename(path) == selected_table:
                file_path = path
                break

        if not file_path:
            print(f"Erreur : Fichier {selected_table} introuvable pour {source}. Ignoré.")
            continue

        try:
            raw_data = parse_file(file_path, separator, page, selected_columns=None)
            if not raw_data:
                print(f"Erreur : Aucune donnée extraite pour {selected_table}. Ignoré.")
                continue

            titles, data = extract_data(raw_data, title_range, data_range)
            if not titles or not data:
                print(f"Avertissement : Aucune donnée ou titre extrait pour {selected_table}. Ignoré.")
                continue

            data_col_count = max(len(row) for row in data) if data else 0
            if len(titles) != data_col_count:
                print(f"Erreur : Les titres ({len(titles)} colonnes) ne correspondent pas aux données ({data_col_count} colonnes) pour {source}. Ignoré.")
                continue

            unique_titles = make_unique_titles(titles)
            df = pd.DataFrame(data, columns=unique_titles)

            table_name = source.replace(" ", "_").replace("-", "_").lower()
            try:
                insert_dataframe_to_sql(df, table_name, db_path)
                print(f"Succès : DataFrame pour {source} inséré dans la table {table_name} avec extraction_datetime.")
            except Exception as e:
                print(f"Erreur lors de l'insertion de {source} dans la BDD : {e}")

        except Exception as e:
            print(f"Erreur lors du traitement de {source} : {e}")

def main():
    parser = argparse.ArgumentParser(description="CLI pour le traitement des sources et l'insertion dans une BDD.")
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