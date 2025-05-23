import os
import glob
import pandas as pd
import logging
from datetime import datetime


def concatenate_bkam_taux_csvs(input_dir="bkam_taux_csvs", output_file="combined_taux_2020_2025.csv"):
    """
    Liste et concatène les fichiers CSV du répertoire de téléchargement en un seul fichier.

    Args:
        input_dir (str): Répertoire contenant les fichiers CSV (par défaut: 'bkam_taux_csvs').
        output_file (str): Nom du fichier CSV combiné (par défaut: 'combined_taux_2020_2025.csv').

    Returns:
        bool: True si la concaténation réussit, False sinon.
    """
    # Configurer le logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('concatenate_bkam_taux_csvs.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    # Vérifier si le répertoire existe
    if not os.path.exists(input_dir):
        logger.error(f"Le répertoire {input_dir} n'existe pas.")
        return False

    # Lister tous les fichiers CSV
    csv_pattern = os.path.join(input_dir, "taux_*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        logger.warning(f"Aucun fichier CSV trouvé dans {input_dir}.")
        return False

    logger.info(f"{len(csv_files)} fichiers CSV trouvés dans {input_dir}.")

    # Liste pour stocker les DataFrames
    all_dfs = []

    # Traiter chaque fichier CSV
    for csv_file in csv_files:
        # Extraire la date du nom de fichier (ex. taux_20200106.csv -> 2020-01-06)
        filename = os.path.basename(csv_file)
        try:
            date_str = filename.split('_')[-1].replace('.csv', '')
            date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError as e:
            logger.error(f"Impossible d'extraire la date de {filename}: {e}")
            continue

        try:
            # Lire les colonnes spécifiées, en sautant les métadonnées
            df = pd.read_csv(
                csv_file,
                sep=';',
                encoding='utf-8',
                skiprows=2,
                usecols=['Date d\'échéance', 'Transaction', 'Taux moyen pondéré', 'Date de la valeur']
            )
            # Filtrer les lignes valides (date au format DD/MM/YYYY ou 'Total')
            df = df[df['Date d\'échéance'].str.match(r'\d{2}/\d{2}/\d{4}') | (df['Date d\'échéance'] == 'Total')]
            if df.empty:
                logger.warning(f"Aucune donnée valide dans {csv_file}.")
                continue
            df['Date'] = date
            all_dfs.append(df)
            logger.debug(f"Fichier {csv_file} lu: {df.shape}")
        except Exception as e:
            logger.error(f"Erreur lors de la lecture de {csv_file}: {e}")
            continue

    # Concaténer les DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        # Déplacer la colonne 'Date' en première position
        cols = ['Date', 'Date d\'échéance', 'Transaction', 'Taux moyen pondéré', 'Date de la valeur']
        combined_df = combined_df[cols]

        # Sauvegarder le fichier combiné
        output_path = os.path.join(input_dir, output_file)
        combined_df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        logger.info(f"Fichier combiné sauvegardé: {output_path} ({combined_df.shape})")
        return True
    else:
        logger.warning("Aucun fichier CSV valide n'a pu être concatené.")
        return False


if __name__ == "__main__":
    concatenate_bkam_taux_csvs()