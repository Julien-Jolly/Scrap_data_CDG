import os
import glob
import pandas as pd
import logging
from datetime import datetime


def concatenate_bkam_csvs(input_dir="bkam_exchange_rates", output_file="combined_exchange_rates_2022_2025.csv"):
    """
    Liste et concatène les fichiers CSV du répertoire de téléchargement en un seul fichier.

    Args:
        input_dir (str): Répertoire contenant les fichiers CSV (par défaut: 'bkam_exchange_rates').
        output_file (str): Nom du fichier CSV combiné (par défaut: 'combined_exchange_rates_2022_2025.csv').

    Returns:
        bool: True si la concaténation réussit, False sinon.
    """
    # Configurer le logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('concatenate_bkam_csvs.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    # Vérifier si le répertoire existe
    if not os.path.exists(input_dir):
        logger.error(f"Le répertoire {input_dir} n'existe pas.")
        return False

    # Lister tous les fichiers CSV
    csv_pattern = os.path.join(input_dir, "exchange_rates_*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        logger.warning(f"Aucun fichier CSV trouvé dans {input_dir}.")
        return False

    logger.info(f"{len(csv_files)} fichiers CSV trouvés dans {input_dir}.")

    # Liste pour stocker les DataFrames
    all_dfs = []

    # Traiter chaque fichier CSV
    for csv_file in csv_files:
        # Extraire la date du nom de fichier (ex. exchange_rates_20220701.csv -> 2022-07-01)
        filename = os.path.basename(csv_file)
        try:
            date_str = filename.split('_')[-1].replace('.csv', '')
            date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError as e:
            logger.error(f"Impossible d'extraire la date de {filename}: {e}")
            continue

        try:
            # Lire les colonnes 'Devises' et 'Moyen', en sautant les métadonnées
            df = pd.read_csv(csv_file, sep=';', encoding='utf-8', usecols=['Devises', 'Moyen'], skiprows=3)
            df['Date'] = date
            all_dfs.append(df)
            logger.debug(f"Fichier {csv_file} lu: {df.shape}")
        except Exception as e:
            logger.error(f"Erreur lors de la lecture de {csv_file}: {e}")
            continue

    # Concaténer les DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        cols = ['Date', 'Devises', 'Moyen']
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
    concatenate_bkam_csvs()