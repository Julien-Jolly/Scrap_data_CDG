from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import os

actual_date = datetime.now()
year = actual_date.strftime("%Y")
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")

source_file = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"

dest_path = (
    f"C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Downloads/{month}-{day}"
)
os.makedirs(dest_path, exist_ok=True)


df = pd.read_excel(source_file, sheet_name="Source")


columns = df.columns.tolist()


if __name__ == "__main__":
    errors = []
    for index, row in df.iterrows():
        if row[columns[1]] == 1:  # Vérifie si la deuxième colonne vaut 1
            print(f"Index : {row[columns[0]]} | Valeur adjacente : {row[columns[2]]}")
            final_url = row[columns[2]].format(year=year, month=month, day=day)
            nom_fichier = os.path.basename(final_url)
            fichier_destination = os.path.join(dest_path, nom_fichier)
            print(f"Récupération de : {final_url}")

            try:
                response = requests.get(final_url)
                response.raise_for_status()
                print(f"démarrage de la récupération du fichier : {nom_fichier}")

                with open(fichier_destination, "wb") as fichier:
                    fichier.write(response.content)
                print(f"Fichier téléchargé et enregistré sous : {nom_fichier}")
            except requests.exceptions.RequestException as e:
                print(f"Erreur lors du téléchargement : {e}")
                errors.append(row[columns[0]])

            print("\n---------------------------\n")

    if not errors:
        print("Tous les fichiers ont bien été téléchargés")

    else:
        print("erreur de telechargement pour l(es) index :")
        for e in errors:
            print(e)

            # with sync_playwright() as p:
            #     browser = p.chromium.launch(headless=False)
            #     page = browser.new_page()
            #     page.goto(url)
            #     browser.close()
