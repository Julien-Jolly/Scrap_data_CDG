from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd



fichier_excel = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"


df = pd.read_excel(fichier_excel, index_col=0)



def rechercher_valeur(index):
    try:
        valeur = df.iloc[df.index.get_loc(index), 0]
        return valeur
    except KeyError:
        return "Index introuvable"





if __name__ == "__main__":
    index_recherche = "Source2"
    valeur = rechercher_valeur(index_recherche)
    print(f"La valeur associée à l'index '{index_recherche}' est : {valeur}")



    # with sync_playwright() as p:
    #     browser = p.chromium.launch(headless=False)
    #     page = browser.new_page()
    #
    #
    #
    #     browser.close()