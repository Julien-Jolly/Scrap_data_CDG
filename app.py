from playwright.sync_api import sync_playwright
import requests
from datetime import datetime
import pandas as pd
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

actual_date = datetime.now()
year = actual_date.strftime("%Y")
month = actual_date.strftime("%m")
day = actual_date.strftime("%d")

source_file = "C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Matrice KPI_Gestion_V2_03_01 (2) (1).xlsx"

dest_path = (
    f"C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/Downloads/{month}-{day}"
)
os.makedirs(dest_path, exist_ok=True)


df = pd.read_excel(source_file, sheet_name="Source sans doub")


columns = df.columns.tolist()


def simple_dl():
    global e
    final_url = row[columns[2]].format(year=year, month=month, day=day)
    nom_fichier = os.path.basename(final_url)
    #print(f"Récupération de : {final_url}")
    try:
        response = requests.get(final_url)
        response.raise_for_status()
        #print(f"démarrage de la récupération du fichier : {nom_fichier}")

        with open(fichier_destination, "wb") as fichier:
            fichier.write(response.content)
        #print(f"Fichier téléchargé et enregistré sous : {nom_fichier}")
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors du téléchargement : {e}")
        errors.append(row[columns[0]])
    return errors


def driver_dl():
    global e
    url = row[columns[2]]
    if row[columns[4]] == 1:
        driver_path = "C:/Users/Julien/Downloads/chromedriver-win64/chromedriver-win64/chromedriver.exe"
        service = Service(driver_path)

        options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": os.path.abspath(
                dest_path
            ),
            "download.prompt_for_download": False,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(service=service, options=options)

        try:
            driver.get(url)
            element=""
            xpath = row[columns[3]]
            while not element:
                element = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, xpath))
                )
            element.click()

            time.sleep(5)


        except Exception as e:
            print("Une erreur s'est produite :")
            print(f"Type : {type(e).__name__}")
            print(f"Message : {str(e)}")
            print("Stack trace :", e)
            errors.append(row[columns[0]])
            print("\n---------------------------\n")
            return errors

        finally:
            driver.quit()

    else:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = browser.new_context()
                page = context.new_page()
                page.goto(url)
                code=row[columns[3]]


                with page.expect_download() as download_info:
                    exec(code)
                download = download_info.value


                file_name = download.suggested_filename
                destination_path = os.path.join(dest_path, file_name)
                download.save_as(destination_path)

                context.close()
                browser.close()

        except Exception as e:
            print("Une erreur s'est produite :")
            print(f"Type : {type(e).__name__}")
            print(f"Message : {str(e)}")
            print("Stack trace :", e)
            errors.append(row[columns[0]])
            print("\n---------------------------\n")
            return errors


if __name__ == "__main__":
    errors = []
    for index, row in df.iterrows():
        if row[columns[1]] == 1:
            print(f"Index : {row[columns[0]]} | Valeur adjacente : {row[columns[2]]}")
            print(f"demarrage extraction {row[columns[0]]} - {row[columns[2]]}")
            errors = simple_dl()
            print(f"fin d'extraction {row[columns[0]]} - {row[columns[2]]}")
            print("\n---------------------------\n")

        if row[columns[1]] == 2:
            print(f"demarrage extraction {row[columns[0]]} - {row[columns[2]]}")
            errors = driver_dl()
            print(f"fin d'extraction {row[columns[0]]} - {row[columns[2]]}")
            print("\n---------------------------\n")

        else:
            pass

    if not errors:
        print("Tous les fichiers ont bien été téléchargés")

    else:
        print("erreur de telechargement pour l(es) index :")
        for e in errors:
            print(e)
