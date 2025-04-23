# main.py
import sys
import os

import bs4
import soupsieve
import lxml
import requests
import selenium
import pdfplumber
import tabula
import sqlalchemy
import numpy
import pandas
import openpyxl

print("PATHS :", sys.path)

# Ajout dynamique du chemin vers src pour les imports
if getattr(sys, 'frozen', False):
    # Cas d'exécution via PyInstaller
    base_path = sys._MEIPASS
else:
    # Cas normal (développement)
    base_path = os.path.dirname(os.path.abspath(__file__))

# Ajouter src/ au sys.path
src_path = os.path.join(base_path, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import streamlit as st
from src.downloader import get_sources
from src.extract_ui import extract_section
from src.manage_sources_ui import manage_sources_section
from src.list_sources_ui import list_sources_section
import cli  # Importer le module cli.py

def run_streamlit():
    """Lance l'interface Streamlit."""
    st.set_page_config(page_title="Téléchargement de Fichiers", layout="wide")

    st.sidebar.title("Menu")
    option = st.sidebar.selectbox("Choisir une action", [
        "Téléchargement des fichiers",
        "Analyse et Extraction",
        "Traitement et Insertion dans la Base de Données"
    ])

    st.title("Téléchargement et traitement des sources externes")

    sources = get_sources()

    if option == "Téléchargement des fichiers":
        manage_sources_section(sources)
    elif option == "Analyse et Extraction":
        extract_section()
    elif option == "Traitement et Insertion dans la Base de Données":
        list_sources_section()

def main():
    if len(sys.argv) > 1 and sys.argv[1].lower() == "streamlit":
        print("Lancement de l'interface Streamlit...")
        run_streamlit()
    else:
        print("Lancement du mode CLI...")
        cli.main()

if __name__ == "__main__":
    main()
