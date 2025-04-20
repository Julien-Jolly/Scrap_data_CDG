# main.py
import streamlit as st
from src.downloader import get_sources
from src.extract_ui import extract_section
from src.manage_sources_ui import manage_sources_section
from src.list_sources_ui import list_sources_section

def main():
    # Configuration de la page
    st.set_page_config(page_title="Téléchargement de Fichiers", layout="wide")

    # Sidebar
    st.sidebar.title("Menu")
    option = st.sidebar.selectbox("Choisir une action", [
        "Téléchargement des fichiers",
        "Analyse et Extraction",
        "Traitement et Insertion dans la Base de Données"
    ])

    # Corps de la page
    st.title("Téléchargement et traitement des sources externes")

    # Charger les sources
    sources = get_sources()

    if option == "Téléchargement des fichiers":
        manage_sources_section(sources)
    elif option == "Analyse et Extraction":
        extract_section()
    elif option == "Traitement et Insertion dans la Base de Données":
        list_sources_section()

if __name__ == "__main__":
    main()