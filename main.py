# main.py
import streamlit as st
from src.downloader import get_sources
from src.download_ui import download_section
from src.extract_ui import extract_section
from src.manage_sources_ui import manage_sources_section
from src.list_sources_ui import list_sources_section  # Importer la nouvelle section

def main():
    # Configuration de la page
    st.set_page_config(page_title="Téléchargement de Fichiers", layout="wide")

    # Sidebar
    st.sidebar.title("Menu")
    option = st.sidebar.selectbox("Choisir une action", [
        "Téléchargement des fichiers",
        "Analyse et Extraction",
        "Liste des Sources et DataFrames"  # Nouvelle option
    ])

    # Corps de la page
    st.title("Téléchargement de Fichiers")

    # Charger les sources pour la section "Téléchargement des fichiers"
    sources = get_sources()

    if option == "Téléchargement des fichiers":
        download_section(sources)
        manage_sources_section()
    elif option == "Analyse et Extraction":
        extract_section()
    elif option == "Liste des Sources et DataFrames":
        list_sources_section()

if __name__ == "__main__":
    main()