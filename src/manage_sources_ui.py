# src/manage_sources_ui.py
import streamlit as st
import pandas as pd
from src.utils import load_excel_data, save_to_excel

def manage_sources_section():
    """Affiche la section 'Gestion des Sources'."""
    st.header("Gestion des Sources")
    st.write("Modifiez les informations ci-dessous pour adapter la logique d'extraction des fichiers.")

    df = load_excel_data()
    columns = df.columns.tolist()

    edit_df = df[[columns[0], columns[1], columns[2], columns[3]]].copy()
    edit_df.columns = ["Source", "Type d'extraction", "URL", "XPath reformaté"]

    disabled_columns = {"Source": st.column_config.Column(disabled=True)}

    edited_df = st.data_editor(
        edit_df,
        hide_index=True,
        column_config=disabled_columns,
        use_container_width=True,
        height=400,
        num_rows="fixed"
    )

    if st.button("Sauvegarder les modifications"):
        df[columns[1]] = edited_df["Type d'extraction"]
        df[columns[2]] = edited_df["URL"]
        df[columns[3]] = edited_df["XPath reformaté"]
        save_to_excel(df)
        st.success("Les modifications ont été sauvegardées avec succès dans le fichier Excel !")