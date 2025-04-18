# Scrap_data_CDG - Application de Scraping et d'Extraction de Données

## Description

`Scrap_data_CDG` est une application Python développée avec **Streamlit** pour automatiser le téléchargement, l'extraction, et le traitement de données à partir de sources variées (web, fichiers PDF, Excel, CSV, JSON, HTML). Elle est conçue pour gérer les données financières et les KPI de CDG Capital Gestion, en s'appuyant sur un fichier Excel de configuration (`Matrice KPI_Gestion_V2_03_01.xlsx`). L'application propose trois pages principales :

1. **Gestion des Sources** : Permet de visualiser, modifier, et télécharger des fichiers à partir de sources configurées.
2. **Analyse et Extraction des Données** : Offre une interface pour parser et extraire des données à partir des fichiers téléchargés, avec paramétrage des plages de titres et de données.
3. **Traitement et Insertion dans la Base de Données** : Gère l'insertion des données extraites dans une base de données (non couvert dans ce README, car fonctionnel).

Les fichiers téléchargés sont organisés dans des dossiers datés (`Downloads/MM-DD`, par exemple `Downloads/04-18`), et les paramètres d'extraction sont sauvegardés dans un fichier JSON (`source_settings.json`).

## Prérequis

- **Python 3.8+**
- **Google Chrome** (pour le téléchargement via Selenium)
- **Chromedriver** (compatible avec votre version de Chrome)
- Système d'exploitation : Windows (testé sur Windows 10/11)

### Dépendances Python

Les bibliothèques nécessaires sont listées dans `requirements.txt`. Les principales incluent :
- `streamlit` : Interface utilisateur web.
- `pandas` : Manipulation des données.
- `selenium` : Téléchargement automatisé via navigateur.
- `tabula-py` : Extraction de tableaux PDF.
- `pdfplumber` : Parsing avancé des PDF.
- `beautifulsoup4` : Parsing des fichiers HTML.
- `requests` : Téléchargements HTTP.
- `openpyxl` : Lecture/écriture de fichiers Excel.

## Installation

1. **Cloner le dépôt** :
   ```bash
   git clone <URL_DU_DÉPÔT>
   cd Scrap_data_CDG

Créer un environnement virtuel (recommandé) :
bash

python -m venv venv
.\venv\Scripts\activate  # Windows

Installer les dépendances :
bash

pip install -r requirements.txt

Installer Chromedriver :
Téléchargez la version de Chromedriver correspondant à votre version de Google Chrome depuis ici.

Placez chromedriver.exe dans C:/chromedriver/ (ou mettez à jour CHROMEDRIVER_PATH dans src/config.py si nécessaire).

Configurer le fichier Excel :
Assurez-vous que le fichier Matrice KPI_Gestion_V2_03_01.xlsx est disponible à l'emplacement C:/Users/Julien/OneDrive/Documents/CDG Capital Gestion/.

Ce fichier doit contenir les colonnes suivantes :
Source (identifiant unique)

Type d'extraction (1 pour HTTP, 2 pour Selenium, 3 pour HTML)

URL

XPath (pour Selenium)

Donnée

Commentaires

Statut Excel

Nom de la source (colonne 7)

Structure du Projet

Scrap_data_CDG/
├── Downloads/                # Dossiers de téléchargement datés (ex. 04-18)
├── src/                      # Code source
│   ├── config.py             # Configuration (chemins, constantes)
│   ├── downloader.py         # Logique de téléchargement (HTTP, Selenium, HTML)
│   ├── parser.py             # Parsing des fichiers (PDF, Excel, CSV, JSON, HTML)
│   ├── utils.py              # Fonctions utilitaires (lecture/écriture Excel)
│   ├── manage_sources_ui.py  # Page 1 : Gestion des sources
│   ├── extract_ui.py         # Page 2 : Analyse et extraction
│   └── <fichier_traitement>  # Page 3 : Traitement et insertion (non précisé)
├── main.py                   # Point d'entrée Streamlit
├── requirements.txt          # Dépendances Python
├── source_settings.json      # Paramètres d'extraction sauvegardés
└── README.md                 # Ce fichier

Utilisation
Lancer l'application :
bash

streamlit run main.py

Cela ouvre l'interface dans votre navigateur par défaut (généralement http://localhost:8501).

Page 1 : Gestion des Sources :
Visualisez et modifiez les sources dans un tableau éditable.

Sauvegardez les modifications dans le fichier Excel.

Lancez le téléchargement des fichiers pour toutes les sources ou relancez les téléchargements en erreur.

Les fichiers sont sauvegardés dans Downloads/MM-DD (par exemple, Downloads/04-18).

Page 2 : Analyse et Extraction des Données :
Sélectionnez une source parmi toutes celles définies dans le fichier Excel.

Choisissez une date d'extraction (par défaut : date du jour).

Si aucune donnée n'est disponible pour la source à la date sélectionnée, un message d'avertissement s'affiche.

Parsez les fichiers téléchargés (PDF, Excel, CSV, JSON, HTML) et visualisez les tableaux dans "Contenu brut".

Paramétrez les plages de titres et de données, puis sauvegardez les paramètres dans source_settings.json.

Visualisez les données extraites dans "Titres extraits" et "Données extraites".

Page 3 : Traitement et Insertion dans la Base de Données :
Cette page est fonctionnelle et permet l'insertion des données extraites dans une base de données (détails non fournis).

Fonctionnalités principales
Téléchargement automatisé :
Supporte les téléchargements HTTP (requests), via Selenium (chromedriver), et l'extraction de tableaux HTML.

Gestion des erreurs avec possibilité de relance des téléchargements échoués.

Extraction flexible :
Parsing des fichiers PDF (via tabula-py et pdfplumber), Excel, CSV, JSON, et HTML.

Paramétrage des plages de titres et de données pour une extraction personnalisée.

Interface utilisateur :
Interface Streamlit intuitive avec tableaux éditables et mise à jour en temps réel.

Sélecteur de date pour charger les fichiers d'une date spécifique.

Sauvegarde des paramètres :
Les configurations d'extraction sont sauvegardées dans source_settings.json.

Problèmes connus
Conflits de fichiers existants :
Lors du téléchargement, si un fichier existe déjà dans le répertoire de destination, une erreur [WinError 183] peut survenir. Une relance des téléchargements en erreur résout généralement ce problème.

Dépendance à Chromedriver :
Assurez-vous que la version de Chromedriver correspond à celle de votre navigateur Chrome.

Contribution
Pour contribuer au projet :
Forkez le dépôt.

Créez une branche pour vos modifications (git checkout -b feature/nouvelle-fonctionnalite).

Commitez vos changements (git commit -m "Ajout de nouvelle fonctionnalité").

Poussez votre branche (git push origin feature/nouvelle-fonctionnalite).

Ouvrez une Pull Request.

Contact
Pour toute question ou problème, contactez l'équipe de développement à <julien.jolly@gmail.com> ou ouvrez une issue sur le dépôt.
Licence
Ce projet est sous licence MIT (voir fichier LICENSE pour plus de détails).

