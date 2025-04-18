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

python -m venv venv
.\venv\Scripts\activate  # Windows

Installer les dépendances :

pip install -r requirements.txt


Configurer le fichier Excel :
Assurez-vous que le fichier matrice_source.xlsx est disponible à l'emplacement "../Scrap_data_CDG/matrice sources.xlsx"

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

/Scrap_data_cdg /
├── /.venv/
├── /Downloads/
├── /src/
│   ├── __init__.py
│   └── config.py
│   └── download_ui.py
│   └── downloader.py
│   └── extract_ui.py
│   └── list_sources_ui.py
│   └── manage_sources_ui.py
│   └── parser.py
│   └── utils.py
├── .gitignore
├── main.py              
├── cli.py              
└── requirements.txt     
└── source_settings.json  

Utilisation
Via l'interface Streamlit
Lancer l'application :
bash

streamlit run main.py

Cela ouvre l'interface dans votre navigateur par défaut (http://localhost:8501). La première exécution peut télécharger Chromedriver automatiquement via webdriver-manager.

Page 1 : Gestion des Sources :
Visualisez et modifiez les sources dans un tableau éditable.

Sauvegardez les modifications dans le fichier Excel.

Cliquez sur "Télécharger tous les fichiers" ou "Relancer les téléchargements en erreur" pour lancer les téléchargements.

Les fichiers sont sauvegardés dans Downloads/MM-DD (par exemple, Downloads/04-18).

Page 2 : Analyse et Extraction des Données :
Sélectionnez une source et une date d'extraction (par défaut : date du jour).

Parsez les fichiers téléchargés (PDF, Excel, CSV, JSON, HTML) et visualisez les tableaux dans "Contenu brut".

Paramétrez les plages de titres et de données, puis sauvegardez dans source_settings.json.

Visualisez les données extraites dans "Titres extraits" et "Données extraites".

Un tableau en bas de page liste les sources non paramétrées avec leur type d'extraction et commentaires.

Page 3 : Traitement et Insertion dans la Base de Données :
Cette page permet l'insertion des données extraites dans une base de données SQLite (fonctionnelle, détails non fournis).

Via la ligne de commande
Téléchargement et traitement :
Télécharge les fichiers pour toutes les sources, traite les données, et insère dans la base de données SQLite.

Vérifie les changements de types ou de nature par rapport à la veille, journalisés dans logs/cli_YYYYMMDD_HHMMSS.log.

Génère un fichier de résumé dans logs/summary_YYYYMMDD_HHMMSS.log avec cinq tableaux :
Sources téléchargées

Sources non téléchargées en erreur

Sources traitées

Sources traitées insérées dans la BDD

Sources traitées non insérées dans la BDD en erreur

bash

python cli.py download_and_process --db_path database.db

Traitement seul :
Traite les fichiers déjà téléchargés (dans Downloads/MM-DD) et insère dans la base de données SQLite.

Vérifie les changements de types ou de nature, journalisés dans logs/cli_YYYYMMDD_HHMMSS.log.

Génère le fichier de résumé dans logs/summary_YYYYMMDD_HHMMSS.log.
bash

python cli.py process_only --db_path database.db

Problèmes connus
Connexion Internet : Requise pour télécharger Chromedriver via webdriver-manager lors du premier lancement.

Fichier Excel : Si SOURCE_FILE est incorrect, l'application ne chargera pas les sources. Vérifiez le chemin dans config.py.

Permissions : Assurez-vous d'avoir les droits d'écriture dans Downloads/MM-DD, logs/, et le dossier de cache de webdriver-manager (~/.wdm).

Fichiers non téléchargés : La commande process_only échouera si aucun fichier n'est présent dans Downloads/MM-DD. Exécutez d'abord download_and_process.

Sources non paramétrées : Si une source manque de paramètres dans source_settings.json, elle peut être ignorée. Consultez le tableau des sources non paramétrées dans l'interface.

Données de la veille : Les vérifications de types et de nature nécessitent des données pour la veille dans la base SQLite. Si aucune donnée n'est disponible, un message informatif est affiché.

Fichier source_settings.json : Doit être à la racine du projet (Scrap_data_CDG/) pour éviter les erreurs de chemin.

Dépendances
Python 3.8+

Google Chrome

Bibliothèques listées dans requirements.txt :

streamlit==1.31.0
pandas==2.2.0
openpyxl==3.1.2
selenium==4.17.2
requests==2.31.0
tabula-py==2.9.0
pdfplumber==0.10.3
beautifulsoup4==4.12.2
webdriver-manager==4.0.2
sqlalchemy==2.0.23

Notes
Les anomalies (changements de types ou de nature) sont informatives et n'empêchent pas l'insertion des données.

Le fichier logs/summary_YYYYMMDD_HHMMSS.log fournit un résumé clair des opérations CLI pour le suivi.



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

