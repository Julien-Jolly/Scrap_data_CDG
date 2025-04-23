# run_streamlit_launcher.py
import subprocess
import tempfile
import shutil
import os
import sys

def main():
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))

    # Fichiers à copier
    source_main = os.path.join(base_path, 'main.py')
    source_src = os.path.join(base_path, 'src')

    # Créer le répertoire temporaire
    temp_dir = tempfile.mkdtemp()
    target_main = os.path.join(temp_dir, 'main.py')
    target_src = os.path.join(temp_dir, 'src')

    # Copier main.py et le dossier src
    shutil.copy(source_main, target_main)
    shutil.copytree(source_src, target_src)

    # Lancer Streamlit depuis le main.py copié
    subprocess.run(["streamlit", "run", target_main, "streamlit"])

    shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    main()