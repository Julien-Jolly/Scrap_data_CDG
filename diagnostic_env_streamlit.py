import sys
import os
import importlib.util
from pathlib import Path

def check_module(module_name):
    try:
        __import__(module_name)
        return f"✅ Module {module_name} OK"
    except ImportError as e:
        return f"❌ Module {module_name} introuvable ({e})"

def check_write_permission(path):
    try:
        test_file = os.path.join(path, "test_write_permission.txt")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        return f"✅ Écriture possible dans {path}"
    except Exception as e:
        return f"❌ Écriture impossible dans {path} ({e})"

def main():
    print("=== ENVIRONNEMENT EXÉCUTABLE (STREAMLIT) ===")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")
    print(f"Frozen mode: {getattr(sys, 'frozen', False)}")
    print(f"MEIPASS path (si frozen): {getattr(sys, '_MEIPASS', 'Non défini')}")
    print()

    print("=== CHEMINS DE MODULES ===")
    for p in sys.path:
        print(p)
    print()

    print("=== TEST IMPORTS CRITIQUES ===")
    modules = [
        "requests", "certifi", "bs4", "bs4.element", "soupsieve", "selenium",
        "selenium.webdriver", "pdfplumber", "openpyxl", "sqlalchemy", "pandas", "numpy"
    ]
    for mod in modules:
        print(check_module(mod))
    print()

    print("=== CHEMIN DEST_PATH TEST ===")
    try:
        from src.config import DEST_PATH
        print(f"DEST_PATH depuis config : {DEST_PATH}")
        print(check_write_permission(DEST_PATH))
    except Exception as e:
        print(f"❌ Erreur lors de l'import ou du test de DEST_PATH : {e}")

    print("\n=== FIN DU DIAGNOSTIC ===")
    input("Appuyez sur Entrée pour fermer.")

if __name__ == "__main__":
    main()
