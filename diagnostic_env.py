import os
import sys

print("=== ENVIRONNEMENT PYTHON ===")
print("Python executable:", sys.executable)
print("Python version:", sys.version)
print("Frozen mode:", getattr(sys, 'frozen', False))
print("MEIPASS path (si frozen):", getattr(sys, '_MEIPASS', "Non défini"))

print("\n=== CHEMINS DE MODULES ===")
for p in sys.path:
    print(p)

print("\n=== TEST IMPORTS CRITIQUES ===")
modules = ["requests", "certifi", "bs4", "bs4.element", "selenium", "pdfplumber", "openpyxl"]
for mod in modules:
    try:
        __import__(mod)
        print(f"✅ Module {mod} OK")
    except ImportError as e:
        print(f"❌ Module {mod} NON DISPONIBLE ({str(e)})")

print("\n=== CHEMIN DEST_PATH TEST ===")
try:
    from src.config import DEST_PATH
    print(f"DEST_PATH depuis config : {DEST_PATH}")
    os.makedirs(DEST_PATH, exist_ok=True)
    test_file = os.path.join(DEST_PATH, "test.txt")
    with open(test_file, "w") as f:
        f.write("test")
    print(f"✅ Écriture test réussie dans {DEST_PATH}")
except Exception as e:
    print(f"❌ Erreur sur DEST_PATH : {e}")

print("\n=== FIN DU DIAGNOSTIC ===")
input("Appuyez sur Entrée pour fermer.")
