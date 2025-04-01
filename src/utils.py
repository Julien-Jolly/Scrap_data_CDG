# src/utils.py
import pandas as pd
import concurrent.futures
from src.config import SOURCE_FILE

def run_in_thread(func, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(func, *args, **kwargs)
        return future.result()

def load_excel_data():
    df = pd.read_excel(SOURCE_FILE, sheet_name="Source sans doub", dtype=str)
    return df

def save_to_excel(df):
    with pd.ExcelWriter(SOURCE_FILE, mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name="Source sans doub", index=False)