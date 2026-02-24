import pandas as pd
import os


def extract_csvs(folder_path: str) -> dict[str, pd.DataFrame]:
    dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            path = os.path.join(folder_path, file)
            df = pd.read_csv(path)
            nome = file.replace(".csv", "")
            dfs[nome] = df
    return dfs