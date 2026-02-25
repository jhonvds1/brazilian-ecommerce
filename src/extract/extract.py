import pandas as pd
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

extract_logger = logging.getLogger("EXTRACT")


def extract_csvs(folder_path: str) -> dict[str, pd.DataFrame]:
    extract_logger.info('Iniciando a extracao dos dados')
    dfs = {}
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            path = os.path.join(folder_path, file)
            df = pd.read_csv(path)
            nome = file.replace(".csv", "")
            extract_logger.info(f"dados de {nome} extraidos com sucesso!")
            dfs[nome] = df
    extract_logger.info("Processo de extracao de dados finalizado")
    return dfs