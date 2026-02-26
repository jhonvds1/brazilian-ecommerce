# Módulos responsáveis por cada etapa do pipeline (ETL)
from src.extract.extract import extract_csvs
from src.transform.transform import transform_all
import papermill as pm
from src.load.load import run_load
from pathlib import Path

# Biblioteca para registro de eventos e monitoramento do pipeline
import logging

# Configuração global de logging (nível INFO para acompanhar execução do ETL)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    """
        Orquestra o pipeline de dados (ETL).

        Fluxo:
        1. Extração dos dados brutos (CSV)
        2. Transformação em tabelas dimensionais e fato
        3. Carga dos dados no BigQuery
    """
    try:
        logging.info("Iniciando pipeline de dados")

        # Extrai todos os CSVs do diretório base e consolida em estruturas brutas
        raw_data = extract_csvs("data/")

        # Aplica regras de negócio e modelagem dimensional (dimensões e fato)
        transformed_data = transform_all(raw_data)

        # Realiza a carga final no Data Warehouse (BigQuery)
        run_load(transformed_data)

        # caminho relativo ao main.py
        notebook_path = Path(__file__).resolve().parent.parent / "notebooks" / "visualizacao.ipynb"
        logging.info("Executando Notebook")
        pm.execute_notebook(
            str(notebook_path),
            str(notebook_path)  # sobrescreve o próprio notebook
        )
        logging.info("Notebook executado com Sucesso")


        logging.info("Pipeline finalizado com sucesso!")
    except:
        logging.exception("Falha critica no Pipeline")
        raise

if __name__ == "__main__":
    # Permite execução direta do pipeline via linha de comando
    run_pipeline()