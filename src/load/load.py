# Bibliotecas para manipulação de DataFrames, logging e carga no BigQuery
import pandas as pd
import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


# Configuração de escrita: substitui totalmente os dados da tabela destino
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE" 
)

# Configuração global de logging para a etapa de carga
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# logger específico para estapa de LOAD
load_logger = logging.getLogger("LOAD")

def load_df_to_bq(table: pd.DataFrame, table_id: str):
    """
    Realiza a carga de um DataFrame para uma tabela do BigQuery.

    A operação utiliza WRITE_TRUNCATE, substituindo completamente os dados existentes na tabela destino.
    """
    load_logger.info(f"[{table_id}] Iniciando carga - linhas: {len(table)}")
    client = bigquery.Client(project="brazilian-ecommerce-488418")
    try:
        job = client.load_table_from_dataframe(table, table_id, job_config=job_config)
        job.result()
        load_logger.info(f"[{table_id}] Carga concluída com sucesso")
    except GoogleAPIError as e:
        load_logger.error(f"[{table_id}] Erro na API do BigQuery: {e}")
        raise
    except ValueError as e:
        load_logger.error(f"[{table_id}] Erro no DataFrame enviado: {e}")
        raise
        

def run_load(tables: dict[str, pd.DataFrame]):
    """
    Orquestra a carga das tabelas dimensionais e fato no BigQuery.

    Args:
        tables (dict[str, pd.DataFrame]): Dicionário contendo
        as tabelas já transformadas (dimensões e fato).
    """
    load_logger.info("Iniciando o processo de load das tabelas!")
    load_df_to_bq(tables['df_dim_product'], "brazilian-ecommerce-488418.ecommerce_dw.dim_product")
    load_df_to_bq(tables['df_dim_seller'], "brazilian-ecommerce-488418.ecommerce_dw.dim_seller")
    load_df_to_bq(tables['df_dim_time'], "brazilian-ecommerce-488418.ecommerce_dw.dim_time")
    load_df_to_bq(tables['df_dim_review'], "brazilian-ecommerce-488418.ecommerce_dw.dim_review")
    load_df_to_bq(tables['df_fact_order'], "brazilian-ecommerce-488418.ecommerce_dw.fact_order")
    load_logger.info("Finalizando o processo de load das tabelas!")


