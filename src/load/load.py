import pandas as pd
from google.cloud import bigquery
import logging
from google.api_core.exceptions import GoogleAPIError


job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE" 
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

load_logger = logging.getLogger("LOAD")

def load_df_to_bq(table: pd.DataFrame, table_id: str):
    load_logger.info(f"[{table_id}] Iniciando carga - linhas: {len(table)}")
    client = bigquery.Client(project="brazilian-ecommerce-488418")
    try:
        job = client.load_table_from_dataframe(table, table_id)
        job.result()
        load_logger.info(f"[{table_id}] Carga concluída com sucesso")
    except GoogleAPIError as e:
        load_logger.error(f"[{table_id}] Erro na API do BigQuery: {e}")
        raise
    except ValueError as e:
        load_logger.error(f"[{table_id}] Erro no DataFrame enviado: {e}")
        raise
        

def run_load(tables: pd.DataFrame):
    load_logger.info("Iniciando o processo de load das tabelas!")
    load_df_to_bq(tables['df_dim_product'], "brazilian-ecommerce-488418.ecommerce_dw.dim_product")
    load_df_to_bq(tables['df_dim_seller'], "brazilian-ecommerce-488418.ecommerce_dw.dim_seller")
    load_df_to_bq(tables['df_dim_time'], "brazilian-ecommerce-488418.ecommerce_dw.dim_time")
    load_df_to_bq(tables['df_dim_review'], "brazilian-ecommerce-488418.ecommerce_dw.dim_review")
    load_df_to_bq(tables['df_fact_order'], "brazilian-ecommerce-488418.ecommerce_dw.fact_order")
    load_logger.info("Finalizando o processo de load das tabelas!")


