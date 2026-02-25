import pandas as pd
from google.cloud import bigquery
import logging


job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE" 
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

load_logger = logging.getLogger("LOAD")

def create_tables(df: pd.DataFrame) -> dict:
    load_logger.info("Criando tabelas de dimensões e fato")
    try:
        dfs = {}

        # Dim Product
        dfs['df_dim_product'] = df['olist_products_dataset'][['product_id', 'product_category_name']]
        load_logger.info(f"[dim_product] Linhas: {len(dfs['df_dim_product'])}")

        # Dim Time
        dfs['df_dim_time'] = df['olist_orders_dataset'][['time_id', 'date', 'year', 'month', 'day']]
        load_logger.info(f"[dim_time] Linhas: {len(dfs['df_dim_time'])}")

        # Dim Review
        dfs['df_dim_review'] = df['olist_order_reviews_dataset'][['review_id', 'review_score']]
        load_logger.info(f"[dim_review] Linhas: {len(dfs['df_dim_review'])}")

        # Dim Seller
        dfs['df_dim_seller'] = df['olist_sellers_dataset'][['seller_id', 'seller_city']]
        load_logger.info(f"[dim_seller] Linhas: {len(dfs['df_dim_seller'])}")

        # Fact Order (joins)
        dfs['df_order_review'] = pd.merge(
            df['olist_order_items_dataset'],
            df['olist_order_reviews_dataset'],
            on='order_id'
        )

        dfs['df_order_review'] = pd.merge(
            dfs['df_order_review'],
            df['olist_orders_dataset'],
            on='order_id'
        )

        load_logger.info(f"[order_review] Linhas após joins: {len(dfs['df_order_review'])}")

        dfs['df_fact_order'] = dfs['df_order_review'][
            ['order_id', 'product_id', 'seller_id', 'time_id', 'review_id', 'price']
        ]

        load_logger.info(f"[fact_order] Linhas: {len(dfs['df_fact_order'])}")
        load_logger.info("Criação das tabelas concluída com sucesso")

        return dfs
    except(KeyError, ValueError) as e:
        load_logger.error(f"Erro ao montar tabelas do DW: {e}")
        raise

# TODO: Passar o create table para o transform


def load_df_to_bq(table: pd.DataFrame, table_id: str):
    load_logger.info(f"[{table_id}] Iniciando carga - linhas: {len(table)}")
    client = bigquery.Client(project="brazilian-ecommerce-488418")
    job = client.load_table_from_dataframe(table, table_id)
    job.result()
    load_logger.info(f"[{table_id}] Carga concluída com sucesso")

def run_load(df: pd.DataFrame):
    load_logger.info("Iniciando o processo de load das tabelas!")
    tables = create_tables(df)
    load_df_to_bq(tables['df_dim_product'], "brazilian-ecommerce-488418.ecommerce_dw.dim_product")
    load_df_to_bq(tables['df_dim_seller'], "brazilian-ecommerce-488418.ecommerce_dw.dim_seller")
    load_df_to_bq(tables['df_dim_time'], "brazilian-ecommerce-488418.ecommerce_dw.dim_time")
    load_df_to_bq(tables['df_dim_review'], "brazilian-ecommerce-488418.ecommerce_dw.dim_review")
    load_df_to_bq(tables['df_fact_order'], "brazilian-ecommerce-488418.ecommerce_dw.fact_order")
    load_logger.info("Finalizando o processo de load das tabelas!")


