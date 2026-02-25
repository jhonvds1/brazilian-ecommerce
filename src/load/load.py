from src.transform.transform import transform_all
import pandas as pd
from google.cloud import bigquery

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE" 
)

def create_tables()->dict:
    df = transform_all()
    dfs = {}
    dfs['df_dim_product'] = df['olist_products_dataset'][['product_id', 'product_category_name']]
    dfs['df_dim_time'] = df['olist_orders_dataset'][['time_id', 'date', 'year', 'month', 'day']]
    dfs['df_dim_review'] = df['olist_order_reviews_dataset'][['review_id', 'review_score']]
    dfs['df_dim_seller'] = df['olist_sellers_dataset'][['seller_id', 'seller_city']]
    dfs['df_order_review'] = pd.merge(df['olist_order_items_dataset'], df['olist_order_reviews_dataset'], on='order_id')
    dfs['df_order_review'] = pd.merge(dfs['df_order_review'], df['olist_orders_dataset'], on='order_id')
    dfs['df_fact_order'] = dfs['df_order_review'][['order_id', 'product_id', 'seller_id', 'time_id' ,'review_id', 'price']]
    return dfs


def load_df_to_bq(table: pd.DataFrame, table_id: str):
    client = bigquery.Client(project="brazilian-ecommerce-488418")
    job = client.load_table_from_dataframe(table, table_id)
    job.result()
    print(f"Dados carregados para {table_id}")

def run_load():
    tables = create_tables()
    load_df_to_bq(tables['df_dim_product'], "brazilian-ecommerce-488418.ecommerce_dw.dim_product")
    load_df_to_bq(tables['df_dim_seller'], "brazilian-ecommerce-488418.ecommerce_dw.dim_seller")
    load_df_to_bq(tables['df_dim_time'], "brazilian-ecommerce-488418.ecommerce_dw.dim_time")
    load_df_to_bq(tables['df_dim_review'], "brazilian-ecommerce-488418.ecommerce_dw.dim_review")
    load_df_to_bq(tables['df_fact_order'], "brazilian-ecommerce-488418.ecommerce_dw.fact_order")

run_load()

