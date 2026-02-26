import pandas as pd
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'

)

transform_logger = logging.getLogger("TRANSFORM")

def transform_orders(df: pd.DataFrame)->pd.DataFrame:
    transform_logger.info(f"[orders_dataset] linhas recebidas na transformacao: {len(df)}")
    transform_logger.warning(f"Nulos por coluna:\n{df.isna().sum()}")
    before = len(df)
    df = df.dropna().copy()
    transform_logger.info(f"Dropna aplicado: {before - len(df)} linhas removidas")
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[ns]')
    df['year'] = df['order_purchase_timestamp'].dt.year
    df['month'] = df['order_purchase_timestamp'].dt.month
    df['day'] = df['order_purchase_timestamp'].dt.day
    transform_logger.info("Criadas colunas de tempo: year, month, day")
    df = df.reset_index(drop=True)
    df['time_id'] = df.index + 1
    transform_logger.info("Criada coluna time_id")
    df = df.rename(columns={'order_purchase_timestamp': 'date'})
    transform_logger.info("coluna order_purchase_timestamp renomeada para: date")
    transform_logger.info(f"[orders_dataset] linhas apos limpeza: {len(df)}")
    return df

def transform_order_reviews(df: pd.DataFrame)->pd.DataFrame:
    transform_logger.info(f"[order_reviews] linhas recebidas na transformacao: {len(df)}")
    before = len(df)
    df = df.drop_duplicates(subset='review_id').copy()
    transform_logger.info(f"valores duplicados removidos: {before - len(df)}")
    transform_logger.info(f"[order_reviews] Linhas finais após deduplicação: {len(df)}")
    
    return df

def transform_products(df: pd.DataFrame)->pd.DataFrame:
    transform_logger.info(f"[products_dataset] linhas recebidas na transformacao: {len(df)}")
    df = df.drop(columns=['product_name_lenght', 'product_description_lenght', 'product_photos_qty']).copy()
    transform_logger.info("drop de colunas: product_name_lenght, product_description_lenght, product_photos_qty")
    transform_logger.info(f"[products_dataset] Linhas finais: {len(df)}")
    return df

def transform_sellers(df: pd.DataFrame)->pd.DataFrame:
    transform_logger.info(f"[sellers_dataset] linhas recebidas na transformacao: {len(df)}")
    df = df.drop(columns='seller_zip_code_prefix').copy()
    transform_logger.info("drop de colunas: seller_zip_code_prefix")
    transform_logger.info(f"[sellers_dataset] Linhas finais: {len(df)}")
    return df

def create_tables(df: pd.DataFrame) -> dict:
    transform_logger.info("Criando tabelas de dimensões e fato")
    try:
        dfs = {}

        # Dim Product
        dfs['df_dim_product'] = df['olist_products_dataset'][['product_id', 'product_category_name']]
        transform_logger.info(f"[dim_product] Linhas: {len(dfs['df_dim_product'])}")

        # Dim Time
        dfs['df_dim_time'] = df['olist_orders_dataset'][['time_id', 'date', 'year', 'month', 'day']]
        transform_logger.info(f"[dim_time] Linhas: {len(dfs['df_dim_time'])}")

        # Dim Review
        dfs['df_dim_review'] = df['olist_order_reviews_dataset'][['review_id', 'review_score']]
        transform_logger.info(f"[dim_review] Linhas: {len(dfs['df_dim_review'])}")

        # Dim Seller
        dfs['df_dim_seller'] = df['olist_sellers_dataset'][['seller_id', 'seller_city']]
        transform_logger.info(f"[dim_seller] Linhas: {len(dfs['df_dim_seller'])}")

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

        transform_logger.info(f"[order_review] Linhas após joins: {len(dfs['df_order_review'])}")

        dfs['df_fact_order'] = dfs['df_order_review'][
            ['order_id', 'product_id', 'seller_id', 'time_id', 'review_id', 'price']
        ]

        transform_logger.info(f"[fact_order] Linhas: {len(dfs['df_fact_order'])}")
        transform_logger.info("Criação das tabelas concluída com sucesso")

        return dfs
    except(KeyError, ValueError) as e:
        transform_logger.error(f"Erro ao montar tabelas do DW: {e}")
        raise


def transform_all(df: dict)->pd.DataFrame:
    transform_logger.info("Iniciando processo de transformacao")
    df['olist_orders_dataset'] = transform_orders(df['olist_orders_dataset'])
    df['olist_order_reviews_dataset'] = transform_order_reviews(df['olist_order_reviews_dataset'])
    df['olist_products_dataset'] = transform_products(df['olist_products_dataset'])
    df['olist_sellers_dataset'] = transform_sellers(df['olist_sellers_dataset'])
    df = create_tables(df)
    transform_logger.info("Finalizando processo de transformacao")
    return df