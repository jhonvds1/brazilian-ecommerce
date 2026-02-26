import pandas as pd
import logging

# Configuração global de logging para acompanhar as etapas de transformação
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Logger específico da etapa de transformação (ETL - Transform)
transform_logger = logging.getLogger("TRANSFORM")


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica limpeza e enriquecimento temporal no dataset de pedidos.

    Regras aplicadas:
    - Remove registros com valores nulos (garante integridade analítica)
    - Converte timestamp de compra para datetime
    - Cria colunas derivadas: year, month, day
    - Gera chave substituta (time_id) para a dimensão tempo
    - Renomeia coluna de timestamp para 'date'
    """
    transform_logger.info(f"[orders_dataset] linhas recebidas na transformacao: {len(df)}")

    # Monitoramento de qualidade: identifica quantidade de nulos por coluna
    transform_logger.warning(f"Nulos por coluna:\n{df.isna().sum()}")

    before = len(df)

    # Remove registros incompletos para evitar inconsistências nas análises
    df = df.dropna().copy()
    transform_logger.info(f"Dropna aplicado: {before - len(df)} linhas removidas")

    # Conversão do timestamp para tipo datetime para permitir manipulação temporal
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[ns]')

    # Criação de atributos temporais para suportar análises por período
    df['year'] = df['order_purchase_timestamp'].dt.year
    df['month'] = df['order_purchase_timestamp'].dt.month
    df['day'] = df['order_purchase_timestamp'].dt.day
    transform_logger.info("Criadas colunas de tempo: year, month, day")

    # Reinicia o índice após limpeza para gerar uma chave substituta sequencial
    df = df.reset_index(drop=True)

    # Geração de surrogate key para a dimensão tempo
    df['time_id'] = df.index + 1
    transform_logger.info("Criada coluna surrogate key: time_id")

    # Renomeia coluna para padronização semântica no DW
    df = df.rename(columns={'order_purchase_timestamp': 'date'})
    transform_logger.info("Coluna order_purchase_timestamp renomeada para: date")

    transform_logger.info(f"[orders_dataset] linhas apos limpeza: {len(df)}")
    return df


def transform_order_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove avaliações duplicadas garantindo unicidade por review_id.

    Esta etapa assegura consistência na dimensão de reviews,
    evitando múltiplos registros para a mesma avaliação.
    """
    transform_logger.info(f"[order_reviews] linhas recebidas na transformacao: {len(df)}")

    before = len(df)

    # Remove registros duplicados mantendo apenas uma avaliação por review_id
    df = df.drop_duplicates(subset='review_id').copy()

    transform_logger.info(f"Valores duplicados removidos: {before - len(df)}")
    transform_logger.info(f"[order_reviews] Linhas finais após deduplicação: {len(df)}")

    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas textuais não relevantes para análises dimensionais.

    Reduz volume de dados e mantém apenas atributos essenciais
    para a dimensão produto.
    """
    transform_logger.info(f"[products_dataset] linhas recebidas na transformacao: {len(df)}")

    # Remove atributos desnecessários para análise analítica
    df = df.drop(columns=[
        'product_name_lenght',
        'product_description_lenght',
        'product_photos_qty'
    ]).copy()

    transform_logger.info(
        "Removidas colunas irrelevantes: product_name_lenght, "
        "product_description_lenght, product_photos_qty"
    )
    transform_logger.info(f"[products_dataset] Linhas finais: {len(df)}")

    return df


def transform_sellers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove atributos não utilizados na dimensão de vendedores.

    Mantém apenas colunas relevantes para análises geográficas
    e relacionamento com a tabela fato.
    """
    transform_logger.info(f"[sellers_dataset] linhas recebidas na transformacao: {len(df)}")

    # Remove CEP prefixo por não ser utilizado nas análises
    df = df.drop(columns='seller_zip_code_prefix').copy()

    transform_logger.info("Coluna removida: seller_zip_code_prefix")
    transform_logger.info(f"[sellers_dataset] Linhas finais: {len(df)}")

    return df


def create_tables(df: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Cria as tabelas dimensionais e a tabela fato para o Data Warehouse.

    Dimensões:
    - dim_product: informações dos produtos
    - dim_time: atributos temporais derivados dos pedidos
    - dim_review: avaliações dos pedidos
    - dim_seller: dados dos vendedores

    Fato:
    - fact_order: relaciona pedidos, produtos, vendedores, tempo e avaliações
      contendo a métrica principal de preço.
    """
    transform_logger.info("Criando tabelas de dimensões e fato")

    try:
        dfs = {}

        # =========================
        # DIMENSÃO PRODUTO
        # Contém atributos descritivos dos produtos
        # =========================
        dfs['df_dim_product'] = df['olist_products_dataset'][
            ['product_id', 'product_category_name']
        ]
        transform_logger.info(f"[dim_product] Linhas: {len(dfs['df_dim_product'])}")

        # =========================
        # DIMENSÃO TEMPO
        # Baseada na data de compra dos pedidos
        # =========================
        dfs['df_dim_time'] = df['olist_orders_dataset'][
            ['time_id', 'date', 'year', 'month', 'day']
        ]
        transform_logger.info(f"[dim_time] Linhas: {len(dfs['df_dim_time'])}")

        # =========================
        # DIMENSÃO REVIEW
        # Representa as avaliações únicas dos pedidos
        # =========================
        dfs['df_dim_review'] = df['olist_order_reviews_dataset'][
            ['review_id', 'review_score']
        ]
        transform_logger.info(f"[dim_review] Linhas: {len(dfs['df_dim_review'])}")

        # =========================
        # DIMENSÃO SELLER
        # Informações geográficas dos vendedores
        # =========================
        dfs['df_dim_seller'] = df['olist_sellers_dataset'][
            ['seller_id', 'seller_city']
        ]
        transform_logger.info(f"[dim_seller] Linhas: {len(dfs['df_dim_seller'])}")

        # =========================
        # CONSTRUÇÃO DA TABELA FATO
        # Realiza junções para consolidar métricas e chaves dimensionais
        # =========================

        # Join entre itens do pedido e avaliações
        dfs['df_order_review'] = pd.merge(
            df['olist_order_items_dataset'],
            df['olist_order_reviews_dataset'],
            on='order_id'
        )

        # Join com pedidos para incluir a dimensão tempo
        dfs['df_order_review'] = pd.merge(
            dfs['df_order_review'],
            df['olist_orders_dataset'],
            on='order_id'
        )

        transform_logger.info(
            f"[order_review] Linhas após joins: {len(dfs['df_order_review'])}"
        )

        # Seleção final das colunas que compõem a tabela fato
        dfs['df_fact_order'] = dfs['df_order_review'][
            ['order_id', 'product_id', 'seller_id', 'time_id', 'review_id', 'price']
        ]

        transform_logger.info(f"[fact_order] Linhas: {len(dfs['df_fact_order'])}")
        transform_logger.info("Criação das tabelas dimensionais e fato concluída")

        return dfs

    except (KeyError, ValueError) as e:
        transform_logger.error(f"Erro ao montar tabelas do Data Warehouse: {e}")
        raise


def transform_all(df: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Orquestra todas as transformações do pipeline.

    Fluxo:
    1. Limpeza e enriquecimento do dataset de pedidos
    2. Deduplicação das avaliações
    3. Redução e padronização da dimensão produto
    4. Ajuste da dimensão vendedor
    5. Criação das tabelas dimensionais e da tabela fato
    """
    transform_logger.info("Iniciando processo de transformacao")

    # Aplicação das transformações específicas por dataset
    df['olist_orders_dataset'] = transform_orders(df['olist_orders_dataset'])
    df['olist_order_reviews_dataset'] = transform_order_reviews(
        df['olist_order_reviews_dataset']
    )
    df['olist_products_dataset'] = transform_products(
        df['olist_products_dataset']
    )
    df['olist_sellers_dataset'] = transform_sellers(
        df['olist_sellers_dataset']
    )

    # Criação final das tabelas dimensionais e fato do DW
    df = create_tables(df)

    transform_logger.info("Finalizando processo de transformacao")
    return df