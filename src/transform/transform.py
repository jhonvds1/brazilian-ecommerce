import pandas as pd


def transform_orders(df: pd.DataFrame)->pd.DataFrame:
    df = df.dropna().copy()
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[ns]')
    df['year'] = df['order_purchase_timestamp'].dt.year
    df['month'] = df['order_purchase_timestamp'].dt.month
    df['day'] = df['order_purchase_timestamp'].dt.day
    df = df.reset_index(drop=True)
    df['time_id'] = df.index + 1
    df = df.rename(columns={'order_purchase_timestamp': 'date'})
    return df

def transform_order_reviews(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop_duplicates(subset='review_id').copy()
    return df

def transform_products(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns=['product_name_lenght', 'product_description_lenght', 'product_photos_qty']).copy()
    return df

def transform_sellers(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns='seller_zip_code_prefix').copy()
    return df


def transform_all(df: dict)->pd.DataFrame:
    df['olist_orders_dataset'] = transform_orders(df['olist_orders_dataset'])
    df['olist_order_reviews_dataset'] = transform_order_reviews(df['olist_order_reviews_dataset'])
    df['olist_products_dataset'] = transform_products(df['olist_products_dataset'])
    df['olist_sellers_dataset'] = transform_sellers(df['olist_sellers_dataset'])
    return df

