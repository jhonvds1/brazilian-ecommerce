from src.extract.extract import extract_csvs
import pandas as pd


def transform_customers(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns='customer_zip_code_prefix')
    return df

def transform_orders(df: pd.DataFrame)->pd.DataFrame:
    df = df.dropna()
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[ns]')
    df['year_purchase'] = df['order_purchase_timestamp'].dt.year
    df['month_purchase'] = df['order_purchase_timestamp'].dt.month
    df['day_purchase'] = df['order_purchase_timestamp'].dt.day
    df = df.reset_index(drop=True)
    df['time_id'] = df.index + 1
    return df

def transform_order_items(df: pd.DataFrame)->pd.DataFrame:
    # print(df[''])
    return df



def transform_order_payments(df: pd.DataFrame)->pd.DataFrame:
    # df = df.drop_duplicates(subset='order_id') 
    return df

def transform_order_reviews(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop_duplicates(subset='review_id')
    # df = df.drop_duplicates(subset='order_id')
    return df

def transform_products(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns=['product_name_lenght', 'product_description_lenght', 'product_photos_qty'])
    return df

def transform_sellers(df: pd.DataFrame)->pd.DataFrame:
    df = df.drop(columns='seller_zip_code_prefix')
    return df


def transform_all()->pd.DataFrame:
    df = extract_csvs("data/")
    df['olist_customers_dataset'] = transform_customers(df['olist_customers_dataset'])
    df['olist_orders_dataset'] = transform_orders(df['olist_orders_dataset'])
    df['olist_order_items_dataset'] = transform_order_items(df['olist_order_items_dataset'])
    df['olist_order_payments_dataset'] = transform_order_payments(df['olist_order_payments_dataset'])
    df['olist_order_reviews_dataset'] = transform_order_reviews(df['olist_order_reviews_dataset'])
    df['olist_products_dataset'] = transform_products(df['olist_products_dataset'])
    df['olist_sellers_dataset'] = transform_sellers(df['olist_sellers_dataset'])
    return df

