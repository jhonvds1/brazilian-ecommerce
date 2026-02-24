from src.transform.transform import transform_all
import pandas as pd


#TODO: NAO EXISTE UM ID TEMPO AINDA

def run_load():
    df = transform_all()
    df_dim_product = df['olist_products_dataset'][['product_id', 'product_category_name']]
    df_dim_time = df['olist_orders_dataset'][['time_id', 'order_purchase_timestamp', 'year_purchase', 'month_purchase', 'day_purchase']]
    df_dim_review = df['olist_order_reviews_dataset'][['review_id', 'review_score']]
    df_dim_seller = df['olist_sellers_dataset'][['seller_id', 'seller_city']]
    df_order_review = pd.merge(df['olist_order_items_dataset'], df['olist_order_reviews_dataset'], on='order_id')
    df_order_review = pd.merge(df_order_review, df['olist_orders_dataset'], on='order_id')
    df_fact_order = df_order_review[['order_id', 'product_id', 'seller_id', 'time_id' ,'review_id', 'price']]
    
    print(df_fact_order)


run_load()

