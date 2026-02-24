CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_product` (
  product_id STRING,
  product_category_name STRING
);

CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_review`(
  review_id STRING,
  revies_score FLOAT64
);  

CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_seller` (
  seller_id STRING,
  seller_city STRING
);

CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_time` (
  time_id INT64,
  date DATETIME,
  year INT64,
  month INT64,
  day INT64
);

CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.fact_order` (
  order_id STRING,
  product_id STRING,
  seller_id STRING,
  time_id STRING,
  review_id STRING,
  price FLOAT64
);

