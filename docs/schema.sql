-- =========================================================
-- DIMENSÃO: PRODUTO
-- Armazena informações sobre os produtos vendidos no e-commerce
-- Cada produto possui uma categoria associada
-- =========================================================
CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_product` (
  product_id STRING,            -- Identificador único do produto
  product_category_name STRING  -- Nome da categoria do produto
);

-- =========================================================
-- DIMENSÃO: REVIEW
-- Contém as avaliações dadas pelos clientes aos pedidos
-- =========================================================
CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_review`(
  review_id STRING,    -- Identificador único da avaliação
  review_score FLOAT64 -- Nota da avaliação (ex: 1 a 5)
);  

-- =========================================================
-- DIMENSÃO: SELLER
-- Armazena informações dos vendedores do marketplace
-- =========================================================
CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_seller` (
  seller_id STRING,  -- Identificador único do vendedor
  seller_city STRING -- Cidade do vendedor
);

-- =========================================================
-- DIMENSÃO: TEMPO
-- Permite análises temporais (ano, mês, dia)
-- time_id é usado como chave para a tabela fato
-- =========================================================
CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.dim_time` (
  time_id INT64,   -- Chave substituta da dimensão tempo
  date DATETIME,   -- Data completa do pedido
  year INT64,      -- Ano do pedido
  month INT64,     -- Mês do pedido
  day INT64        -- Dia do pedido
);

-- =========================================================
-- TABELA FATO: PEDIDOS
-- Armazena as métricas principais do negócio (preço)
-- e faz ligação com todas as dimensões (modelo estrela)
-- =========================================================
CREATE TABLE `brazilian-ecommerce-488418.ecommerce_dw.fact_order` (
  order_id STRING, -- Identificador do pedido
  product_id STRING, -- FK para dim_product
  seller_id STRING,  -- FK para dim_seller
  time_id INT64,     -- FK para dim_time
  review_id STRING,  -- FK para dim_review
  price FLOAT64      -- Métrica: valor do item vendido
);