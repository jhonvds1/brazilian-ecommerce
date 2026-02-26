# 📊 Brazilian E-Commerce Data Pipeline (ETL + BigQuery)

Pipeline de Engenharia de Dados completo, responsável por extrair, transformar e carregar (ETL) dados de e-commerce brasileiro em um Data Warehouse no BigQuery, com posterior análise e visualização automática.

---

## 🚀 Visão Geral

Este projeto implementa um pipeline de dados fim-a-fim seguindo boas práticas de Engenharia de Dados:

1. **Extract** → Leitura automatizada de múltiplos arquivos CSV
2. **Transform** → Limpeza, tratamento e modelagem dimensional (Star Schema)
3. **Load** → Carga das tabelas no Data Warehouse (BigQuery)
4. **Analytics** → Execução automática de notebook para geração de insights

O objetivo é simular um fluxo real de dados utilizado em ambientes corporativos.

---

## 🧠 Arquitetura do Pipeline

```
CSV (Raw Data)
      │
      ▼
[Extract - Pandas]
      │
      ▼
[Transform - Limpeza + Modelagem Dimensional]
      │
      ▼
[Load - BigQuery Data Warehouse]
      │
      ▼
[Notebook Analytics - Visualizações e Insights]
```

---

## 🏗️ Modelagem de Dados

O projeto utiliza **modelagem dimensional (Star Schema)**, contendo:

### 🔹 Tabelas Dimensão

* `dim_product` → Informações de produtos
* `dim_seller` → Informações dos vendedores
* `dim_time` → Datas derivadas dos pedidos
* `dim_review` → Avaliações dos pedidos

### 🔸 Tabela Fato

* `fact_order` → Fatos principais de vendas (price, product_id, seller_id, review_id, time_id)

Essa abordagem é padrão em projetos de Data Warehouse e BI.

---

## 🛠️ Tecnologias Utilizadas

* Python 3
* Pandas
* Google BigQuery
* Seaborn / Matplotlib
* Papermill (execução automática de notebooks)
* Logging (monitoramento do pipeline)

---

## 📁 Estrutura do Projeto

```
├── data/                     # CSVs brutos
├── docs
|   ├── diagrama.png          # Diagrama do Banco de Dados
|   └── schema.sql            # Modelagem do Banco de Dados no BigQuery
├── notebooks/
│   └── visualizacao.ipynb    # Análises e gráficos
├── src/
│   ├── extract/
│   │   └── extract.py        # Extração dos dados
│   ├── transform/
│   │   └── transform.py      # Limpeza e modelagem dimensional
│   ├── load/
│   │   └── load.py           # Carga no BigQuery
│   └── main.py               # Orquestração do pipeline
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚙️ Como Executar o Projeto

### 1️⃣ Clonar o repositório

```bash
git clone https://github.com/jhonvds1/brazilian-ecommerce.git
cd brazilian-ecommerce
```

### 2️⃣ Criar ambiente virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3️⃣ Instalar dependências

```bash
pip install -r requirements.txt
```

### 4️⃣ Configurar credenciais do BigQuery

#### 🔐 Configuração de Acesso ao BigQuery

Para que o pipeline consiga carregar os dados no Google BigQuery, é necessário configurar credenciais de acesso utilizando uma Service Account do Google Cloud.

Essa configuração permite que o projeto se conecte de forma segura ao Data Warehouse durante a etapa de **LOAD**.

---

#### 📌 Pré-requisitos
Antes de executar o pipeline, é necessário:
- Possuir uma conta no Google Cloud
- Ter um projeto criado no Google Cloud
- Criar previamente o dataset `ecommerce_dw` no BigQuery

> As tabelas do Data Warehouse são criadas automaticamente pelo pipeline durante a carga dos dados.

---

#### 🛠️ Passo a passo para criar as credenciais

1. Acesse o Console do Google Cloud:  
   https://console.cloud.google.com/

2. No menu lateral, vá em:  
   **IAM & Admin → Service Accounts**

3. Clique em **"Create Service Account"**

4. Defina:
   - Nome: `bq-pipeline-access` (ou outro de sua preferência)
   - Permissão recomendada: **BigQuery Data Editor** ou **BigQuery Admin**

5. Após criar a conta, vá até a aba **"Keys"**
6. Clique em **"Add Key" → "Create new key"**
7. Escolha o formato **JSON**
8. O arquivo de credenciais será baixado automaticamente (ex: `credentials.json`)

> Observação importante  
> Este projeto não cria nem baixa credenciais automaticamente.  
> O usuário deve fornecer suas próprias credenciais do Google Cloud para autenticação no BigQuery.

### 5️⃣ Executar o Pipeline

```bash
python -m src.main
```

---

## 📊 Análises Realizadas

O notebook executado automaticamente gera insights como:

* Ticket médio por categoria
* Categorias mais vendidas
* Cidades com maior volume de vendas
* Tendência de vendas por mês

Essas análises simulam cenários reais de BI e tomada de decisão.

---

## 📈 Boas Práticas Aplicadas

✔️ Arquitetura ETL modular
✔️ Logging estruturado em todas as etapas
✔️ Tratamento de erros com `try/except`
✔️ Modelagem dimensional (Star Schema)
✔️ Orquestração automatizada do pipeline
✔️ Integração com Data Warehouse em nuvem

---

## 🎯 Objetivo do Projeto

Este projeto foi desenvolvido para fins de estudo e portfólio, com foco em:

* Engenharia de Dados Júnior
* Pipelines ETL reais
* Data Warehouse em nuvem
* Processamento e modelagem de dados analíticos

---

## 📌 Possíveis Melhorias Futuras

* Orquestração com Airflow ou Prefect
* Containerização com Docker
* Testes automatizados (Pytest)
* Monitoramento com métricas de execução

---

## 👨‍💻 Autor

**Jonatha Viegas da Silva**
Estudante de Engenharia de Computação (UFPel)
GitHub: https://github.com/jhonvds1

---

## ⭐ Considerações Finais

Este projeto demonstra a construção de um pipeline de dados completo, desde a ingestão até a geração de insights analíticos, seguindo práticas utilizadas no mercado de Engenharia de Dados.

---
