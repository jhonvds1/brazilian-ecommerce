# Bibliotecas para leitura de arquivos, manipulação de dados e logging da etapa de extração
import pandas as pd
import os
import logging

# Configuração global de logging (nível INFO para acompanhar execução do ETL)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Logger específico para a etapa de extração
extract_logger = logging.getLogger("EXTRACT")


def extract_csvs(folder_path: str) -> dict[str, pd.DataFrame]:
    """
    Lê todos os arquivos CSV de um diretório e retorna um dicionário de DataFrames.

    A chave do dicionário corresponde ao nome do arquivo (sem extensão)
    e o valor é o DataFrame carregado em memória.

    Args:
        folder_path (str): Caminho do diretório contendo os arquivos CSV.

    Returns:
        dict[str, pd.DataFrame]: Dicionário com DataFrames indexados pelo nome do arquivo.
    """
    extract_logger.info('Iniciando a extração dos dados')
    dfs = {}

    # Percorre todos os arquivos presentes no diretório informado
    for file in os.listdir(folder_path):

        # Processa apenas arquivos CSV para compor a camada de dados brutos
        if file.endswith(".csv"):

            # Constrói o caminho completo do arquivo (compatível entre sistemas operacionais)
            path = os.path.join(folder_path, file)

            try:
                df = pd.read_csv(path)
            except Exception as e:
                raise Exception(f"Falha na leitura do CSV: {file}") from e

            # Remove a extensão para usar como chave identificadora do dataset
            nome = file.replace(".csv", "")

            extract_logger.info(f"Dados de '{nome}' extraídos com sucesso")
            dfs[nome] = df

    if not dfs:
        extract_logger.warning("Nenhum arquivo CSV encontrado no diretório informado")

    extract_logger.info("Processo de extração de dados finalizado")
    return dfs