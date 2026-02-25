from src.extract.extract import extract_csvs
from src.transform.transform import transform_all
from src.load.load import run_load
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_pipeline():
    logging.info("Iniciando O Pipeline")
    raw_data = extract_csvs("data/")
    transformed_data = transform_all(raw_data)
    run_load(transformed_data)
    logging.info("Pipeline finalizado!")

if __name__ == "__main__":
    run_pipeline()