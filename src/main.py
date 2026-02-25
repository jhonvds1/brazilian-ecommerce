from src.extract.extract import extract_csvs
from src.transform.transform import transform_all
from src.load.load import run_load


def run_pipeline():
    raw_data = extract_csvs("data/")
    transformed_data = transform_all(raw_data)
    run_load(transformed_data)


run_pipeline()