import pandas as pd

from src.logger import logging
from datetime import datetime


class DataTransformation:
    # start data transformation 
    def dataTransformationMethod(self, cpu_avg_data):
        try:
            logging.info(f"Data Transformation just started at {(datetime.now())}")
            logging.info("Read the csv file")
            # read the csv data
            cpu_df = pd.read_csv(cpu_avg_data)
            print(cpu_df)
        except Exception as e:
            print(e)

