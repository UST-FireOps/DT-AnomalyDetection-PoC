from src.logger import logging
from datetime import datetime

from src.components.dataTransformation import DataTransformation
from src.components.anomalyTrainer import anomalyTrainer

class DataIngestion:
    def dataIngestionMethod():
        logging.info(f"Data ingestion just started at {(datetime.now())}")

# start data ingestion
ingestionObj = DataIngestion()
DataIngestion.dataIngestionMethod()

# start data transformation
transformationObj = DataTransformation()
DataTransformation.dataTransformationMethod()

# start anomaly method training
anomalyObj = anomalyTrainer()
anomalyTrainer.anomalyTrainerMethod()
