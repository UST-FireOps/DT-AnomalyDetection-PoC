import json
import os
import pandas
import csv
from flatten_json import flatten

from datetime import datetime

from dataclasses import dataclass

from src.logger import logging

from src.components.dataTransformation import DataTransformation
from src.components.anomalyTrainer import anomalyTrainer

@dataclass
class DataIngestionConfig:
    cpuAvg_ts_datapath: str=os.path.join('artifacts',"cpuAvg.csv")


class DataIngestion:
    def __init__(self):
        self.ingestionConfig = DataIngestionConfig()

    def dataIngestionMethod(self):
        logging.info(f"Data ingestion just started at {(datetime.now())}")
        try:
            # make a directory from artifacts
            os.makedirs(os.path.dirname(self.ingestionConfig.cpuAvg_ts_datapath), exist_ok=True)
            csvDatafile = open(self.ingestionConfig.cpuAvg_ts_datapath, 'w', newline='')
            #csvWriter = csv.writer(csvDatafile)

            # read the data from data folder
            logging.info("Read the json dataset form dataset folder")
            json_list = json.load(open('dataset/timesries.json', 'r'))
            
            logging.info("extract the item fro the json file")
            key_list = ['@timestamp','Host Name','Host OS','CPU Avg']
            source = [innerData['_source'] for innerData in json_list['hits']['hits']]
            innerData = [{k:id[k] for k in key_list} for id in source]

            # flatten and convert into into the dataframe
            logging.info("flatten and convert into into the dataframe")
            json_list_flatten = (flatten(d, '.') for d in innerData)
            df = pandas.DataFrame(json_list_flatten)

            # export dataframe into csv file
            logging.info("export data into csv file")
            df_csv = df.to_csv(csvDatafile, sep=',', index=None, header=True)

            # ingestion is completed
            logging.info("Data ingestion is completed")

            return(
                self.ingestionConfig.cpuAvg_ts_datapath
            )
   

                
        except Exception as e:
            print(e)

if __name__ == "__main__":
    # start data ingestion
    ingestionObj = DataIngestion()
    cpu_avg_data = ingestionObj.dataIngestionMethod()

    # start data transformation
    transformationObj = DataTransformation()
    transformationObj.dataTransformationMethod(cpu_avg_data)

    # start anomaly method training
    anomalyObj = anomalyTrainer()
    anomalyObj.anomalyTrainerMethod()
