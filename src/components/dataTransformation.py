import pandas as pd
import os

from src.logger import logging
from datetime import datetime
from dataclasses import dataclass

@dataclass
class DataTransformationConfig:
    batch_cpu_dataset = os.path.join('artifacts','batch_cpu')
    os.makedirs(batch_cpu_dataset, exist_ok=True)


class DataTransformation:
    def __init__(self):
        self.transformation = DataTransformationConfig()

    # start data transformation 
    def dataTransformationMethod(self, cpu_avg_data):
        try:
            logging.info(f"Data Transformation just started at {(datetime.now())}")
            logging.info("Read the csv file")
            # read the csv data
            cpu_df = pd.read_csv(cpu_avg_data)
            # groupby based on the ipAddress/Hostname
            for i,g in cpu_df.groupby(['Host Name']):
                g['@timestamp'] = pd.to_datetime(g['@timestamp'], format='%Y-%m-%dT%H:%M:%S.%f')
                g['CPU Avg'] = g['CPU Avg'].astype(float)
                g['Host Name'] = g['Host Name'].astype(str)
                g['Host OS'] = g['Host OS'].astype(str)
                g.sort_values(by=['@timestamp'])
                print(i)
                file_path = '{}.csv'.format(i)
                dataset = os.path.join(self.transformation.batch_cpu_dataset,file_path)
                g.to_csv(dataset, header=True, index_label=False, index=False)
            

        except Exception as e:
            print(e)

