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
            logging.info("Read the csv file form artifacts folder")
            # read the csv data
            cpu_df = pd.read_csv(cpu_avg_data)
            # groupby based on the ipAddress/Hostname
            logging.info("Grouping the dataset based on the host ip address")
            self.number_of_jobs = 0
            for i,g in cpu_df.groupby(['Host Name']):
                g['@timestamp'] = pd.to_datetime(g['@timestamp'], format='%Y-%m-%dT%H:%M:%S.%f')
                g['CPU Avg'] = g['CPU Avg'].astype(float)
                # find the Null values
                if g['CPU Avg'].isnull().values.any():
                    logging.info("find and remove the NaN value from {}".format(i))
                    g['CPU Avg'].fillna((g['CPU Avg'].mean()), inplace=True)
                g['Host Name'] = g['Host Name'].astype(str)
                g['Host OS'] = g['Host OS'].astype(str)
                g.sort_values(by=['@timestamp'])
                filename = i[0]
                file_path = '{}.csv'.format(filename)
                self.number_of_jobs = self.number_of_jobs + 1
                dataset = os.path.join(self.transformation.batch_cpu_dataset,file_path)
                g.to_csv(dataset, header=True, index_label=False, index=False)

            
            
            return (
                self.number_of_jobs,
                self.transformation.batch_cpu_dataset
            )
                
            

        except Exception as e:
            print(e)

