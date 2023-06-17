import os
import pandas as pd

from src.logger import logging
from datetime import datetime

class anomalyTrainer:
    def anomalyTrainerMethod(self, number_of_jobs, batch_dir):
        try:
            logging.info(f"Anomaly Training has just started at {(datetime.now())}")
            print("Total number of batch jobs we required : {}".format(number_of_jobs))
            #print("Batch dataset directory is : {}".format(batch_dir))
            print(f"Batch dataset directory is : {(batch_dir)}")
            logging.info("List down the csv dataset")
            dir_list = os.listdir(batch_dir)
        
            # for sample anomaly detection using stat function (statistics method)
            logging.info("Anomaly detection using stat function (statistics method)")
            # test with sample dataset
            test_dataset = dir_list[0]
            test_file_path = os.path.join(batch_dir, test_dataset)

            df = pd.read_csv(test_file_path)
        
        except Exception as e:
            print(e)



