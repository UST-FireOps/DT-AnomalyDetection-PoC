import os
import pandas as pd
import numpy as np
import scipy
import scipy.stats as ss
import sys


from src.logger import logging
from src.exception import CustomException

from datetime import datetime

class statAnomalyTrainerConfig:
    statCPUanomaly = os.path.join('artifacts','statAnomaly')
    os.makedirs(statCPUanomaly, exist_ok=True)

class anomalyTrainer:
    def __init__(self):
        self.statAnConfig = statAnomalyTrainerConfig()
        

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
            for item in dir_list:
                test_dataset = item
                test_file_path = os.path.join(batch_dir, test_dataset)

                df = pd.read_csv(test_file_path)
                # calculate mean and standard deviation of the CPU Avg
                print(f"mean of cpu average :{df['CPU Avg'].mean()}")
                print("standard deviation of cpu average : {}".format(df['CPU Avg'].std(ddof=0)))
            
                # start anomaly detection using z-score 
                '''
                    z-score measure how far a data point is away from the mean as a signed multiple of the 
                    standard deviation. Large absolute values of the z-score suggest an anomaly
                '''

                # Calculate the z-score and add the result to the dataframe
                z_score = ss.zscore(df['CPU Avg'], ddof=0)
                #print(z_score)
                df = df.assign(zscore = z_score)

                # setting up threshold and anomaly
                threshold_pos = 2
                threshold_neg = -2
                df['IsAnomaly'] = np.where(df['zscore'] < threshold_neg, 'Yes', np.where(df['zscore'] > 
                    threshold_pos, 'Yes', 'No'))
                
                # calculated z-score in the dataset
                print(df)
                ano_path = str(test_dataset.rsplit('.',maxsplit=1)[0])+"_anomaly.csv"
                dataset = os.path.join(self.statAnConfig.statCPUanomaly,ano_path)
                df.to_csv(dataset, header=True, index_label=False, index=False)
      
            
        except Exception as e:
            raise CustomException(e, sys) 



