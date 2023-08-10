import logging
import os

import dateutil
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile
from send_email import email_notify


class add_durations:
    
    
    def adddurations(self):
        
        try:
            
            path = "E:/project/ETL_Pipeline/data/partition_data"
            
            if (os.path.exists(path)):
                counter = 0
                for file in os.listdir(path):
                    # print(path+"/"+str(file))
                    df = ParquetFile(path+"/"+str(file))
                    df_timeseries = pd.read_parquet(path+"/"+str(file))

                    # df_timeseries = pd.read_parquet("/home/manjeet/Downloads/ETL_Pipeline/Data/parquet/parquet_data.parquet")

                    # Converting to date-type (if it's already in that format)
                    df_timeseries['tpep_pickup_datetime'] = df_timeseries['tpep_pickup_datetime'].apply(dateutil.parser.parse)

                    #Then use `tz_localize` to make timestamps aware about time zone and then convert to IST (Asia/Kolkata)
                    df_timeseries['Time Series_ist'] = pd.to_datetime(df_timeseries['tpep_pickup_datetime']).\
                                                                dt.tz_localize('utc').\
                                                                dt.tz_convert('Asia/Kolkata') 

                    df_timeseries['tpep_dropoff_datetime'] = df_timeseries['tpep_dropoff_datetime'].apply(dateutil.parser.parse)

                    df_timeseries['Time Series_ist'] = pd.to_datetime(df_timeseries['tpep_dropoff_datetime']).\
                                                                dt.tz_localize('utc').\
                                                                dt.tz_convert('Asia/Kolkata') 
                                                                
                    df_timeseries["Duration"] = (df_timeseries['tpep_dropoff_datetime'] - df_timeseries['tpep_pickup_datetime']).dt.total_seconds() / 60 

                    #converting float to int 
                    df_timeseries['Duration'] = df_timeseries['Duration'].astype('int')

                    df_timeseries1 = df_timeseries.drop(['Unnamed: 0','tpep_pickup_datetime','tpep_dropoff_datetime','Time Series_ist'],axis = 1)

                    df_timeseries1.to_csv(f'E:/project/ETL_Pipeline/data/duration/duration{counter}.csv')
                    
                    counter += 1
                    
                    
            email_notify.email_send_requests('duration coulmn added successfully')  
            
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('duration coulmn added successfully')
            
        except:      
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('adddurations function showing error') 
            
            logging.exception('exception')  
  

# durations = add_durations()

# durations.adddurations()