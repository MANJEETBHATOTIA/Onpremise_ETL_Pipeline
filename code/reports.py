import logging
import os
import smtplib

import pandas as pd
from send_email import email_notify


class Report:
    
    
        def maxlocation(self, path):
            df = pd.read_csv(path)
            df_max_location = df.location.value_counts()
            return(df_max_location.head(10))
            
        def minlocation(self,path):
            df = pd.read_csv(path)
            df_min_location = df.location.value_counts()
            return(df_min_location.tail(10))


        def save_location_report(self):
            
            try:
            
                path = "E:/project/ETL_Pipeline/data/location"
                
                counter = 0
                for file in os.listdir(path):
                    # print(path+"/"+str(file))
                    max = self.maxlocation(path+"/"+str(file))
                    max.to_csv(f'E:/project/ETL_Pipeline/report/locationreport/location_report_max{counter}.csv')
                    
                    
                    min = self.minlocation(path+"/"+str(file))
                    min.to_csv(f'E:/project/ETL_Pipeline/report/locationreport/location_report_min{counter}.csv')

                    counter = counter+1
                    
                email_notify.email_send_requests('location report saved successfully')
                
                logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

                logging.info('location report saved successfully')
            
            except:
                
                logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

                logging.error('save_location_report function showing error')
        
        
        def max_duration(self,path):
            df = pd.read_csv(path)
            df_max_duration = df.Duration.value_counts()
            return(df_max_duration.head(10))

        def min_duration(self,path):
            df = pd.read_csv(path)
            df_min_duration = df.Duration.value_counts()
            return(df_min_duration.tail(10))


        def save_timeseries_report(self):
            
            try:
            
                path = 'E:/project/ETL_Pipeline/data/duration'
                
                counter = 0
                for file in os.listdir(path):
                    # print(path+"/"+str(file))
                    max = self.max_duration(path+"/"+str(file))
                    max.to_csv(f'E:/project/ETL_Pipeline/report/durationreport/duration_report_max{counter}.csv')
                    
                    min = self.min_duration(path+"/"+str(file))
                    min.to_csv(f'E:/project/ETL_Pipeline/report/durationreport/duration_report_min{counter}.csv')

                    counter = counter+1
                    
                email_notify.email_send_requests('duration report saved successfully')  
                
                logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

                logging.info('duration report saved successfully') 
                
            except:
                logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="")

                logging.error('save_location_report function showing error')
          
        
   
            
# repo = Report()
# repo.save_location_report()
# repo.save_timeseries_report()
                

