import logging
import os
import smtplib

import pandas as pd
import psycopg2
from database_connection import conn
from send_email import email_notify

cur = conn.cursor()

#class for inserting the data into tables

class insert_data:
    

    
    def storing_db_location(self):
        
        cur = conn.cursor()
        
        try:
            path = 'E:/project/ETL_Pipeline/data/location'
            
            insertion = """ 
                    INSERT INTO locationreport (file_name,location) VALUES (%s,%s)
                    """
                    
            for file in os.listdir(path):
                df = pd.read_csv(os.path.join(path,file))
                df1 = list(df['location'])
                df1 = ",".join(df1) # converting the coulmn into string 
            
                cur.execute(insertion,(file,df1))
                    
            conn.commit()
            
            email_notify.email_send_requests('data inserted in locationreport table successfully')
            
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('location data is stored in table successfully')
            
        except:
        
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('location data is not storing')
            
            logging.exception('exception')    
        
        
        

    def storing_db_duration(self):
        
        cur = conn.cursor()
        
        try:
            path = 'E:/project/ETL_Pipeline/data/duration'
            insertion = """ 
                    INSERT INTO durationreport (file_name,duration) VALUES (%s,%s)
                    """
                    
            for file in os.listdir(path):
                # print(path)
                df = pd.read_csv(os.path.join(path,file))
                
                df1 = list(df['Duration'].astype(str)) # converting int into string 
                df1 = ",".join(df1) # convert every element of col into string 
            
                cur.execute(insertion,(file,df1))
                    
            conn.commit()
            
            email_notify.email_send_requests('data inserted in durationreport table successfully')
            
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('duration data is stored in table successfully')
            
        except:
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('duration data is not storing')
            
            # logging.exception('exception')
        
        conn.close() 
        
# insert = insert_data()

# insert.storing_db_location()

# insert.storing_db_duration()                  
 


   
    