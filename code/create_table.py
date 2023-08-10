# from database_connection import conn
import logging

from connect import con
from send_email import email_notify

# class create_tables:
    
    
    
#     def table_creator(self):
        
#         cur = conn.cursor()

#         try:
            
#             create_table1 = """
#             CREATE TABLE locationreport(
#                 file_id serial PRIMARY KEY,
#                 file_name text,
#                 location text
                
#             )
#             """

#             create_table2 = """
#             CREATE TABLE durationreport(
#                 file_id serial PRIMARY KEY,
#                 file_name text,
#                 duration text
            
#             )
#             """

#             email_notify.email_send_requests('tables created successfully')

#             cur.execute(create_table1)

#             cur.execute(create_table2)

#             conn.commit()        
            
        
#             logging.basicConfig(filename="/home/manjeet/Downloads/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

#             logging.info('tables are created successfully') 
            
#         except:
            
#             logging.basicConfig(filename="/home/manjeet/Downloads/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

#             logging.error('tables are not created successfully') 
            
            
#         # conn.close()
    

# tables = create_tables()

# tables.table_creator()


class create_tables:
    

    def table_creator(self):
        
        conn = con()
        
        cur = conn.cursor()
        

        try:
            
            create_table1 = """
            CREATE TABLE locationreport(
                file_id serial PRIMARY KEY,
                file_name text,
                location text
                
            )
            """

            create_table2 = """
            CREATE TABLE durationreport(
                file_id serial PRIMARY KEY,
                file_name text,
                duration text
            
            )
            """

            email_notify.email_send_requests('tables created successfully')

            cur.execute(create_table1)

            cur.execute(create_table2)

            conn.commit()        
            
        
            logging.basicConfig(filename="/home/manjeet/Downloads/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

            logging.info('tables are created successfully') 
            
        except:
            
            logging.basicConfig(filename="/home/manjeet/Downloads/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('tables are not created successfully') 
            
            
        conn.close()
    

# tables = create_tables()

# tables.table_creator()


 
