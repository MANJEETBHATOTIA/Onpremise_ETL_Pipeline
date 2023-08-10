import psycopg2
from config import config


def con():
    """ Connect to the PostgreSQL database server """
    conn = None
    # read connection parameters
    params = config()
    conn = psycopg2.connect(**params)
    # print(conn)
    cur = conn.cursor()  
   
    return conn
# connect()




