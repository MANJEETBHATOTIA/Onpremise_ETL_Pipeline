# from database_connection import conn 

from connect import con

conn = con()

cur = conn.cursor()

drop_table_durationreport = """
drop table durationreport;
"""

drop_table_locationreport = """
drop table locationreport 
"""

cur.execute(drop_table_durationreport)

cur.execute(drop_table_locationreport)

conn.commit()

conn.close()    