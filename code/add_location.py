import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetFile
from send_email import email_notify


class add_locations:
    
    
    def addlocations(self):
        
        path = "E:\project\ETL_Pipeline\data\partition_data"
        
        try:
            if (os.path.exists(path)):
                counter = 0
                for file in os.listdir(path):
                    # print(path+"/"+str(file))
                    df = ParquetFile(path+"/"+str(file))
                    df_location = pd.read_parquet(path+"/"+str(file))
                    
                    def location(value):
                        if value == 142:
                            return "Noida Sec-1"
                        elif value == 237:
                            return "Noida Sec-2"
                        elif value == 161:
                            return "Noida Sec-3"
                        elif value == 79:
                            return "Noida Sec-4"
                        elif value == 170:
                            return "Noida Sec-5"
                        elif value == 48:
                            return "Noida Sec-6"
                        elif value == 263:
                            return "Noida Sec-7"
                        elif value == 132:
                            return "Noida Sec-8"
                        elif value == 249:
                            return "Noida Sec-9"
                        elif value == 236:
                            return "Noida Sec-10"
                        elif value == 138:
                            return "Noida Sec-11"
                        elif value == 164:
                            return "Noida Sec-12"
                        elif value == 163:
                            return "Noida Sec-13"
                        elif value == 148:
                            return "Noida Sec-14"
                        elif value == 238:
                            return "Noida Sec-15"
                        elif value == 113:
                            return "Noida Sec-16"
                        elif value == 50:
                            return "Noida Sec-17"
                        elif value == 229:
                            return "Noida Sec-18"
                        elif value == 162:
                            return "Noida Sec-19"
                        elif value == 231:
                            return "Noida Sec-20"
                        elif value == 43:
                            return "Noida Sec-21"
                        elif value == 68:
                            return "Noida Sec-22"
                        elif value == 186:
                            return "Noida Sec-23"
                        elif value == 158:
                            return "Noida Sec-24"
                        elif value == 141:
                            return "Noida Sec-25"
                        elif value == 144:
                            return "Noida Sec-26"
                        elif value == 239:
                            return "Noida Sec-27"
                        elif value == 75:
                            return "Noida Sec-28"
                        elif value == 125:
                            return "Noida Sec-29"
                        elif value == 264:
                            return "Noida Sec-30"
                        elif value == 140:
                            return "Noida Sec-31"
                        elif value == 151:
                            return "Noida Sec-32"
                        elif value == 246:
                            return "Noida Sec-33"
                        elif value == 234:
                            return "Noida Sec-34"
                        elif value == 211:
                            return "Noida Sec-35"
                        elif value == 166:
                            return "Noida Sec-36"
                        elif value == 233:
                            return "Noida Sec-37"
                        elif value == 137:
                            return "Noida Sec-38"
                        elif value == 244:
                            return "Noida Sec-39"
                        elif value == 116:
                            return "Noida Sec-40"
                        elif value == 262:
                            return "Noida Sec-41"
                        elif value == 90:
                            return "Noida Sec-42"
                        elif value == 42:
                            return "Noida Sec-43"
                        elif value == 129:
                            return "Noida Sec-44"
                        elif value == 62:
                            return "Noida Sec-45"
                        elif value == 41:
                            return "Noida Sec-46"
                        elif value == 69:
                            return "Noida Sec-47"
                        elif value == 261:
                            return "Noida Sec-48"
                        elif value == 114:
                            return "Noida Sec-49"
                        elif value == 107:
                            return "Noida Sec-50"
                        elif value == 229:
                            return "Noida Sec-51"
                        elif value == 143:
                            return "Noida Sec-52"
                        elif value == 13:
                            return "Noida Sec-53"
                        elif value == 255:
                            return "Noida Sec-54"
                        elif value == 243:
                            return "Noida Sec-55"
                        elif value == 74:
                            return "Noida Sec-56"
                        elif value == 189:
                            return "Noida Sec-57"
                        elif value == 65:
                            return "Noida Sec-58"
                        elif value == 168:
                            return "Noida Sec-59"
                        elif value == 87:
                            return "Noida Sec-60"
                        elif value == 45:
                            return "Noida Sec-61"
                        elif value == 88:
                            return "Noida Sec-62"
                        elif value == 100:
                            return "Noida Sec-63"
                        elif value == 209:
                            return "Noida Sec-64"
                        elif value == 152:
                            return "Noida Sec-65"
                        elif value == 24:
                            return "Noida Sec-66"
                        elif value == 181:
                            return "Noida Sec-67"
                        elif value == 179:
                            return "Noida Sec-68"
                        elif value == 265:
                            return "Noida Sec-69"
                        elif value == 119:
                            return "Noida Sec-70"
                        elif value == 35:
                            return "Noida Sec-71"
                        elif value == 80:
                            return "Noida Sec-72"
                        elif value == 260:
                            return "Noida Sec-73"
                        elif value == 4:
                            return "Noida Sec-74"
                        elif value == 37:
                            return "Noida Sec-75"
                        elif value == 193:
                            return "Noida Sec-76"
                        elif value == 230:
                            return "Noida Sec-77"
                        elif value == 70:
                            return "Noida Sec-78"
                        elif value == 145:
                            return "Noida Sec-79"
                        elif value == 7:
                            return "Noida Sec-80"
                        elif value == 232:
                            return "Noida Sec-81"
                        elif value == 226:
                            return "Noida Sec-82"
                        elif value == 228:
                            return "Noida Sec-83"
                        elif value == 196:
                            return "Noida Sec-84"
                        elif value == 146:
                            return "Noida Sec-85"
                        elif value == 66:
                            return "Noida Sec-86"
                        elif value == 146:
                            return "Noida Sec-87"
                        elif value == 97:
                            return "Noida Sec-88"
                        elif value == 224:
                            return "Noida Sec-89"
                        

                    
                    df_location['location'] = df_location['pulocationid'].map(location)


                    df_location1 = df_location.drop(['Unnamed: 0'],axis = 1)

                    df_location1.to_csv(f'E:/project/ETL_Pipeline/data/location/location{counter}.csv') 
                    counter += 1
                    
                email_notify.email_send_requests('location coulmn added successfully')  
                
                logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.INFO,filemode="a")

                logging.info('location coulmn added successfully')  
                
        except:   
            logging.basicConfig(filename="E:/project/ETL_Pipeline/logs/logss.log",format='%(asctime)s - %(message)s', level=logging.ERROR,filemode="a")

            logging.error('add_location is not working')    

            
# loc = add_locations()
# loc.addlocations(path)


# path = "/home/manjeet/Downloads/ETL_Pipeline/data/partition_data"

# def read_partition_data(path=""):
#     if (os.path.exists(path)):
#         counter = 0
#         for file in os.listdir(path):
#             # print(path+"/"+str(file))
#             df = ParquetFile(path+"/"+str(file))
#             df_location = pd.read_parquet(path+"/"+str(file))

# # df_location = pd.read_parquet("/home/manjeet/Downloads/ETL_Pipeline/Data/parquet/parquet_data.parquet")
#             locations = {142 : "Noida Sec-1",138 : "Noida Sec-2",231 : "Noida Sec-3",75 : "Noida Sec-4",166 : "Noida Sec-30",129 : "Noida Sec-31",143 : "Noida Sec-37",87 : "Noida Sec-43",
#                         161 : "Noida Sec-5",163 : "Noida Sec-10",43 : "Noida Sec-15",125 : "Noida Sec-20",233 : "Noida Sec-25",62 : "Noida Sec-32",13 : "Noida Sec-38",45 : "Noida Sec-44",
#                         79 : "Noida Sec-6",164 : "Noida Sec-11",68 : "Noida Sec-16",264 : "Noida Sec-21",137 : "Noida Sec-26",41 : "Noida Sec-33",255 : "Noida Sec-39",88 : "Noida Sec-45",
#                         237 : "Noida Sec-7",148 : "Noida Sec-12",186 : "Noida Sec-17",140 : "Noida Sec-22",244 : "Noida Sec-27",69 : "Noida Sec-34",243 : "Noida Sec-40",100 : "Noida Sec-46",
#                         170 : "Noida Sec-8",238 : "Noida Sec-13",158 : "Noida Sec-18",151 : "Noida Sec-23",116 : "Noida Sec-28",261 : "Noida Sec-35",74 : "Noida Sec-41",209 : "Noida Sec-47",
#                         48 : "Noida Sec-9",113 : "Noida Sec-14",141 : "Noida Sec-19",246 : "Noida Sec-24",262 : "Noida Sec-29",114 : "Noida Sec-36",189 : "Noida Sec-42",152 : "Noida Sec-48",
#                         132 : "Noida Sec-49",50 : "Noida Sec-50",144 : "Noida Sec-51",234 : "Noida Sec-52",90 : "Noida Sec-53",107 : "Noida Sec-54",65 : "Noida Sec-55",24 : "Noida Sec-56",
#                         249 : "Noida Sec-57",229 : "Noida Sec-58",239 : "Noida Sec-59",211 : "Noida Sec-60",42 : "Noida Sec-61",229 : "Noida Sec-62",168 : "Noida Sec-63",181 : "Noida Sec-64",
#                         236 : "Noida Sec-65",162 : "Noida Sec-66",179 : "Noida Sec-68"}
#             def location(val):
#                 for key,value in locations.items():
#                     if key == val:
#                         return value
        
  
#             df_location['location'] = df_location['pulocationid'].map(location)

#             df_location1 = df_location.drop(['Unnamed: 0'],axis=1)

#             df_location1.to_csv(f'/home/manjeet/Downloads/ETL_Pipeline/data/location/location{counter}.csv') 
#             counter += 1
            
             
# fun = read_partition_data(path)   