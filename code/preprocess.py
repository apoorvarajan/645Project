import psycopg2
import pandas as pd
from sqlalchemy import create_engine 
# conn = psycopg2.connect(database="apoorvarajan",
#                         host="localhost",
#                         user="apoorvarajan",
#                         password="")
# cursor = conn.cursor()
# cursor.execute("SELECT * FROM authors limit 2")
# # checking connection
# print(cursor.fetchone())
#conn_string = 'postgres://user:password@host/data1'
conn_string='postgresql://apoorvarajan@localhost/apoorvarajan'
db = create_engine(conn_string)
conn = db.connect()
# conn = psycopg2.connect(conn_string)
# data = {'Name': ['Tom', 'dick', 'harry'], 
#         'Age': [22, 21, 24]} 
  
# # Create DataFrame 
# df = pd.DataFrame(data) 
# df.to_sql('data', con=conn, if_exists='replace', 
#           index=False) 
# conn = psycopg2.connect(conn_string 
#                         ) 
# conn.autocommit = True
# cursor = conn.cursor() 
  
# sql1 = '''select * from data;'''
# cursor.execute(sql1) 
# for i in cursor.fetchall(): 
#     print(i) 
  
# # conn.commit() 
# conn.close() 

import pandas as pd
headers = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
           "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss integer", 
           "hours_per_week", "native_country", "income"]
train = pd.read_csv("../data/train.csv", header=0, names = headers, na_values=" ?")
test = pd.read_csv("../data/test.csv", header=0, names = headers, na_values=" ?")
traintest = pd.concat([train, test])
married_adults_header = [' Married-civ-spouse', ' Widowed', ' Married-spouse-absent', ' Married-AF-spouse', ' Separated']
unmarried_adults_header = [' Divorced',' Never-married']
for m in married_adults_header:
    traintest['marital_status'] = traintest['marital_status'].replace(m,"Married")
    train['marital_status'] = train['marital_status'].replace(m,"Married")
for u in unmarried_adults_header:
    traintest['marital_status'] = traintest['marital_status'].replace(u,"Unmarried")
    train['marital_status'] = train['marital_status'].replace(u,"Unmarried")
train.to_sql('adults', con=conn, if_exists='replace', index=False)
traintest.to_sql("total_adults", con=conn, if_exists='replace', index=False)
# traintest.to_csv("../data/adults_total.csv", index=False)
# train.to_csv("../data/preprocessed_train.csv", index=False)
conn = psycopg2.connect(conn_string) 
conn.autocommit = True
cursor = conn.cursor() 
sql1 = '''select * from adults limit 1;'''
sql2 = '''select * from total_adults limit 5;'''
cursor.execute(sql1)
cursor.execute(sql2)
for i in cursor.fetchall(): 
    print(i) 
conn.close()