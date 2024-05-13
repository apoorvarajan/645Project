import psycopg2
import pandas as pd
from sqlalchemy import create_engine 

conn_string='postgresql://femimoljoseph@localhost/SeeDB_Project'
db = create_engine(conn_string)
conn = db.connect()

headers = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status",
           "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", 
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

conn = psycopg2.connect(conn_string) 
conn.autocommit = True
cursor = conn.cursor() 
sql_drop_unmarried ='''drop table if exists unmarried_adults;'''
sql_drop_married = '''drop table if exists married_adults;'''
sql1 = '''create table unmarried_adults as select * from total_adults where marital_status ='Unmarried';'''
sql2 = '''create table married_adults as select * from total_adults where marital_status ='Married';'''
sql3 = '''alter table unmarried_adults drop marital_status;'''
sql4 = '''alter table married_adults drop marital_status;'''
cursor.execute(sql_drop_unmarried)
cursor.execute(sql_drop_married)
cursor.execute(sql1)
cursor.execute(sql2)
cursor.execute(sql3)
cursor.execute(sql4)

conn.close()