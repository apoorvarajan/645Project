import psycopg2
import pandas as pd
from sqlalchemy import create_engine 
from collections import defaultdict
conn_string='postgresql://apoorvarajan@localhost/apoorvarajan'
# db = create_engine(conn_string)
# conn = db.connect()
conn = psycopg2.connect(conn_string) 
#conn.autocommit = True
cursor = conn.cursor() 

measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
aggregates=['sum','avg','max','min','count']

visualization = []
for f in aggregates:
    for a in dimension_attr:
        for m in measure_attr:
            visualization.append((a,m,f))
for a,m,f in visualization:
    sql_married='select '+a+', '+f+'('+m+') from married_adults where '+a+' is not null group by '+a+' order by '+a+';'
    sql_unmarried='select '+a+', '+f+'('+m+') from unmarried_adults where '+a+' is not null group by '+a+' order by '+a+';'
    cursor.execute(sql_married)
    res_married= cursor.fetchall()
    cursor.execute(sql_unmarried)
    res_unmarried = cursor.fetchall()
    dict_married = defaultdict(list)
    print(res_married)
    break
    # f_list_1,f_list_2 = get_agg_lists(res_1,res_2)
    # dict_kl[a,m,f].append(get_utility(f_list_1,f_list_2))

