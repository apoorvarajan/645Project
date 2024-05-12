import psycopg2
import pandas as pd
from sqlalchemy import create_engine 
from collections import defaultdict
from decimal import Decimal
conn_string='postgresql://apoorvarajan@localhost/apoorvarajan'
# db = create_engine(conn_string)
# conn = db.connect()
conn = psycopg2.connect(conn_string) 
#conn.autocommit = True
cursor = conn.cursor() 

measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
aggregates=['sum','avg','max','min','count']

#Exhaustive Search

# visualization = []
# for f in aggregates:
#     for a in dimension_attr:
#         for m in measure_attr:
#             visualization.append((a,m,f))
# for a,m,f in visualization:
#     print("....."+a+"......"+m+"....."+f)
#     sql_married='select '+a+', '+f+'('+m+') from married_adults where '+a+' is not null group by '+a+' order by '+a+';'
#     sql_unmarried='select '+a+', '+f+'('+m+') from unmarried_adults where '+a+' is not null group by '+a+' order by '+a+';'
#     cursor.execute(sql_married)
#     res_married= cursor.fetchall()
#     cursor.execute(sql_unmarried)
#     res_unmarried = cursor.fetchall()
#     dict_result = defaultdict(list)
#     print(res_married,".......")
#     for key,value in res_married:
#         dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))
#     for key,value in res_unmarried:
#         if key not in dict_result:
#             dict_result[key].append(Decimal(1e-10))
#         dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))
#     for key, value in dict_result.items():
#         if len(value)!=2:
#             dict_result[key].append(Decimal(1e-10))
#     print(dict_result)
#     # for key,value in res_unmarried:
#     #     dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))
#     break



# Sharing Based

married_combine_query = 'select ' + ','.join(dimension_attr)
aggregate_query_married = ''
for f in aggregates:
    for m in measure_attr:
        aggregate_query_married+= (','+f+'('+m+') as '+f+'_'+m)
married_combine_query+=aggregate_query_married + ' from married_adults where '
where_married = ''
for a in dimension_attr:
    where_married+=' '+a+' is not null and'
where_married=where_married[:-3]
married_combine_query+=where_married + ' group by '+ ','.join(dimension_attr)+' ;'

unmarried_combine_query = 'select ' + ','.join(dimension_attr)
unmarried_combine_query+=aggregate_query_married + ' from unmarried_adults where '
unmarried_combine_query+=where_married + ' group by '+ ','.join(dimension_attr)+' ;'

# print(married_combine_query)
# print(unmarried_combine_query)

cursor.execute(married_combine_query)
res_married= pd.DataFrame(cursor.fetchall())
cols_married = cursor.description
cursor.execute(unmarried_combine_query)
res_unmarried = pd.DataFrame(cursor.fetchall())
cols_unmarried = cursor.description
# print(cols_unmarried)

res_married.columns = [col[0] for col in cols_married]
res_unmarried.columns = [col[0] for col in cols_unmarried]

# print(res_unmarried)

visualization = []
for f in aggregates:
    for a in dimension_attr:
        for m in measure_attr:
            visualization.append((a,m,f))

#Evaluation - Sharing Based

dict_kl=defaultdict(list)
for a,m,f in visualization:
    attributes_married = defaultdict(list)
    for val in res_married[a].unique():
        # print(val)
        # print(res_married[res_married[a] == val])
        # print(f,m)
        all_values=list(res_married[res_married[a] == val]["{}_{}".format(f,m)]) 
        #print(val_list)
        if f=='sum':
            attributes_married[val]=sum(all_values)
        elif f=='max':
            attributes_married[val]=max(all_values)
        elif f=='min':
            attributes_married[val]=min(all_values)
        elif f=='count':
            attributes_married[val]=sum(all_values)
        elif f=='avg':
            get_counts = list(res_married[res_married[a] == val]["count_{}".format(m)])
            s=0
            t=0
            for i,j in zip(all_values, get_counts):
                s += i*j
                t += j
            if t>0:
                attributes_married[val]=s/t
            else:
                attributes_married[val]=Decimal(1e-10)
    attributes_unmarried = defaultdict(list)
    for val in res_unmarried[a].unique():
        # print(val)
        # print(res_married[res_married[a] == val])
        # print(f,m)
        all_values=list(res_unmarried[res_unmarried[a] == val]["{}_{}".format(f,m)]) 
        #print(val_list)
        if f=='sum':
            attributes_unmarried[val]=sum(all_values)
        elif f=='max':
            attributes_unmarried[val]=max(all_values)
        elif f=='min':
            attributes_unmarried[val]=min(all_values)
        elif f=='count':
            attributes_unmarried[val]=sum(all_values)
        elif f=='avg':
            get_counts = list(res_unmarried[res_unmarried[a] == val]["count_{}".format(m)])
            s=0
            t=0
            for i,j in zip(all_values, get_counts):
                s += i*j
                t += j
            if t>0:
                attributes_unmarried[val]=s/t
            else:
                attributes_unmarried[val]=Decimal(1e-10)
    dict_result = defaultdict(list)
    # print(attributes_married)
    for key,value in attributes_married.items():
        dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))
    for key,value in attributes_unmarried.items():
        if key not in dict_result:
            dict_result[key].append(Decimal(1e-10))
        dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))
    for key, value in dict_result.items():
        if len(value)!=2:
            dict_result[key].append(Decimal(1e-10))
    print(dict_result)
    # for key,value in res_unmarried:
    #     dict_result[key].append(value) if value!=0 else dict_result[key].append(Decimal(1e-10))



