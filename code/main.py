import psycopg2
import pandas as pd
from sqlalchemy import create_engine 
from collections import defaultdict
from decimal import Decimal


conn_string='postgresql://femimoljoseph@localhost/SeeDB_Project'
conn = psycopg2.connect(conn_string) 
cursor = conn.cursor() 

measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
aggregates=['sum','avg','max','min','count']





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


#Combining target and reference view query
# g1 will be kept as 1 for all married adults and 0 for unmarried
'''SELECT 
    (a1, a2, ... an),
    (f1_m1, f1_m2, ... f1_mn, ... f2_m1, f2_m2, ... fn_mn),
    CASE 
        WHEN marital_status = 'married' THEN 1 
        ELSE 0 
    END as g1
FROM 
    total_adults 
GROUP BY 
    a1, a2, ... an, g1
'''

def combiningTargetandReference():
    combined_query = 'select ' + ','.join(dimension_attr)
    combined_query+=aggregate_query + ", CASE  WHEN marital_status = 'Married' THEN 1 ELSE 0 END as g1 from total_adults where "
    combined_query+=where_married + ' group by '+ ','.join(dimension_attr)+',g1 ;'
    cursor.execute(combined_query)
    res_TargetRef= pd.DataFrame(cursor.fetchall())
    
    cols_TargetRef= cursor.description
    #print("cols_TargetRef:",cols_TargetRef)
    return res_TargetRef,cols_TargetRef

aggregate_query=''
for f in aggregates:
    for m in measure_attr:
        aggregate_query+= (','+f+'('+m+') as '+f+'_'+m)

data,cols = combiningTargetandReference()
data.columns = [col[0] for col in cols]
target_df = data[data['g1'] == 1].copy()  # Filter rows where g1 is 1 or married
reference_df = data[data['g1'] == 0].copy() 
# print("target_df:",target_df)
# print("reference_df:",reference_df)





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



