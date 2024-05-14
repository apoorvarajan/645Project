import psycopg2
import pandas as pd
from collections import defaultdict
from decimal import Decimal
import utils,numpy as np
from itertools import product



conn_string='postgresql://femimoljoseph@localhost/SeeDB_Project'
conn = psycopg2.connect(conn_string) 
cursor = conn.cursor() 

measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
aggregates=['sum','avg','max','min','count']

import time

start_time = time.time()

# Combining target and reference and aggregates view query
'''g1 will be kept as 1 for all married adults and 0 for unmarried'''

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
    a1, a2, ... an,g1
'''

def combiningTargetandReference():

    combined_query = 'select ' + ','.join(dimension_attr)
    combined_query+=aggregate_query + ", CASE  WHEN marital_status = 'Married' THEN 1 ELSE 0 END as g1 from total_adults where "
    combined_query+=where_married + ' group by '+ ','.join(dimension_attr)+',g1 ;'
    cursor.execute(combined_query)
    res_TargetRef= pd.DataFrame(cursor.fetchall())
    cols_TargetRef= cursor.description
    return res_TargetRef,cols_TargetRef


aggregate_query=''
for f in aggregates:
    for m in measure_attr:
        aggregate_query+= (','+f+'('+m+') as '+f+'_'+m)

where_married = ''
for a in dimension_attr:
    where_married+=' '+a+' is not null and'
where_married=where_married[:-3]




# Extra credit : Combine MultipleGROUPBYs and  aggregates
'''SELECT 
    (a1, a2, ... an),
    (f1_m1, f1_m2, ... f1_mn, ... f2_m1, f2_m2, ... fn_mn)
FROM 
    married_adults 
 GROUP BY grouping sets(a1, a2, ... an ) ;
'''
'''
SELECT 
    (a1, a2, ... an),
    (f1_m1, f1_m2, ... f1_mn, ... f2_m1, f2_m2, ... fn_mn)
FROM 
    unmarried_adults 
 GROUP BY  grouping sets(a1, a2, ... an ) ;
'''

def combiningMultipleGroupbyMultiAgg(table):

    combined_query = 'select ' + ','.join(dimension_attr)
    combined_query+=aggregate_query + " from {}  where ".format(table)
    combined_query+=where_married + ' group by grouping sets( '+ ','.join(dimension_attr)+') ;'
    cursor.execute(combined_query)
    res_= pd.DataFrame(cursor.fetchall())
    cols_= cursor.description
    return res_,cols_


#Evaluation - Sharing Based

def EvaluationViews(target_df,reference_df):
    ViewScores = {}
    for i, view in views.items():
        f,m,a = view
        vName='{}_{}'.format(f,m)
        married = target_df.loc[target_df[a].notnull(), [a,vName]]
        unmarried = reference_df.loc[reference_df[a].notnull(), [a, vName]]
        temp = married.join(unmarried.set_index(a), on=a, how="inner", lsuffix='_target', rsuffix='_reference')
        #print("Temp:",temp)
        TarValues = temp[vName+'_target'.format(i)].values
        RefValues = temp[vName+'_reference'.format(i)].values
        #print("TarValues:", TarValues)
        UtiltiScore = utils.KL_divergence(TarValues, RefValues)
        #print("UtiltiScore:", UtiltiScore)
        ViewScores[i] = UtiltiScore
        
    SortedViewScores = sorted(ViewScores.items(), key=lambda x: x[1], reverse=True)
    return SortedViewScores

views = utils.get_all_views()

#Evaluation for combining  Target & Ref and aggregates

data,cols = combiningTargetandReference()
data.columns = [col[0] for col in cols]
target1 = data[data['g1'] == 1].copy()  # Filter rows where g1 is 1 or married
reference1 = data[data['g1'] == 0].copy() 
print("target_df:",target1)
print("reference_df:",reference1)
SortedViewScores=EvaluationViews(target1,reference1)
print("top 5 scores :",SortedViewScores[:5])
utils.top_5_AggViews([rank[0] for rank in SortedViewScores],views)

#Evaluation for combining Multiple Groupby and Target & Ref and aggregates

TData,TCols=combiningMultipleGroupbyMultiAgg('married_adults')
TData.columns = [col[0] for col in TCols]
print("TData:",TData)
RData,RCols=combiningMultipleGroupbyMultiAgg('unmarried_adults')
RData.columns = [col[0] for col in RCols]
print("RData:",RData)
SortedViewScores=EvaluationViews(TData,RData)
print("top 5 scores :",SortedViewScores[:5])
utils.top_5_AggViews([rank[0] for rank in SortedViewScores],views)

end_time = time.time()
execution_time = end_time - start_time
print("Execution time:", execution_time, "seconds")

