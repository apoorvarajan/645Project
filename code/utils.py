from itertools import product
import math
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
import psycopg2
import pandas as pd

conn_string='postgresql://femimoljoseph@localhost/SeeDB_Project'
conn = psycopg2.connect(conn_string) 
cursor = conn.cursor() 

measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
aggregates=['sum','avg','max','min','count']

def get_prune_query(start_index_target,end_index_target, start_index_ref,end_index_ref):
    rows_target = """
    WITH RankedRowsm AS (
        SELECT *,
               ROW_NUMBER() OVER () AS row_num
        FROM married_adults
    )
"""

    rows_ref = """
    WITH RankedRowsum AS (
        SELECT *,
               ROW_NUMBER() OVER () AS row_num
        FROM unmarried_adults
    )
"""
    target_query = rows_target + 'select ' + ','.join(dimension_attr)
    aggregate_query_married = ''
    for f in aggregates:
        for m in measure_attr:
            aggregate_query_married+= (','+f+'('+m+') as '+f+'_'+m)
    target_query+=aggregate_query_married + ' from RankedRowsm where '
    where_married = ''
    for a in dimension_attr:
        where_married+=' '+a+' is not null and'
    where_married=where_married[:-3]
    target_query += where_married + f' AND row_num BETWEEN {start_index_target} AND {end_index_target} GROUP BY ' + ','.join(dimension_attr) + ' ;'

    reference_query = rows_ref + 'select ' + ','.join(dimension_attr)
    reference_query+=aggregate_query_married + ' from RankedRowsum where '
    reference_query += where_married + f' AND row_num BETWEEN {start_index_ref} AND {end_index_ref} GROUP BY ' + ','.join(dimension_attr) + ' ;'

    return target_query, reference_query

def get_confidence_interval(m,N,delta):
    epsilon=math.sqrt((0.5/m)*(1-((m-1)/N))*(2*math.log(math.log(m))+math.log((math.pi**2)/(3*delta))))
    return epsilon

def KL_divergence(target, reference):
    #Compute dsitance between probability distributions of target and reference

    target = np.asarray(target, dtype=np.float64)
    reference = np.asarray(reference, dtype=np.float64)
    p = target/np.sum(target)
    q = reference/np.sum(reference)
    replace = 0.00008
    p[p == 0] = replace
    q[q == 0] = replace
    if len(p) > len(q):
        p = np.random.choice(p, len(q))
    elif len(q) >= len(p):
        q = np.random.choice(q, len(p))
    return np.sum(p * np.log(p / q)) 
   
    
def get_all_views():
    #Computes all the possible combination views based on measure, dimension and aggregate values
    views = {k: v for k,v in enumerate(list(product(aggregates,measure_attr,dimension_attr)))}
    return views

def execute_query(query):
    conn_string='postgresql://priyankavirupakshappadevoor@localhost/priyankavirupakshappadevoor'
    conn = psycopg2.connect(conn_string) 
    cursor = conn.cursor() 
    cursor.execute(query)
    result= pd.DataFrame(cursor.fetchall())
    target_cols = cursor.description
    result.columns = [col[0] for col in target_cols]
    return result

def EvaluationViews(views, view_scores, target_df, reference_df):
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
        UtiltiScore = KL_divergence(TarValues, RefValues)
        #print("UtiltiScore:", UtiltiScore)
        view_scores[i].append(UtiltiScore)

    return view_scores

def top_5_AggViews(ranking,views):
    print("Top 5 views:",ranking[:5])
    for viewid in ranking[:5]:
        f, m, a = views[viewid]
        print("View {} :{}".format(viewid,views[viewid]))
        query = "SELECT {}, {}({}) FROM married_adults GROUP BY {};".format(a, f, m, a)
        cursor.execute(query)
        tgt_rows = cursor.fetchall()

        query = "SELECT {}, {}({}) FROM unmarried_adults GROUP BY {};".format(a, f, m, a)
        cursor.execute(query)
        ref_rows = cursor.fetchall()

        t_Dict = dict(tgt_rows)
        r_Dict = dict(ref_rows)

        for k in t_Dict.keys():
            if k not in r_Dict:
                r_Dict[k] = 0

        for k in r_Dict.keys():
            if k not in t_Dict:
                t_Dict[k] = 0
        display_Graph(t_Dict, r_Dict, (a, m, f))

def display_Graph(target_data, ref_data, view_tuple):
    n_groups = len(target_data)
    group_by, measure, function = view_tuple

    means_target = target_data.values()
    means_ref = ref_data.values()

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.35
    opacity = 0.8

    plt.bar(index, means_target, bar_width,
                     alpha=opacity,
                     color='#800080',
                     label='married')

    plt.bar(index + bar_width, means_ref, bar_width,
                     alpha=opacity,
                     color='#FFB6C1',
                     label='unmarried')

    plt.xlabel('{}'.format(group_by))
    plt.ylabel('{}({})'.format(function, measure))
    plt.xticks(index + bar_width, target_data.keys(), rotation=45)
    plt.legend(title="Married")

    plt.tight_layout()
    plt.show()
