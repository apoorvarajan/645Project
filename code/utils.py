from itertools import product
import math
import numpy as np

def get_prune_query(start_index_target,end_index_target, start_index_ref,end_index_ref):
    measure_attr=['age','fnlwgt', 'education_num','capital_gain','capital_loss','hours_per_week']
    dimension_attr=['workclass','education','occupation','relationship','race','sex','native_country','income']
    aggregates=['sum','avg','max','min','count']
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

def get_confidence_interval(m,N,delta):
    epsilon=math.sqrt((0.5/m)*(1-((m-1)/N))*(2*math.log(math.log(m))+math.log((math.pi**2)/(3*delta))))
    return epsilon

def KL_divergence(target, reference):
    #Compute dsitance between probability distributions of target and reference
    target = np.asarray(target, dtype=np.float64)
    target = target.reshape(-1) 
    reference = np.asarray(reference, dtype=np.float64)
    reference = reference.reshape(-1)
    print(target.shape)
    return np.sum(np.where(target != 0, target * np.log(target / reference), 0))

    
def get_all_views(measure_attr,dimension_attr,aggregates):
    #Computes all the possible combination views based on measure, dimension and aggregate values
    views = []
    for a in dimension_attr:
        for m in measure_attr:
            for f in aggregates:
                views.append((a,f,m))
    return views

