import psycopg2
import numpy as np
import pandas as pd
from sqlalchemy import create_engine 
from collections import defaultdict
from decimal import Decimal
import utils

N = 5 #Number of phases
delta = 0.05

conn_string='postgresql://priyankavirupakshappadevoor@localhost/priyankavirupakshappadevoor'
conn = psycopg2.connect(conn_string) 
cursor = conn.cursor() 

total_married_records = 26092  # Total number of records in married table
total_unmarried_records = 22749  # Total number of records in unmarried table

# Calculate the size of each partition for married and unmarried tables
partition_size_married = total_married_records // N
partition_size_unmarried = total_unmarried_records // N

for i in range(N):
    # Calculate start and end index for married table
    start_index_target = 1 + i * partition_size_married
    end_index_target = start_index_target + partition_size_married - 1

    # Calculate start and end index for unmarried table
    start_index_ref = 1 + i * partition_size_unmarried
    end_index_ref = start_index_ref + partition_size_unmarried - 1

    target_query, reference_query = utils.get_prune_query(start_index_target,end_index_target, start_index_ref,end_index_ref)
    cursor.execute(target_query)
    target_result= pd.DataFrame(cursor.fetchall())
    target_cols = cursor.description
    target_result.columns = [col[0] for col in target_cols]

    cursor.execute(reference_query)
    reference_result= pd.DataFrame(cursor.fetchall())
    reference_cols = cursor.description
    reference_result.columns = [col[0] for col in reference_cols]

    # Convert target and reference results to probability distributions
    target_prob = target_result[target_result.columns[-1]].values / np.sum(target_result[target_result.columns[-1]].values)
    reference_prob = reference_result[reference_result.columns[-1]].values / np.sum(reference_result[reference_result.columns[-1]].values)

    utility = utils.KL_divergence(target_prob,reference_prob)
    print(utility)


