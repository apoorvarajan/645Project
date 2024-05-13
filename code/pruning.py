import psycopg2
import numpy as np
import pandas as pd
import utils
import time

start_time = time.time()

N = 10 #Number of phases
delta = 0.05
top = 5

total_married_records = 26092  # Total number of records in married table
total_unmarried_records = 22749  # Total number of records in unmarried table

# Calculate the size of each partition for married and unmarried tables
partition_size_married = total_married_records // N
partition_size_unmarried = total_unmarried_records // N

views = utils.get_all_views()
view_scores = {k: [] for k, v in views.items()}
result_views = {k: v for k, v in views.items()}

for i in range(N):
    # Calculate start and end index for married table
    start_index_target = 1 + i * partition_size_married
    end_index_target = start_index_target + partition_size_married - 1

    # Calculate start and end index for unmarried table
    start_index_ref = 1 + i * partition_size_unmarried
    end_index_ref = start_index_ref + partition_size_unmarried - 1

    target_query, reference_query = utils.get_prune_query(start_index_target,end_index_target, start_index_ref,end_index_ref
                                                          )
    target_result = utils.execute_query(target_query)
    reference_result = utils.execute_query(reference_query)

    view_scores = utils.EvaluationViews(views, view_scores, target_result, reference_result)
    if i > 0:
        m = i+1 
        epsilon = utils.get_confidence_interval(m,N,delta)
        result_view_scores = [(k, v) for k, v in view_scores.items() if k in result_views]
        views_ranking = sorted(result_view_scores, key=lambda x: np.mean(x[1]), reverse=True)
        lower_bound = np.mean(views_ranking[top][1]) - epsilon

        for k, v in view_scores.items():
            if k in result_views and np.mean(v) + epsilon < lower_bound:
                del result_views[k]
            
print('Total number of pruned views: {}.'.format(len(views)-len(result_views)))
utils.top_5_AggViews([r[0] for r in views_ranking] ,views)
elapsed_time = time.time() - start_time
print(elapsed_time)