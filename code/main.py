import psycopg2
import pandas as pd
from sqlalchemy import create_engine 
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



# 2. Combining target and reference and aggregates view query
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
    GROUPING SETS(a1, a2, ... an,g1)
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

data,cols = combiningTargetandReference()
data.columns = [col[0] for col in cols]
target1 = data[data['g1'] == 1].copy()  # Filter rows where g1 is 1 or married
reference1 = data[data['g1'] == 0].copy() 
print("target_df:",target1)
print("reference_df:",reference1)


#3. Extra credit : Combine MultipleGROUPBYs and  target and Reference aggregates
'''SELECT 
    (a1, a2, ... an),
    (f1_m1, f1_m2, ... f1_mn, ... f2_m1, f2_m2, ... fn_mn)
FROM 
    total_adults 
 GROUP BY  ;
'''
def combiningMultipleGroupbyTargetRef():

    combined_query = 'select ' + ','.join(dimension_attr)
    combined_query+=aggregate_query + ", CASE  WHEN marital_status = 'Married' THEN 1 ELSE 0 END as g1 from total_adults where "
    combined_query+=where_married + ' group by grouping sets( '+ ','.join(dimension_attr)+',g1 ) ;'
    cursor.execute(combined_query)
    res_TargetRef= pd.DataFrame(cursor.fetchall())
    cols_TargetRef= cursor.description
    return res_TargetRef,cols_TargetRef




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

#top 5 
def top_5_AggViews(ranking):
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
        utils.display_Graph(t_Dict, r_Dict, (a, m, f))

#views={0: ('sum', 'age', 'workclass'), 1: ('sum', 'age', 'education'), 2: ('sum', 'age', 'occupation'), 3: ('sum', 'age', 'relationship'), 4: ('sum', 'age', 'race'), 5: ('sum', 'age', 'sex'), 6: ('sum', 'age', 'native_country'), 7: ('sum', 'age', 'income'), 8: ('sum', 'fnlwgt', 'workclass'), 9: ('sum', 'fnlwgt', 'education'), 10: ('sum', 'fnlwgt', 'occupation'), 11: ('sum', 'fnlwgt', 'relationship'), 12: ('sum', 'fnlwgt', 'race'), 13: ('sum', 'fnlwgt', 'sex'), 14: ('sum', 'fnlwgt', 'native_country'), 15: ('sum', 'fnlwgt', 'income'), 16: ('sum', 'education_num', 'workclass'), 17: ('sum', 'education_num', 'education'), 18: ('sum', 'education_num', 'occupation'), 19: ('sum', 'education_num', 'relationship'), 20: ('sum', 'education_num', 'race'), 21: ('sum', 'education_num', 'sex'), 22: ('sum', 'education_num', 'native_country'), 23: ('sum', 'education_num', 'income'), 24: ('sum', 'capital_gain', 'workclass'), 25: ('sum', 'capital_gain', 'education'), 26: ('sum', 'capital_gain', 'occupation'), 27: ('sum', 'capital_gain', 'relationship'), 28: ('sum', 'capital_gain', 'race'), 29: ('sum', 'capital_gain', 'sex'), 30: ('sum', 'capital_gain', 'native_country'), 31: ('sum', 'capital_gain', 'income'), 32: ('sum', 'capital_loss', 'workclass'), 33: ('sum', 'capital_loss', 'education'), 34: ('sum', 'capital_loss', 'occupation'), 35: ('sum', 'capital_loss', 'relationship'), 36: ('sum', 'capital_loss', 'race'), 37: ('sum', 'capital_loss', 'sex'), 38: ('sum', 'capital_loss', 'native_country'), 39: ('sum', 'capital_loss', 'income'), 40: ('sum', 'hours_per_week', 'workclass'), 41: ('sum', 'hours_per_week', 'education'), 42: ('sum', 'hours_per_week', 'occupation'), 43: ('sum', 'hours_per_week', 'relationship'), 44: ('sum', 'hours_per_week', 'race'), 45: ('sum', 'hours_per_week', 'sex'), 46: ('sum', 'hours_per_week', 'native_country'), 47: ('sum', 'hours_per_week', 'income'), 48: ('avg', 'age', 'workclass'), 49: ('avg', 'age', 'education'), 50: ('avg', 'age', 'occupation'), 51: ('avg', 'age', 'relationship'), 52: ('avg', 'age', 'race'), 53: ('avg', 'age', 'sex'), 54: ('avg', 'age', 'native_country'), 55: ('avg', 'age', 'income'), 56: ('avg', 'fnlwgt', 'workclass'), 57: ('avg', 'fnlwgt', 'education'), 58: ('avg', 'fnlwgt', 'occupation'), 59: ('avg', 'fnlwgt', 'relationship'), 60: ('avg', 'fnlwgt', 'race'), 61: ('avg', 'fnlwgt', 'sex'), 62: ('avg', 'fnlwgt', 'native_country'), 63: ('avg', 'fnlwgt', 'income'), 64: ('avg', 'education_num', 'workclass'), 65: ('avg', 'education_num', 'education'), 66: ('avg', 'education_num', 'occupation'), 67: ('avg', 'education_num', 'relationship'), 68: ('avg', 'education_num', 'race'), 69: ('avg', 'education_num', 'sex'), 70: ('avg', 'education_num', 'native_country'), 71: ('avg', 'education_num', 'income'), 72: ('avg', 'capital_gain', 'workclass'), 73: ('avg', 'capital_gain', 'education'), 74: ('avg', 'capital_gain', 'occupation'), 75: ('avg', 'capital_gain', 'relationship'), 76: ('avg', 'capital_gain', 'race'), 77: ('avg', 'capital_gain', 'sex'), 78: ('avg', 'capital_gain', 'native_country'), 79: ('avg', 'capital_gain', 'income'), 80: ('avg', 'capital_loss', 'workclass'), 81: ('avg', 'capital_loss', 'education'), 82: ('avg', 'capital_loss', 'occupation'), 83: ('avg', 'capital_loss', 'relationship'), 84: ('avg', 'capital_loss', 'race'), 85: ('avg', 'capital_loss', 'sex'), 86: ('avg', 'capital_loss', 'native_country'), 87: ('avg', 'capital_loss', 'income'), 88: ('avg', 'hours_per_week', 'workclass'), 89: ('avg', 'hours_per_week', 'education'), 90: ('avg', 'hours_per_week', 'occupation'), 91: ('avg', 'hours_per_week', 'relationship'), 92: ('avg', 'hours_per_week', 'race'), 93: ('avg', 'hours_per_week', 'sex'), 94: ('avg', 'hours_per_week', 'native_country'), 95: ('avg', 'hours_per_week', 'income'), 96: ('max', 'age', 'workclass'), 97: ('max', 'age', 'education'), 98: ('max', 'age', 'occupation'), 99: ('max', 'age', 'relationship'), 100: ('max', 'age', 'race'), 101: ('max', 'age', 'sex'), 102: ('max', 'age', 'native_country'), 103: ('max', 'age', 'income'), 104: ('max', 'fnlwgt', 'workclass'), 105: ('max', 'fnlwgt', 'education'), 106: ('max', 'fnlwgt', 'occupation'), 107: ('max', 'fnlwgt', 'relationship'), 108: ('max', 'fnlwgt', 'race'), 109: ('max', 'fnlwgt', 'sex'), 110: ('max', 'fnlwgt', 'native_country'), 111: ('max', 'fnlwgt', 'income'), 112: ('max', 'education_num', 'workclass'), 113: ('max', 'education_num', 'education'), 114: ('max', 'education_num', 'occupation'), 115: ('max', 'education_num', 'relationship'), 116: ('max', 'education_num', 'race'), 117: ('max', 'education_num', 'sex'), 118: ('max', 'education_num', 'native_country'), 119: ('max', 'education_num', 'income'), 120: ('max', 'capital_gain', 'workclass'), 121: ('max', 'capital_gain', 'education'), 122: ('max', 'capital_gain', 'occupation'), 123: ('max', 'capital_gain', 'relationship'), 124: ('max', 'capital_gain', 'race'), 125: ('max', 'capital_gain', 'sex'), 126: ('max', 'capital_gain', 'native_country'), 127: ('max', 'capital_gain', 'income'), 128: ('max', 'capital_loss', 'workclass'), 129: ('max', 'capital_loss', 'education'), 130: ('max', 'capital_loss', 'occupation'), 131: ('max', 'capital_loss', 'relationship'), 132: ('max', 'capital_loss', 'race'), 133: ('max', 'capital_loss', 'sex'), 134: ('max', 'capital_loss', 'native_country'), 135: ('max', 'capital_loss', 'income'), 136: ('max', 'hours_per_week', 'workclass'), 137: ('max', 'hours_per_week', 'education'), 138: ('max', 'hours_per_week', 'occupation'), 139: ('max', 'hours_per_week', 'relationship'), 140: ('max', 'hours_per_week', 'race'), 141: ('max', 'hours_per_week', 'sex'), 142: ('max', 'hours_per_week', 'native_country'), 143: ('max', 'hours_per_week', 'income'), 144: ('min', 'age', 'workclass'), 145: ('min', 'age', 'education'), 146: ('min', 'age', 'occupation'), 147: ('min', 'age', 'relationship'), 148: ('min', 'age', 'race'), 149: ('min', 'age', 'sex'), 150: ('min', 'age', 'native_country'), 151: ('min', 'age', 'income'), 152: ('min', 'fnlwgt', 'workclass'), 153: ('min', 'fnlwgt', 'education'), 154: ('min', 'fnlwgt', 'occupation'), 155: ('min', 'fnlwgt', 'relationship'), 156: ('min', 'fnlwgt', 'race'), 157: ('min', 'fnlwgt', 'sex'), 158: ('min', 'fnlwgt', 'native_country'), 159: ('min', 'fnlwgt', 'income'), 160: ('min', 'education_num', 'workclass'), 161: ('min', 'education_num', 'education'), 162: ('min', 'education_num', 'occupation'), 163: ('min', 'education_num', 'relationship'), 164: ('min', 'education_num', 'race'), 165: ('min', 'education_num', 'sex'), 166: ('min', 'education_num', 'native_country'), 167: ('min', 'education_num', 'income'), 168: ('min', 'capital_gain', 'workclass'), 169: ('min', 'capital_gain', 'education'), 170: ('min', 'capital_gain', 'occupation'), 171: ('min', 'capital_gain', 'relationship'), 172: ('min', 'capital_gain', 'race'), 173: ('min', 'capital_gain', 'sex'), 174: ('min', 'capital_gain', 'native_country'), 175: ('min', 'capital_gain', 'income'), 176: ('min', 'capital_loss', 'workclass'), 177: ('min', 'capital_loss', 'education'), 178: ('min', 'capital_loss', 'occupation'), 179: ('min', 'capital_loss', 'relationship'), 180: ('min', 'capital_loss', 'race'), 181: ('min', 'capital_loss', 'sex'), 182: ('min', 'capital_loss', 'native_country'), 183: ('min', 'capital_loss', 'income'), 184: ('min', 'hours_per_week', 'workclass'), 185: ('min', 'hours_per_week', 'education'), 186: ('min', 'hours_per_week', 'occupation'), 187: ('min', 'hours_per_week', 'relationship'), 188: ('min', 'hours_per_week', 'race'), 189: ('min', 'hours_per_week', 'sex'), 190: ('min', 'hours_per_week', 'native_country'), 191: ('min', 'hours_per_week', 'income'), 192: ('count', 'age', 'workclass'), 193: ('count', 'age', 'education'), 194: ('count', 'age', 'occupation'), 195: ('count', 'age', 'relationship'), 196: ('count', 'age', 'race'), 197: ('count', 'age', 'sex'), 198: ('count', 'age', 'native_country'), 199: ('count', 'age', 'income'), 200: ('count', 'fnlwgt', 'workclass'), 201: ('count', 'fnlwgt', 'education'), 202: ('count', 'fnlwgt', 'occupation'), 203: ('count', 'fnlwgt', 'relationship'), 204: ('count', 'fnlwgt', 'race'), 205: ('count', 'fnlwgt', 'sex'), 206: ('count', 'fnlwgt', 'native_country'), 207: ('count', 'fnlwgt', 'income'), 208: ('count', 'education_num', 'workclass'), 209: ('count', 'education_num', 'education'), 210: ('count', 'education_num', 'occupation'), 211: ('count', 'education_num', 'relationship'), 212: ('count', 'education_num', 'race'), 213: ('count', 'education_num', 'sex'), 214: ('count', 'education_num', 'native_country'), 215: ('count', 'education_num', 'income'), 216: ('count', 'capital_gain', 'workclass'), 217: ('count', 'capital_gain', 'education'), 218: ('count', 'capital_gain', 'occupation'), 219: ('count', 'capital_gain', 'relationship'), 220: ('count', 'capital_gain', 'race'), 221: ('count', 'capital_gain', 'sex'), 222: ('count', 'capital_gain', 'native_country'), 223: ('count', 'capital_gain', 'income'), 224: ('count', 'capital_loss', 'workclass'), 225: ('count', 'capital_loss', 'education'), 226: ('count', 'capital_loss', 'occupation'), 227: ('count', 'capital_loss', 'relationship'), 228: ('count', 'capital_loss', 'race'), 229: ('count', 'capital_loss', 'sex'), 230: ('count', 'capital_loss', 'native_country'), 231: ('count', 'capital_loss', 'income'), 232: ('count', 'hours_per_week', 'workclass'), 233: ('count', 'hours_per_week', 'education'), 234: ('count', 'hours_per_week', 'occupation'), 235: ('count', 'hours_per_week', 'relationship'), 236: ('count', 'hours_per_week', 'race'), 237: ('count', 'hours_per_week', 'sex'), 238: ('count', 'hours_per_week', 'native_country'), 239: ('count', 'hours_per_week', 'income')}
#top_5_views= [(14, 44984409277802.805), (12, 38288678846414.13), (13, 32942311117702.027), (8, 30525423567250.36), (15, 17781030620990.69)]

views = {k: v for k,v in enumerate(list(product(aggregates,measure_attr,dimension_attr)))}

#Evaluation for combining  Target & Ref and aggregates
SortedViewScores=EvaluationViews(target1,reference1)
top_5_AggViews([rank[0] for rank in SortedViewScores])

#Evaluation for combining Multiple Groupby and Target & Ref and aggregates
resMarried,resUnmarried=combiningMultipleGroupbyTargetRef()
SortedViewScores=EvaluationViews(resMarried,resUnmarried)
top_5_AggViews([rank[0] for rank in SortedViewScores])

