from pprint import pprint

import pandas as pd
import pandasql as ps

data_file = pd.read_csv('db_table.csv', delimiter=',',
                        names=['date', 'updateDate', 'code1',
                               'code2', 'code3', 'value', 'source']
                        )
'''Step 0'''
siCode_sql = '''
        SELECT
        CASE WHEN bitCode == 1 
        THEN
            CASE WHEN code3 == 'AP' OR code3 == 'AH' THEN 'A' 
            WHEN code3 == 'PRD' THEN 'B' 
            WHEN code3 == 'YLD' THEN 'BpA' 
            ELSE 0 END 
        ELSE
            CASE WHEN code3 == 'AP' or code3 == 'AH' THEN 'H' 
            WHEN code3 == 'PRD' THEN 'T' 
            WHEN code3 == 'YLD' THEN 'TpH' 
            ELSE 0 END 
        END AS siCode 
        FROM data_file
'''
bitCode_sql = ''' SELECT CASE WHEN source == 'S1' THEN 1 ELSE 0 END AS bitCode FROM data_file'''

data_file['bitCode'] = ps.sqldf(bitCode_sql)
data_file['siCode'] = ps.sqldf(siCode_sql)
data_file.to_csv('main.csv', index=False)

'''Step 1'''
code: dict = eval(
    input('Please enter something like request from the frontend side:\nFor example: {"code1": "shC", "code2": "C"}'))
code1, code2 = code['code1'], code['code2']
dict_values = {}
data_dict = {}

if isinstance(code, dict):
    filtered_data = data_file[(data_file.code1.str.match(code1)) & (data_file.code2.str.contains(code2))]
    dict_values[(code1, code2)] = filtered_data.source.values
    pprint(dict_values)  # return --> {(#G_code1#, #G_code2#): [#source1#, #source2#, ..., #sourceN#]}

    '''Step 2'''
    all_sources = set(*dict_values.values())
    for source in all_sources:
        source_filtered_data = filtered_data[filtered_data.source == source].values
        data_dict[(code1, code2, source)] = source_filtered_data

    pprint(data_dict)  # return --> {(#G_code1#, #G_code2#, #sourceK#): [data rows]}
