# coding: utf-8

import json
import pandas as pd
from sqlalchemy import create_engine

# загрузить данные из предыдущей версии
get_ipython().system('scp -r smedintsev@mysrv:/home/smedintsev/projects/taxi_analyser/data/taxi_Yandex_Marino_20181228_2334.json /Users/medintsev/tmp/taxi_Yandex_Marino_20181228_2334_20181229_0137.json')


SOURCE_PATH = '/Users/medintsev/tmp/taxi_Yandex_Marino_20181228_2334_20181229_0137.json'
RESULT_PATH = '/data/yandex.taxi.prices.db'


engine = create_engine('sqlite://' + RESULT_PATH, echo=False)

with open(SOURCE_PATH, 'r') as f:
    result_raw = f.readlines()
    
results = []
for element in result_raw:
    results.append(json.loads(element))
    
    
start_point_desc = pd.DataFrame(
    [
        {
            'data.start_point': '37.58928,55.734210',
            'data.start_point_desc': 'office'
        },
        {
            'data.start_point': '37.74242,55.644689',
            'data.start_point_desc': 'home'
        },
    ]
)

end_point_desc = pd.DataFrame(
    [
        {
            'data.end_point': '37.58928,55.734210',
            'data.end_point_desc': 'office'
        },
        {
            'data.end_point': '37.74242,55.644689',
            'data.end_point_desc': 'home'
        },
    ]
)

result_df = pd.merge(
    left=pd.DataFrame(results),
    right=start_point_desc,
    how='left',
    on='data.start_point'
)

result_df = pd.merge(
    left=result_df,
    right=end_point_desc,
    how='left',
    on='data.end_point'
)

result_df_to_db = result_df[[
    'data.ts',
    'data.surge',
    'data.class_level',
    'data.class_name',
    'data.class_text',
    'data.distance',
    'data.time',
    'data.waiting_time',
    'data.currency',
    'data.min_price',
    'data.price',
    'data.price_text',
    'data.start_point',
    'data.start_point_desc',
    'data.end_point',
    'data.end_point_desc'    
]]

result_df_to_db.rename(inplace=True, columns={
    'data.ts': 'request_datetime',
    'data.surge': 'surge',
    'data.class_level': 'class_level',
    'data.class_name': 'class_name',
    'data.class_text': 'class_text',
    'data.distance': 'distance',
    'data.time': 'time',
    'data.waiting_time': 'waiting_time',
    'data.currency': 'currency',
    'data.min_price': 'min_price',
    'data.price': 'price',
    'data.price_text': 'price_text',
    'data.start_point': 'start_point',
    'data.start_point_desc': 'start_point_desc',
    'data.end_point': 'end_point',
    'data.end_point_desc': 'end_point_desc'
})

result_df_to_db.to_sql(name='prices', con=engine, index=False, if_exists='replace')









