# coding: utf-8

from urllib import parse
import requests
from datetime import datetime
from time import sleep
import random
import os.path

from pandas.io.json import json_normalize
import json
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError
from lxml.html import fromstring

from sqlalchemy import create_engine



RESULT_PATH = '/data/yandex.taxi.prices.db'



COORD_YANDEX_RR = '37.58928,55.734210'
COORD_MY_HOME   = '37.74242,55.644689' # марьино

result_path = './data/log/taxi_price_collector_log_{}.json'.format(datetime.now().strftime("%Y%m%d"))

DELAY_MIN = 30
DELAY_MAX = 90

engine = create_engine('sqlite://' + RESULT_PATH, echo=False)



def write_results_to_db(df):
    df_to_db = df[[
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

    df_to_db.rename(inplace=True, columns={
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

    df_to_db.to_sql(name='prices', con=engine, index=False, if_exists='append')




# https://www.scrapehero.com/how-to-rotate-proxies-and-ip-addresses-using-python-3/
def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            #Grabbing IP and corresponding PORT
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies

def print_log_message(text):
    print('[{}]: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), text))


def get_taxi_data(csrf_token, headers, start_point, start_point_desc, end_point, end_point_desc, come_back=False, come_back_sleep_sec=0):
    base_url = 'https://yandex.ru/maps/api/taxi?ajax=1&{params}'
    
    url = base_url.format(
        params=parse.urlencode({
            'csrfToken': csrf_token,
            'lang': 'ru',
            'route[0]': start_point,
            'route[1]': end_point
        })
    )
    response = requests.get(url=url, headers=headers, proxies={"http": PROXY, "https": PROXY})
    result = response.json()
    if result.get('data'):
        result['data'].update(
            {
                'ts': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'start_point': start_point,
                'start_point_desc': start_point_desc,
                'end_point': end_point,
                'end_point_desc': end_point_desc
            }
        )
    else:
        result.update({'data':
            {
                'ts': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'start_point': start_point,
                'start_point_desc': start_point_desc,
                'end_point': end_point,
                'end_point_desc': end_point_desc
            }
        })
        
    results = [json_normalize(result)]
    if come_back:
        sleep(come_back_sleep_sec)
        results.append(
            get_taxi_data(
                csrf_token=csrf_token,
                headers=headers,
                start_point=end_point,
                start_point_desc=end_point_desc,
                end_point=start_point,
                end_point_desc=start_point_desc
            )
        )
        
    return pd.concat(results, axis=0)

PROXY = ''
def choose_new_proxy(proxies):
    if len(proxies) < 2:
        proxies += get_proxies()

    PROXY = proxies.pop(0)
    print_log_message('Новый адрес прокси:\t{}. Осталось:\t{} адресов (при обнулении, автоматически загрузятся новые)'.format(PROXY, len(proxies)))

    return None


print_log_message('Пробуем получить список прокси...')
proxies = []
choose_new_proxy(proxies)
# print_log_message('Список прокси получили. Длина списка:\t{}'.format(len(proxies)))

def get_tokens():
    while True:
        try:
            response = requests.get('https://yandex.ru/maps/213/moscow/',proxies={"http": PROXY, "https": PROXY})
            break
        except ConnectionError:
            print_log_message('Прокси: {}. Произошла ошибка подключения при получении ключей. Пробуем снова...'.format(PROXY))
            choose_new_proxy(proxies)

    yandexuid = response.cookies['yandexuid']

    soup = BeautifulSoup(response.content,'lxml')
    config_raw = soup.select('script[class="config-view"]')
    config = json.loads(config_raw[0].text)
    csrfToken = config['csrfToken']

    return yandexuid, csrfToken

yandexuid, csrfToken = get_tokens()

print_log_message('\n\tКлючи получены:\n\t\tyandexuid:\t{}\n\t\tcsrfToken:\t{}'.format(
    yandexuid, csrfToken
))



while True:

    try:
        result_df = get_taxi_data(
            csrf_token=csrfToken,
            headers={'Cookie': 'yandexuid=' + yandexuid},
            start_point=COORD_YANDEX_RR,
            start_point_desc='office',
            end_point=COORD_MY_HOME,
            end_point_desc='home',
            come_back=True,
            come_back_sleep_sec=2
        )
    except ConnectionError:
        print_log_message('Произошла ошибка подключения к API. Перезапрашиваем ключи и пробуем снова...')
        yandexuid, csrfToken = get_tokens()
        choose_new_proxy(proxies)
        print_log_message('\n\tКлючи получены:\n\t\tyandexuid:\t{}\n\t\tcsrfToken:\t{}'.format(
            yandexuid, csrfToken
        ))
        continue

    if 'data.options' in result_df.columns:
        result_df.drop('data.options', axis=1, inplace=True)

    if 'error.code' in result_df.columns:
        print_log_message('API вернул код с ошибкой. Перезапрашиваем ключи и пробуем снова...')
        yandexuid, csrfToken = get_tokens()
        choose_new_proxy(proxies)
        print_log_message('\n\tКлючи получены:\n\t\tyandexuid:\t{}\n\t\tcsrfToken:\t{}'.format(
            yandexuid, csrfToken
        ))
        continue

    if random.random() > 0.999:
        print_log_message('Случайно решили поменять ключи. Перезапрашиваем ключи и пробуем снова...')
        yandexuid, csrfToken = get_tokens()
        choose_new_proxy(proxies)
        print_log_message('\n\tКлючи получены:\n\t\tyandexuid:\t{}\n\t\tcsrfToken:\t{}'.format(
            yandexuid, csrfToken
        ))
        continue

    with open(result_path, 'a', encoding='utf8') as f:
        for element in result_df.to_dict(orient='records'):
            f.write(json.dumps(element) + '\n')
    
    print_log_message('Пробуем записать данные в БД')
    result_df_for_db = result_df[~result_df['data.price'].isnull()]
    if result_df_for_db.shape[0] > 0:
        write_results_to_db(result_df_for_db)
            
    delay_sec = random.randint(DELAY_MIN, DELAY_MAX)
    print_log_message('Засыпаем на {}\tсек.'.format(delay_sec))
    sleep(delay_sec)











