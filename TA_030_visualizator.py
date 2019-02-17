# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine

from time import sleep
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
# get_ipython().magic('matplotlib inline')


MORNING_TIME_RANGE = ['05:00:00', '12:00:00']
EVENING_TIME_RANGE = ['19:30:00', '23:59:59']

RESULT_PATH = '/data/yandex.taxi.prices.db'

CHART_DETAILED_FREQ = '5min'
CHART_DETAILED_MORNING_PATH = './data/plots/yt_plot_morning.png'
CHART_DETAILED_EVENING_PATH = './data/plots/yt_plot_evening.png'

CHART_GENERAL_FREQ = '1440min'
CHART_GENERAL_PATH = './data/plots/yt_plot_general.png'

CHART_WEEKDAYS_FREQ = '60min'
CHART_WEEKDAYS_TO_HOME_PATH   = './data/plots/yt_plot_week_to_home.png'
CHART_WEEKDAYS_TO_OFFICE_PATH = './data/plots/yt_plot_week_to_office.png'

DELAY = 60 # секунд

engine = create_engine('sqlite://' + RESULT_PATH, echo=False)

def print_log_message(text):
    print('[{}]: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), text))

def get_data4plot(start_date, end_date):
    query_base = """
        SELECT
          request_datetime,
          price,
          start_point_desc,
          end_point_desc
        FROM
          prices
        WHERE
          request_datetime >= '{start_date}'
          AND request_datetime < '{end_date}'
        """
    
    query = query_base.format(start_date=start_date, end_date=end_date)
    
    data4plot_raw = pd.read_sql(sql=query, con=engine)
    
    # Преобразовываем формат даты
    to_datetime = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    data4plot_raw['request_datetime'] = data4plot_raw['request_datetime'].apply(to_datetime)
    
    data4plot_raw['trip_desc'] = data4plot_raw.apply(
        func = lambda x: 'From {} to {}'.format(x['start_point_desc'], x['end_point_desc']),
        axis=1
    )
    
    return data4plot_raw

def data_grouper(data4plot_raw, chart_freq):
    
    data4plot = (
    data4plot_raw
        .groupby(by=['trip_desc', pd.Grouper(key='request_datetime', freq=chart_freq)])
        .aggregate({'price': 'mean'})
        .reset_index()
    )

    time_correction = lambda x: x + timedelta(minutes=int(chart_freq.replace('min', ''))/2)
    data4plot['request_datetime'] = data4plot['request_datetime'].apply(time_correction)

    data4plot['date']    = data4plot['request_datetime'].apply(func = lambda x: x.date())
    data4plot['time']    = data4plot['request_datetime'].apply(func = lambda x: x.time())
    data4plot['weekday'] = data4plot['request_datetime'].apply(func = lambda x: x.date().weekday())
    
    return data4plot

def analyze_statistics(df, use_only_same_weekday = False):
    max_date = df.date.max()

    if use_only_same_weekday:
        df_tmp = df[df.weekday == max_date.weekday()]
    elif max_date.weekday() < 5:
        df_tmp = df[df.weekday.isin(range(0, 5))]
    else:
        df_tmp = df[df.weekday.isin((5, 6))]

    df_prev = df_tmp[df_tmp.date != max_date]

    df_cur = df_tmp[df_tmp.date == max_date]
    df_cur = df_cur[['trip_desc', 'time', 'price']]
    
    
    df_prev_grouped = (
        df_prev
            .groupby(['trip_desc', 'time'])
            .aggregate({
                'price': {
                    '025': lambda x: x.quantile(q=0.25),
                    '050': lambda x: x.quantile(q=0.50),
                    '075': lambda x: x.quantile(q=0.75)
                }
            })
            .reset_index()
    )

    df_prev_grouped.columns = ['_'.join(col) if col[1] != '' else col[0] for col in df_prev_grouped.columns]
    
    df_final = pd.merge(
        left = df_prev_grouped,
        right = df_cur,
        how='left',
        on=['trip_desc', 'time']
    )
    
    return {'data': df_final, 'max_date': max_date}

def plot_weekdays(data4plot, result_path):
    
    weekdays = data4plot.weekday.unique()
    
    plt.grid(b=True)

    plt.xlabel('Time')
    plt.ylabel('Price, rubles')
    plt.title('Yandex.Taxi price by weekdays: ' + data4plot.trip_desc.values[0])

    for weekday in weekdays:
        data4plot_tmp = data4plot[data4plot.weekday == weekday]

        x = data4plot_tmp['time'].values
        y = data4plot_tmp['price'].values

        # plot
        plt.plot(x,y)


    # Так и не смог запустить, из-за ошибок
    #xlocator = md.MinuteLocator(byminute=[0,15,30,45], interval = 1)
    #plt.axes[0].xaxis.set_major_locator(hours)
    plt.gcf().autofmt_xdate()

    plt.legend(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], loc='upper left')

    plt.legend()
    plt.savefig(result_path)
    plt.close()

def plot_detailed(data4plot, trip_desc, max_date, result_path):
    data4plot = data4plot[data4plot['trip_desc'] == trip_desc]

    price_columns = ['price', 'price_025', 'price_050', 'price_075']
    plt.grid(b=True)

    plt.xlabel('Time')
    plt.ylabel('Price, rubles')
    plt.title(data4plot.trip_desc.values[0])

    for price_column in price_columns:

        if price_column in ['price_025', 'price_075']:
            color_tmp='red'
            linestyle_tmp='dotted'
            label_tmp='percentile {}%'.format(price_column[-2:])
        elif price_column == 'price_050':
            color_tmp='red'
            linestyle_tmp='dashed'
            label_tmp='median'
        else:
            color_tmp='blue'
            linestyle_tmp='solid'
            label_tmp=max_date


        x = data4plot['time'].values
        y = data4plot[price_column].values

        # plot
        plt.plot(x,y, color=color_tmp, linestyle=linestyle_tmp, label=label_tmp)


    # Так и не смог запустить, из-за ошибок
    #xlocator = md.MinuteLocator(byminute=[0,15,30,45], interval = 1)
    #plt.axes[0].xaxis.set_major_locator(hours)
    plt.gcf().autofmt_xdate()

    #plt.legend(dates, loc='upper right')

    plt.legend()
    plt.savefig(result_path)
    plt.close()

def plot_general(data4plot, result_path):
    
    trips = data4plot.trip_desc.unique()
    
    plt.grid(b=True)

    plt.xlabel('Time')
    plt.ylabel('Price, rubles')
    plt.title('Yandex.Taxi price')

    for trip in trips:
        
        data4plot_tmp = data4plot[data4plot.trip_desc == trip]

        if trip == 'From home to office':
            color_tmp='red'
            linestyle_tmp='solid'
            label_tmp=trip
        elif trip == 'From office to home':
            color_tmp='blue'
            linestyle_tmp='solid'
            label_tmp=trip
        else:
            color_tmp='green'
            linestyle_tmp='dashed'
            label_tmp=trip


        x = data4plot_tmp['request_datetime'].values
        y = data4plot_tmp['price'].values

        # plot
        plt.plot(x,y, color=color_tmp, linestyle=linestyle_tmp, label=label_tmp)


    # Так и не смог запустить, из-за ошибок
    #xlocator = md.MinuteLocator(byminute=[0,15,30,45], interval = 1)
    #plt.axes[0].xaxis.set_major_locator(hours)
    plt.gcf().autofmt_xdate()

    #plt.legend(dates, loc='upper right')

    plt.legend()
    plt.savefig(result_path)
    plt.close()

# import matplotlib.dates as md

while True:
    start_date = (datetime.now() - timedelta(days=7*5)).strftime('%Y-%m-%d')
    # Из-за особенностей выгрузки данных из БД (см. запрос)
    # необходимо чтобы последняя дата выгрузки была на день больше
    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    print_log_message('Выгружаем данные за период с {} по {}'.format(start_date, end_date))

    data4plot_raw     = get_data4plot(start_date=start_date, end_date=end_date)
    # Для утра и вечера
    data4plot         = data_grouper(data4plot_raw=data4plot_raw, chart_freq=CHART_DETAILED_FREQ)
    # В целом на картину посмотреть
    data4plot_general = data_grouper(data4plot_raw=data4plot_raw, chart_freq=CHART_GENERAL_FREQ)
    # Для дней недели
    data4plot_weekdays = data_grouper(data4plot_raw=data4plot_raw, chart_freq=CHART_WEEKDAYS_FREQ)

    str2time = lambda x: datetime.strptime(x, '%H:%M:%S').time()

    data4plot_morning_raw = data4plot[
        (data4plot.time >= str2time(MORNING_TIME_RANGE[0])) &
        (data4plot.time <= str2time(MORNING_TIME_RANGE[1]))
    ]

    data4plot_evening_raw = data4plot[
        (data4plot.time >= str2time(EVENING_TIME_RANGE[0])) &
        (data4plot.time <= str2time(EVENING_TIME_RANGE[1]))
    ]


    print_log_message('Строим графики...')
    data4plot_evening = analyze_statistics(data4plot_evening_raw, use_only_same_weekday=True)
    plot_detailed(
        data4plot=data4plot_evening['data'],
        trip_desc='From office to home',
        max_date=data4plot_evening['max_date'],
        result_path=CHART_DETAILED_EVENING_PATH
    )

    data4plot_morning = analyze_statistics(data4plot_morning_raw, use_only_same_weekday=True)
    plot_detailed(
        data4plot=data4plot_morning['data'],
        trip_desc='From home to office',
        max_date=data4plot_morning['max_date'],
        result_path=CHART_DETAILED_MORNING_PATH
    )

    plot_general(
        data4plot=data4plot_general,
        result_path=CHART_GENERAL_PATH
    )

    data4plot_weekly = data4plot_weekdays.groupby(['trip_desc', 'weekday', 'time'], as_index=False).price.median()
    plot_weekdays(
        data4plot=data4plot_weekly[data4plot_weekly.trip_desc=='From home to office'],
        result_path=CHART_WEEKDAYS_TO_OFFICE_PATH
        )
    plot_weekdays(
        data4plot=data4plot_weekly[data4plot_weekly.trip_desc=='From office to home'],
        result_path=CHART_WEEKDAYS_TO_HOME_PATH
        )

    print_log_message('Засыпаем на {} сек.'.format(DELAY))
    sleep(DELAY)




