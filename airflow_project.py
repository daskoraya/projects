import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import json

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

STAT_GAMES = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"

# Инициализируем DAG
default_args = {
    'owner': 'darja-skorobogatko-anb5756',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 20)
}

schedule_interval = '10 10 * * *'

# Таски
@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def dag2_skorobogako():
    @task()
    def get_data():
        login = 'darja-skorobogatko-anb5756'
        year = 1994 + hash(f'{login}') % 23
        link = "https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv"
        data = pd.read_csv(link)
        data_year = data.query('Year == @year').dropna().to_json(orient="records")
        return {'data_year': data_year, 'year': year}

    @task()
    def get_best_game(data_dict_year):
        data = pd.DataFrame(json.loads(data_dict_year['data_year']))
        best_sales_game = data.query('Global_Sales == Global_Sales.max()')
        best_sales_game = best_sales_game[['Name']].to_string(index=False, header=False)
        return best_sales_game

    @task()
    def best_genre_eu(data_dict_year):
        data = pd.DataFrame(json.loads(data_dict_year['data_year']))
        bestsales_Genre_EU = data.groupby('Genre')['EU_Sales'].sum().sort_values().idxmax()
        return bestsales_Genre_EU

    @task()
    def best_game_na(data_dict_year):
        data = pd.DataFrame(json.loads(data_dict_year['data_year']))
        na_million = data.query('NA_Sales > 1') 
        max_games_na = na_million.groupby('Platform')['Name'].count().sort_values().idxmax()
        return max_games_na

    @task()
    def get_best_avg_jp(data_dict_year):
        data = pd.DataFrame(json.loads(data_dict_year['data_year']))
        avg_sales_jp = data.groupby('Publisher')['JP_Sales'].mean().sort_values().idxmax()
        return avg_sales_jp
    
    @task()
    def eu_vs_jp(data_dict_year):
        data = pd.DataFrame(json.loads(data_dict_year['data_year']))
        sales = data.groupby('Name')[['EU_Sales', 'JP_Sales']].sum()
        sales_eu = sales.query('EU_Sales > JP_Sales')['EU_Sales'].count()
        return sales_eu

    @task()
    def print_data(data_dict_year, best_sales_game, bestsales_Genre_EU, max_games_na, avg_sales_jp, sales_eu):

        context = get_current_context()
        date = context['ds']
        year = data_dict_year['year']

        print(f'{date} \n Самая продаваемая игра в {year} году: {best_sales_game}')
        print(f'{date} \n Самый продаваемый жанр в Европе в {year} году: {bestsales_Genre_EU}')
        print(f'{date} \n Больше всего игр, с тиражом свыше миллиона в {year} году в Северной Америке было на платформе: {max_games_na}')
        print(f'{date} \n Cамые высокие средние продажи в Японии в {year} году у издателя: {avg_sales_jp}')
        print(f'{date} \n {sales_eu} игр в {year} году продались в Европе лучше, чем в Японии')


    
    data_dict_year = get_data()
    best_sales_game = get_best_game(data_dict_year)
    bestsales_Genre_EU = best_genre_eu(data_dict_year)
    max_games_na = best_game_na(data_dict_year)
    avg_sales_jp = get_best_avg_jp(data_dict_year)
    sales_eu = eu_vs_jp(data_dict_year)
    
    print_data(data_dict_year, best_sales_game, bestsales_Genre_EU, max_games_na, avg_sales_jp, sales_eu)

dag2_skorobogako = dag2_skorobogako()
