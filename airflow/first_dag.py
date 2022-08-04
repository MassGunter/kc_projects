#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    # Здесь пока оставили запись в файл, как передавать переменую между тасками будет в третьем уроке
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df["domain_zone"] = top_data_df['domain'].str.split('.').str[-1]
    top_data_top_10_domain_zone = top_data_df         .groupby('domain_zone', as_index = False)         .agg({"rank" : 'count'})         .sort_values('rank', ascending = False)         .head(10)
    
    with open('top_domain_zone.csv', 'w') as f:
        f.write(top_data_top_10_domain_zone.to_csv(index = False, header = False))

def longest_domain_name():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    top_data_df['domain_length'] = top_data_df['domain'].apply(len)
    top_data_df[['domain', 'domain_length']].sort_values('domain_length', ascending = False).reset_index().loc[0]['domain']
    
    with open('top_data_top_10_com.csv', 'w') as f:
        f.write(top_data_top_10.to_csv(index=False, header=False))

def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    
    with open('airflow_rank.csv', 'w') as f:
        if top_data_df[top_data_df['domain'] == 'airflow.com'].empty:
            f.write('airflow.com is not found')
        else: 
            airflow_rank = top_data_df[top_data_df['domain'] == 'airflow.com']['rank']
            f.write(airflow_rank)
        

def print_data(ds):
    with open('top_data_top_10_com.csv', 'r') as f:
        top_data_top_10_zone = f.read()
        
    with open('longest_domain_name.csv', 'r') as f:
        longest_domain_name = f.read()
    
    with open('airflow_rank.csv') as f:
        airflow_rank = f.read()
    
    date = ds

    print(f'Top 10 domain zones for date {date}')
    print(top_data_top_10_zone)

    print(f'Longest domain name for date {date}')
    print(longest_domain_name)
    
    print(f'Position for "airflow.com" for date  {date}')
    print(airflow_rank)


default_args = {
    'owner': 'k-stojanov-21',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 14, 7),
}
schedule_interval = '0 4 * * *'


dag = DAG('k-stojanov-21_top10', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_domain_zone',
                    python_callable=top_10_domain_zone,
                    dag=dag)

t3 = PythonOperator(task_id='longest_domain_name',
                        python_callable=longest_domain_name,
                        dag=dag)

t4 = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)


t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t3, t4] >> t3

#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)
