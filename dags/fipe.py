import airflow
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
import pandas as pd
from datetime import timedelta
from docker.types import Mount
import os
from sys import api_version

default_args = {
    'owner': 'Victor',    
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    }

def extractFile():
    dls = "https://github.com/VDelomo/analise_fipe/blob/develop/data/fipe_cars.csv"
    resp = requests.get(dls, allow_redirects=True)

    output = open('/opt/airflow/data', 'wb')
    output.write(resp.content)

def refined():
    df_fipe = pd.read_csv('/diretorio/input/fipe_cars.csv',sep = ";")

    df_fipe.columns = df_fipe.columns.to_series().apply(lambda x: x.strip())

    df_fipe_layout = df_fipe[['year_of_reference','brand','model','fuel','gear','engine_size','year_model','avg_price_brl']]

    df_fipe_mean = round(df_fipe_layout.groupby(["year_of_reference",
                                                   "brand",
                                                   "model",
                                                   "fuel",
                                                   "gear",
                                                   "engine_size",
                                                   "year_model"]
                                                   ).agg(['mean']),2)
    
    df_fipe_refined = df_fipe_layout[["year_of_reference",
                                      "brand",
                                      "model",
                                      "fuel",
                                      "gear",
                                      "engine_size",
                                      "year_model"]].drop_duplicates()
    print('passou do teste 3,5')

    print('fipe')
    print(df_fipe_refined.head(1))

    print('mean')
    print(df_fipe_mean.head(1))

    df_fipe_refined = df_fipe_refined.merge(df_fipe_mean,how = 'inner', on=["year_of_reference",
                                                                            "brand",
                                                                            "model",
                                                                            "fuel",
                                                                            "gear",
                                                                            "engine_size",
                                                                            "year_model"])
    
    df_fipe_refined = df_fipe_refined.rename(columns={('avg_price_brl', 'mean'):"mean_price"})
    print('passou do teste 4')

    df_fipe_refined['diff_row'] = (
    round(df_fipe_refined.groupby(["year_of_reference",
                                   "brand",
                                   "model",
                                   "fuel",
                                   "gear",
                                   "engine_size"])['mean_price'].diff())
                                   )
    
    df_fipe_refined["diff_row"] = df_fipe_refined["diff_row"].fillna(0)

    df_fipe_2021 = df_fipe_refined[df_fipe_refined.year_of_reference == 2021].rename(columns={"mean_price":"mean_price_2021","diff_row":"diff_row_2021"})
    df_fipe_2022 = df_fipe_refined[df_fipe_refined.year_of_reference == 2022].rename(columns={"mean_price":"mean_price_2022","diff_row":"diff_row_2022"})
    df_fipe_2023 = df_fipe_refined[df_fipe_refined.year_of_reference == 2023].rename(columns={"mean_price":"mean_price_2023","diff_row":"diff_row_2023"})

    df_fipe_2021.to_csv('/diretorio/output/fipe_2021.csv', sep=',',index=False)
    df_fipe_2022.to_csv('/diretorio/output/fipe_2022.csv', sep=',',index=False)
    df_fipe_2023.to_csv('/diretorio/output/fipe_2023.csv', sep=',',index=False)

def fipe_merge():
    df_fipe_2021 = pd.read_csv('/diretorio/output/fipe_2021.csv')
    df_fipe_2022 = pd.read_csv('/diretorio/output/fipe_2022.csv')
    df_fipe_2023 = pd.read_csv('/diretorio/output/fipe_2023.csv')

    df_fipe_merge = df_fipe_2021.merge(df_fipe_2022, on=["brand","model","fuel","gear","engine_size","year_model"], how='inner', sort=False, suffixes=('_x', '_y'), copy=True, indicator=False)
    df_fipe_merge = pd.merge(df_fipe_merge, df_fipe_2023, how = 'inner', on=["brand","model","fuel","gear","engine_size","year_model"])

    df_fipe_merge = df_fipe_merge[["brand",
                               "model",
                               "fuel",
                               "gear",
                               "engine_size",
                               "year_model",
                               "mean_price_2021",
                               "mean_price_2022",
                               "mean_price_2023",
                               "diff_row_2021",
                               "diff_row_2022",
                               "diff_row_2023"]]

    df_fipe_merge['price_increase'] = round(df_fipe_merge.mean_price_2023 - df_fipe_merge.mean_price_2021,2)

    df_fipe_merge.to_csv('/diretorio/output/fipe_merge.csv', sep=',',index=False)

with DAG(
    dag_id = 'fipe',
    default_args=default_args,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
) as dag:
    
    #extractFiles = PythonOperator(
    #    task_id="extractFile",
    #    python_callable= extractFile
    #)

    refined = PythonOperator(
        task_id="refined",
        python_callable= refined
    )


    fipe_merge = PythonOperator(
        task_id="fipe_merge",
        python_callable = fipe_merge
    )

    #extractFiles >> refined    
    refined >> fipe_merge
