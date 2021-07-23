from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import json
import logging
import pandas as pd
from google.cloud import bigquery


def readBucket():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('exchange_info_bucket')
    blob = bucket.blob('info.txt')
    data = json.loads(blob.download_as_string())
    return data


def transform(**kwargs):
    task_instance = kwargs['task_instance']
    data = task_instance.xcom_pull(task_ids='read_file_from_bucket')
    currencyInfo = data["conversion_rates"]
    strongerThanDollarTicker = []
    strongerThanDollarPrice = []
    weakerThanDollarTicker = []
    weakerThanDollarPrice = []
    for k in currencyInfo:
        if currencyInfo[k] < currencyInfo["USD"]:
            strongerThanDollarTicker.append(k)
            strongerThanDollarPrice.append(currencyInfo[k])
        elif currencyInfo[k] > currencyInfo["USD"]:
            weakerThanDollarTicker.append(k)
            weakerThanDollarPrice.append(currencyInfo[k])
    dataFSD = {"Currency_Ticker": strongerThanDollarTicker, "Currency_Price": strongerThanDollarPrice}
    dataFWD = {"Currency_Ticker": weakerThanDollarTicker, "Currency_Price": weakerThanDollarPrice}

    pdFSD = pd.DataFrame(data=dataFSD).to_json()
    pdFWD = pd.DataFrame(data=dataFWD).to_json()

    return pdFSD, pdFWD


def load(**kwargs):
    datasetID = "Currency_exchange_rate_for_usd"
    task_instance = kwargs["task_instance"]
    data = task_instance.xcom_pull(task_ids="transform_data")
    pdFSD = pd.read_json(data[0])
    pdFWD = pd.read_json(data[1])
    logging.info("Received dataframes")
    logging.info(pdFSD)
    logging.info(pdFWD)
    bqclient = bigquery.client.Client(project="peak-emblem-319723")
    dataset = bqclient.dataset(datasetID)
    dataset.location = "US"
    try:
        bqclient.create_dataset(dataset)
        bqclient.create_table("Currency_stronger_than_dollar")
        bqclient.create_table("Currency_weaker_than_dollar")
    except:
        pass
    bqclient.load_table_from_dataframe(pdFSD, "peak-emblem-319723.Currency_exchange_rate_for_usd.Currency_stronger_than_dollar")
    bqclient.load_table_from_dataframe(pdFWD, "peak-emblem-319723.Currency_exchange_rate_for_usd.Currency_weaker_than_dollar")


with DAG(dag_id='ETL_DAG', start_date=days_ago(1), schedule_interval=None) as dag:

    # Print the received dag_run configuration.
    # The DAG run configuration contains information about the
    # Cloud Storage object change.

    extract = PythonOperator(
        task_id="read_file_from_bucket",
        python_callable=readBucket,
        dag=dag
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
        provide_context=True,
        dag=dag
    )

    load = PythonOperator(
        task_id="load_into_bq",
        python_callable=load,
        provide_context=True,
        dag=dag
    )

    extract >> transform >> load