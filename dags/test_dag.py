from datetime import datetime as dt

import requests
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from psycopg2 import extras

def get_airflow_connection(airflow_conn_id):
    pghook = PostgresHook(postgres_conn_id=airflow_conn_id)
    conn = pghook.get_connection(airflow_conn_id)

    connection = psycopg2.connect(
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    db = conn.schema
    print(f"Database - {db} - connection opened successfully")

    return connection

@dag(schedule=None, start_date=dt(2022, 2, 18), catchup=False)
def test_aero():

    @task()
    def take_data():
        result = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=100')
        return result.json()

    @task()
    def load_data(data):
        conn = get_airflow_connection('dwh_conenction') # Предварительно создал конекшн в аифлоу
        target_table = 'cannabis_data' # Предварительно создал таблицу
        query = f"INSERT INTO {target_table} VALUES %s"
        if data:
            with conn.cursor() as curs:
                for i in data:
                    values = [tuple(i.values())]
                    extras.execute_values(curs, query, values)
        conn.commit()
        conn.close()

    api_data = take_data()
    load_data(api_data)

dag = test_aero()
