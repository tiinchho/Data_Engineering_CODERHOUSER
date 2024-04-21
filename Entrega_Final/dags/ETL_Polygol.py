# dags/etl_dag.py

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import pandas as pd
from typing import List
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


class GestorDeDatos:
    def __init__(self):
        self.__datos = Variable.get("SECRETS_API_KEY")
        print(f'el valor de la variable es: {self.__datos}')

    def cryptoapi(self,stocksTicker:List[str],timespan: str = 'day',p_from: datetime = datetime.now() - timedelta(days=8), p_to: datetime = datetime.now() - timedelta(days=1)) -> list:
        """
        Obtener datos de la API de Polygon para un rango de fechas específico y para una lista variable de tickers.

        Args:
            stocksTicker (List[str]): Lista variable de tickers.
            timespan (str): El período de tiempo para los datos (por defecto 'day').
            p_from (datetime): La fecha inicial para la consulta (por defecto hace 8 días).
            p_to (datetime): La fecha final para la consulta (por defecto ayer).

        Returns:
            list: Lista de resultados de la API para cada ticker proporcionado. Cada elemento de la lista es un diccionario con 'ticker' y 'data'.
                - 'ticker': El símbolo del instrumento financiero.
                - 'data': Lista de resultados de la API para el ticker, cada elemento contiene información de agregación.

        Raises:
            requests.exceptions.HTTPError: Se lanza en caso de errores HTTP.
            Exception: Se lanza en caso de errores no manejados.
        """
        from requests import get,exceptions
        import time
        all_results = []  # Lista para almacenar resultados de todas las ejecuciones
        print(f'el valor de la variable es: {self.__datos}')
        for ticker in stocksTicker:
            url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/{p_from.strftime("%Y-%m-%d")}/{p_to.strftime("%Y-%m-%d")}'
            paramsApi = {"apiKey" : f"{self.__datos}"}
            print(paramsApi)
            try:
                response = get(url, params=paramsApi)
                response.raise_for_status()  # Lanza una excepción para errores HTTP
                json_data = response.json()
                results = json_data.get('results', [])
                all_results.append({'ticker': ticker, 'data': results})  # Agregar resultados a la lista
            except exceptions.HTTPError as err:
                if response.status_code == 429:
                    print(f"Error 429: Demasiadas solicitudes para {ticker}. Esperando 1 minuto antes de intentar nuevamente.")
                    time.sleep(60)  # Espera 1 minuto antes de intentar nuevamente
                    continue  # Salta a la siguiente iteración del bucle
                else:
                    print(f"Error HTTP: {err}")
            except Exception as e:
                print(f"Error en la solicitud a la API para {ticker}: {e}")
        print("La solicitud finalizo con exito")
        return all_results
        
    def dataframeCrypto(self,dataList:list)-> dict:
        """
        Convierte una lista de datos de la API de Polygon en un DataFrame de pandas.

        Args:
            dataList (list): Lista de diccionarios que contienen datos de la API de Polygon.

        Returns:
            JSON: Json consolidado con los datos procesados.

        El Json resultante incluye columnas renombradas y dos columnas adicionales 'Exchange_Symbol' que
        representa el símbolo de intercambio asociado a cada conjunto de datos y 'aud_ins_dttm' que representa la fecha de la ejecucion.
        """
        all_dataframes = []
        for rows in dataList:
            tmp_dataframe = pd.DataFrame(rows.get('data'))
            tmp_dataframe = tmp_dataframe.assign(Exchange_Symbol=rows.get('ticker'))
            all_dataframes.append(tmp_dataframe)


        result_dataframe = pd.concat(all_dataframes, ignore_index=True)
        renameColumns = {
            "v":'Trading_Volume',
            "vw":'Volume_Weighted_Average_Price',
            "o":'Open_Price',
            "c":'Close_Price',
            "h":'Highest_Price',
            "l":'Lowest_Price',
            "t":'Event_Date',
            "n":'Number_of_transactions'
            }
        result_dataframe = result_dataframe.rename(columns=renameColumns)
        result_dataframe['Event_Date'] = pd.to_datetime(result_dataframe['Event_Date'], unit='ms').dt.date
        result_dataframe['aud_ins_dttm']   = datetime.now()
        # Reorganizar las columnas
        init_columns = ['aud_ins_dttm','Exchange_Symbol', 'Event_Date']
        result_dataframe = result_dataframe[init_columns + [col for col in result_dataframe.columns if col not in init_columns]]
        return result_dataframe.to_json(date_format='iso')
    
    def insertIntoSql(self,dic_exchanges,nameTable):
        """
            Esta función realiza un 'upsert' (actualización o inserción) en una tabla de base de datos.
            Los datos para el 'upsert' provienen de un DataFrame de pandas. La función utiliza SQLAlchemy para
            la conexión con la base de datos y ejecución de consultas SQL en bruto.
            
            Parámetros:
            df_exchanges (pandas.DataFrame): DataFrame que contiene los datos que se insertarán o actualizarán en la tabla de destino.
            nameTable (str): Nombre de la tabla de destino en la base de datos donde se realizará el 'upsert'.

            La función realiza las siguientes operaciones:
            1. Carga los datos del DataFrame `df_exchanges` en una tabla de staging o temporal en la base de datos.
            Si la tabla ya existe, su contenido es reemplazado.
            2. Actualiza los registros existentes en la tabla de destino con los valores correspondientes de la tabla de staging
            si las claves primarias (o una condición única) coinciden.
            3. Inserta nuevos registros en la tabla de destino desde la tabla de staging para aquellos que no tienen una coincidencia
            en las claves primarias (o condición única) con los registros ya existentes en la tabla de destino.

            Nota: La función asume que las estructuras de la tabla de destino y la tabla de staging son idénticas y que
            `temp_exchange_stocks_data` es el nombre de la tabla de staging utilizada para el 'upsert'.

            La conexión con la base de datos se establece utilizando las credenciales almacenadas en `self.__datos['SQLCREDENTIAL']`.
        """
        from sqlalchemy import text,types
        pg_hook = PostgresHook(postgres_conn_id='redshift_connection')
        engine = pg_hook.get_sqlalchemy_engine()
        temp_table_name = f"{nameTable}"
        df_exchanges = pd.read_json(dic_exchanges, convert_dates=['Event_Date', 'aud_ins_dttm'])
        df_exchanges.to_sql(temp_table_name, engine, index=False, if_exists='replace', method='multi', dtype={'aud_ins_dttm': types.DateTime(),'Event_Date': types.Date()})
        update_query = text("""
            UPDATE martin_pm_coderhouse.exchange_stocks_data as target
            SET
                trading_volume = staging.trading_volume,
                volume_weighted_average_price = staging.volume_weighted_average_price,
                open_price = staging.open_price,
                close_price = staging.close_price,
                highest_price = staging.highest_price,
                lowest_price = staging.lowest_price,
                number_of_transactions = staging.number_of_transactions
            FROM martin_pm_coderhouse.temp_exchange_stocks_data as staging
            WHERE target.exchange_symbol = staging.exchange_symbol
            AND target.event_date = staging.event_date;
        """)
        with engine.connect() as conn:
            conn.execute(update_query)

        insert_query = text("""
            INSERT INTO martin_pm_coderhouse.exchange_stocks_data
            SELECT staging.*
            FROM martin_pm_coderhouse.temp_exchange_stocks_data as staging
            LEFT JOIN martin_pm_coderhouse.exchange_stocks_data as target
            ON staging.exchange_symbol = target.exchange_symbol
            AND staging.event_date = target.event_date
            WHERE target.exchange_symbol IS NULL;
        """)
        with engine.connect() as conn:
            conn.execute(insert_query)



gestor = GestorDeDatos()
def extract_data(tickers, p_from_str, p_to_str):
    # Convertir las cadenas de fecha a objetos datetime
    p_from = datetime.strptime(p_from_str, '%Y-%m-%d')
    p_to = datetime.strptime(p_to_str, '%Y-%m-%d')
    response_Api = gestor.cryptoapi(stocksTicker=tickers, p_from=p_from, p_to=p_to)
    print(response_Api)
    return response_Api

def transform_data(**context):
    response_Api = context['task_instance'].xcom_pull(task_ids='extract_data_task')
    print(response_Api)
    df_exchanges = gestor.dataframeCrypto(response_Api)
    return df_exchanges

def load_data(**context):
    df_exchanges = context['task_instance'].xcom_pull(task_ids='transform_data_task')
    gestor.insertIntoSql(df_exchanges, 'temp_exchange_stocks_data')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_polygon_data',
    default_args=default_args,
    description='DAG para el ETL de polygon, api con la informacion de instrumentos financieros',
    schedule_interval='@daily',
    catchup=False,
)


extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    op_kwargs={
        'tickers': ["AAPL", "GOOGL", "SP"],
        'p_from_str': '{{ macros.ds_add(ds, -8) }}',  # Fecha de ejecución + 1 día
        'p_to_str': '{{ ds }}',  # Fecha de ejecución
    },
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_data_task >> transform_data_task >> load_data_task
