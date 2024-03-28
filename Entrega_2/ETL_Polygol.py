# %% [markdown]
# ### <b style='color:orange'>ETL</b>

# %% [markdown]
# #### DOCUMENTACION: https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to

# %%
import json
from datetime import datetime, timedelta
import pandas as pd
from typing import List

class GestorDeDatos:
    def __init__(self):
        self.__datos = self.__get_key("claves.json")

    def __get_key(self,nombre_archivo_json):
        """
        Carga un archivo JSON y retorna su contenido.
        Este método es privado y solo se puede llamar desde dentro de la clase.

        :return: Un diccionario con los datos del JSON o None si hay un error.
        """
        try:
            with open(nombre_archivo_json, 'r') as archivo:
                datos = json.load(archivo)
                return datos
        except FileNotFoundError:
            print(f"El archivo {nombre_archivo_json} no fue encontrado.")
        except json.JSONDecodeError:
            print(f"El archivo {nombre_archivo_json} no contiene un JSON válido.")
        except Exception as e:
            print(f"Ocurrió un error al cargar el archivo {nombre_archivo_json}: {e}")
        return None

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
        for ticker in stocksTicker:
            url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/{p_from.strftime("%Y-%m-%d")}/{p_to.strftime("%Y-%m-%d")}'
            paramsApi = self.__datos['API']
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
        
    def dataframeCrypto(self,dataList:list)-> pd.DataFrame:
        """
        Convierte una lista de datos de la API de Polygon en un DataFrame de pandas.

        Args:
            dataList (list): Lista de diccionarios que contienen datos de la API de Polygon.

        Returns:
            pd.DataFrame: DataFrame consolidado con los datos procesados.

        El DataFrame resultante incluye columnas renombradas y dos columnas adicionales 'Exchange_Symbol' que
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
            "t":'Event_DateTime',
            "n":'Number_of_transactions'
            }
        result_dataframe = result_dataframe.rename(columns=renameColumns)
        result_dataframe['Event_DateTime'] = pd.to_datetime(result_dataframe['Event_DateTime'], unit='ms')
        result_dataframe['aud_ins_dttm']   = datetime.now()
        # Reorganizar las columnas
        init_columns = ['aud_ins_dttm','Exchange_Symbol', 'Event_DateTime']
        result_dataframe = result_dataframe[init_columns + [col for col in result_dataframe.columns if col not in init_columns]]
        return result_dataframe
    
    def insertIntoSql(self,df_exchanges,nameTable):
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
        from sqlalchemy import create_engine,text
        credential = self.__datos['SQLCREDENTIAL']
        engine = create_engine(f'postgresql://{credential["sqlUser"]}:{credential["sqlkey"]}@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database')
        temp_table_name = f"{nameTable}"
        df_exchanges.to_sql(temp_table_name, engine, index=False, if_exists='replace', method='multi')

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
            AND target.event_datetime = staging.event_datetime;
        """)
        with engine.connect() as conn:
            conn.execute(update_query)

        insert_query = text("""
            INSERT INTO martin_pm_coderhouse.exchange_stocks_data
            SELECT staging.*
            FROM martin_pm_coderhouse.temp_exchange_stocks_data as staging
            LEFT JOIN martin_pm_coderhouse.exchange_stocks_data as target
            ON staging.exchange_symbol = target.exchange_symbol
            AND staging.event_datetime = target.event_datetime
            WHERE target.exchange_symbol IS NULL;
        """)
        with engine.connect() as conn:
            conn.execute(insert_query)


        

# %% [markdown]
# ### <b style='color:Orange'>Simbolos de las acciones</b>
# 
# * AAPL: Apple Inc. (acciones de Apple)
# * GOOGL: Alphabet Inc. (clase A de acciones de Google)
# * AMZN: Amazon.com Inc.
# * MSFT: Microsoft Corporation
# * TSLA: Tesla Inc.
# * FB: Meta Platforms, Inc. (anteriormente conocida como Facebook Inc.)
# * JPM: JPMorgan Chase & Co.
# * GOOG: Alphabet Inc. (clase C de acciones de Google)
# * NFLX: Netflix Inc.
# * V: Visa Inc.
# 
# La api no cuenta con los dias de fin de semana

# %%
gestor = GestorDeDatos()
response_Api = gestor.cryptoapi(stocksTicker=["AAPL","GOOGL","AMZN"]) #,p_from=datetime(2024,1,1),p_to=datetime(2024,1,2)
print(response_Api)
df_exchanges = gestor.dataframeCrypto(response_Api)
gestor.insertIntoSql(df_exchanges,'temp_exchange_stocks_data')


