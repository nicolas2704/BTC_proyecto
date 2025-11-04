import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup # No se usa en este script
import psycopg2 # No se usa en este script
import json # No se usa en este script
from sqlalchemy import create_engine, text # No se usa en este script
from datetime import datetime, timedelta, timezone # Importar timezone
from pathlib import Path # No se usa en este script
from pycoingecko import *
import os
from email.message import EmailMessage
import ssl
import smtplib
from dotenv import load_dotenv
load_dotenv()

# librerias airflow
# definen la programacion del DAG cada cuanto se repetira,
# Es un objeto que representa un intervalo de tiempo, es decir, una cantidad de días, horas, minutos, segundos
from datetime import timedelta
# el objeto del DAG que necesitara para instanciar un DAG
# importa la clase DAG
from airflow.models import DAG
# Operadores, se necesita para escribir tareas
# estos definen que hace cada tarea en su DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
POSTGRES_CONN_ID = "postgres_nicolas" # para la conexion a la base de datos


# Trabajar con UTC para APIs
fecha_actual = datetime.now(timezone.utc)
# tomar los ultimos 30 dias
ultimos_30_dias = fecha_actual - timedelta(days=30)

# Convertir objetos datetime a timestamps Unix (segundos)
from_timestamp_unix = int(ultimos_30_dias.timestamp())
to_timestamp_unix = int(fecha_actual.timestamp())

print(f"Rango de fechas para la API:")
print(f"  Desde (UTC): {ultimos_30_dias} ({from_timestamp_unix} segundos)")
print(f"  Hasta (UTC): {fecha_actual} ({to_timestamp_unix} segundos)\n")

# definicion de variables
api_key=os.getenv("api_btc")
ruta_excel=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_precio_30.xlsx")
archivo_registro=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_registro.txt")
nombre_tabla="bitcoin_precios_30"

# obtener el precio de maximo historico

# Funciones
# EXTRAER
def extraer():
    proceso_log("Proceso ETL inicializado\n")
    proceso_log("Proceso de Extraccion Inicializado")
    ruta_datos_extraidos=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_datos_extraidos.xlsx")
    # Almacenar valores de la API
    datos_bitcoin = api_key.get_coin_market_chart_range_by_id(
        id="bitcoin",
        vs_currency="usd",
        from_timestamp=from_timestamp_unix, # Usar el timestamp numérico
        to_timestamp=to_timestamp_unix      # Usar el timestamp numérico
    )
    # creacion del dataframe
    datos_precio = pd.DataFrame(datos_bitcoin["prices"], columns=["TimeStamp", "Price"])
    datos_precio.to_excel(ruta_datos_extraidos) # extrae el dataframe
    print(datos_precio)
    proceso_log("Proceso de Extraccion finalizado\n")

# TRANSFORMAR
def transformar():
    proceso_log("Proceso de Transformacion Inicializado")
    # rutas
    ruta_datos_extraidos=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_datos_extraidos.xlsx")
    ruta_datos_transformados=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_precios_30.xlsx")
    # lee el dataframe de la funcion anterior
    datos_precio=pd.read_excel(ruta_datos_extraidos)
    # convierte la columna a fecha y hora
    datos_precio["TimeStamp"] = pd.to_datetime(datos_precio["TimeStamp"], unit='ms')
    # convierte la columna a entero
    datos_precio["Price"]=datos_precio["Price"].astype(int)

    # renombrar las columnas
    datos_precio.rename(columns={"TimeStamp": "fecha","Price":"Precio"}, inplace=True)

    # quita la hora y deja solo la fecha 
    datos_precio['fecha'] = datos_precio['fecha'].dt.date # Extraer solo la fecha para agrupar

    # muestra las fecha y precios con el formato correcto solo los 5 primeros
    print("DataFrame de precios crudos con DateTime:")
    print(datos_precio.head())

    diagrama_de_velas=datos_precio.groupby(datos_precio.fecha).agg({"Precio":["min","max","first","last"]})
    diagrama_de_velas.columns = diagrama_de_velas.columns.map('_'.join)
    diagrama_de_velas = diagrama_de_velas.rename(columns={
        "Precio_min":"minimo",
        "Precio_max":"maximo",
        "Precio_first":"apertura",
        "Precio_last":"cierre"
    })
    diagrama_de_velas = diagrama_de_velas.reset_index()
    # para que el indice comience a partir de 1
    diagrama_de_velas.index = diagrama_de_velas.index + 1
    # es necesario convertir a string la fecha para la exportacion
    diagrama_de_velas["fecha"]=diagrama_de_velas["fecha"].astype(str)
    # eliminar columna de indice
    # diagrama_de_velas=diagrama_de_velas.iloc[:,1:] # elimina la primera columna
    print("Diagrama de velas de Bitcoin")
    print(diagrama_de_velas)
    diagrama_de_velas.to_excel(ruta_datos_transformados)
    proceso_log("Proceso de Transformacion Finalizado\n")

# CARGAR
def cargar():
    proceso_log("Proceso de Carga Inicializado:")
    ruta_datos_transformados=Path("/home/nicolas27/data/ETL/airflow/proyectos_airflow/proyectos_dags/proyectos_completos/Apis_proyectos/btc_precios_30.xlsx")
    dataframe=pd.read_excel(ruta_datos_transformados)
    nombre_tabla="bitcoin_precios_30" #nombre de la tabla en la base de datos
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_uri = pg_hook.get_uri()
        # Dentro de la función 'cargar' antes de crear el engine:
        engine = create_engine(conn_uri)
        print("conexion exitosa a la BD")
        dataframe.to_sql(nombre_tabla, engine, if_exists='replace', index=False) # crea la tabla, si existe la reemplaza y carga los datos
        print("Datos cargados correctamente en la Base de Datos")
        proceso_log("Carga en Base de datos PostgreSQL")

    except Exception as ex:
        print(ex)
        print("No se pudieron cargar los datos")
        proceso_log(f"Error durante la carga: {ex}")
        raise # fuerza el fallo en caso de error

    proceso_log("Proceso de Carga Finalizado\n")
    proceso_log("Proceso ETL Finalizado\n")

#registro de procesos
def proceso_log(mensaje):
    formato_tiempo="%Y-%h-%d %H:%M:%S" #forma de fecha deseado
    fecha_actual=datetime.now() # fecha actual
    marca_de_tiempo=fecha_actual.strftime(formato_tiempo) # transformacion de la fecha
    with open(archivo_registro, "a") as registro: #abre o crea el archivo
        registro.write(marca_de_tiempo+ " , "+ mensaje+"\n") # escribe en el archivo

def enviar_email(ath_actual, precio_maximo):
    email_emisor = os.getenv("email_emisor")
    contraseña = os.getenv("contraseña_mail")
    email_receptor = os.getenv("email_receptor")

    subject = "Nuevo Maximo Historico BTC 🚀🚀🚀"
    
    # URL de un logo de Bitcoin pequeño (se recomienda alojar la propia imagen)
    # Usaremos un tamaño de 30x30px para que sea sutil.
    BITCOIN_LOGO_URL = "https://upload.wikimedia.org/wikipedia/commons/5/50/Bitcoin.png" # Ejemplo de logo BTC

    body_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <body>
        <div style="font-family: Arial, sans-serif; border: 1px solid #ddd; padding: 0; max-width: 600px; margin: 0 auto; border-radius: 8px; background-color: #f9f9f9;">
            
            <div style="background-color: #ff9900; color: white; padding: 15px 20px; border-radius: 8px 8px 0 0; text-align: center; font-size: 24px; font-weight: bold;">
                <img src="{BITCOIN_LOGO_URL}" alt="Logo BTC" style="vertical-align: middle; margin-right: 10px; width: 30px; height: 30px;">
                ¡NUEVO RÉCORD DETECTADO! 📈
            </div>
            
            <div style="padding: 20px; background-color: white; border-radius: 0 0 8px 8px;">
                <p style="font-size: 16px; color: #333;">
                    Bitcoin ha alcanzado un nuevo Máximo Histórico. 🚀🚀🚀 Los datos confirman:
                </p>
                
                <hr style="border: 0; height: 1px; background: #eee; margin: 20px 0;">
                
                <div style="margin-bottom: 15px; padding: 10px; border-left: 5px solid #28a745; background-color: #e6ffed;">
                    <p style="margin: 0; color: #555; font-size: 14px;">ATH Nuevo Maximo Historico:</p>
                    <p style="font-size: 28px; color: #28a745; font-weight: bold; margin: 5px 0 0 0;">
                        ${ath_actual} USD
                    </p>
                </div>
                
                <div style="margin-bottom: 20px; padding: 10px; border-left: 5px solid #007bff; background-color: #e9f5ff;">
                    <p style="margin: 0; color: #555; font-size: 14px;">Maximo Historico Anterior:</p>
                    <p style="font-size: 20px; color: #007bff; font-weight: bold; margin: 5px 0 0 0;">
                        ${precio_maximo} USD
                    </p>
                </div>
                
                <p style="margin-top: 30px; font-size: 12px; color: #888; text-align: center;">
                    Este correo es un aviso automático de su sistema Airflow.
                </p>
            </div>
        </div>
    </body>
    </html>
    """
    
    em = EmailMessage()
    em["From"] = email_emisor
    em["To"] = email_receptor
    em["Subject"] = subject
    
    # IMPORTANTE: Usar add_alternative para renderizar HTML
    em.add_alternative(body_html, subtype='html')

    contexto = ssl.create_default_context()

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=contexto) as smtp:
        smtp.login(email_emisor, contraseña)
        smtp.sendmail(email_emisor, email_receptor, em.as_string())

# Modificar esta función: NO necesita el Hook de Postgres ni actualizar ninguna tabla.
def verificar_alerta():
    # consulta los datos de la API
    datos_resumen_btc = api_key.get_coin_by_id(
        id="bitcoin",
        localization="false", # Desactiva la localización para simplificar
        tickers="false",
        market_data="true",  # Asegura que se incluyan los datos de mercado
        community_data="false",
        developer_data="false",
        sparkline="false"
    )

    # conexion a la BD
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn_uri = pg_hook.get_uri()
    engine = create_engine(conn_uri)

    # consultar maximo en la BD
    with engine.connect() as conexion:
        resultado = conexion.execute(text("SELECT MAX(maximo) FROM bitcoin_precios_30"))
        precio_maximo = resultado.scalar()

    try:
        # consulta el ATH o valor de precio maximo
        precio_maximo_historico = datos_resumen_btc['market_data']['ath']['usd']
        # Almacena el valor en una variable
        ath_actual = precio_maximo_historico
        
        # hace la comparacion entre el precio maximo y el maximo de los 30 dias
        if ath_actual >= precio_maximo:
            enviar_email(ath_actual, precio_maximo)
            proceso_log("Notificacion de Nuevo Maximo, enviado por Email")
        else:
            proceso_log("No hubo nuevo Maximo Historico")
            
    except Exception as e:
        proceso_log(f"Ocurrio el siguiente error:{e}")

# especificacion de argumentos del DAG
# se pueden anular por tarea durante la inicializacion del operador
default_args ={
    "owner":"Nicolas",
    "start_date": pendulum.today('UTC').add(days=-1),# decide la fecha de inicio 1 se ejecuta inmediatamente 0 se ejecuta a las 00:00 del otro dia
    #cantidad de veces que debe intentar si falla
    "retries":0,
    # tiempo de espera entre intentos
    "retry_delay":timedelta(minutes=5),
    "email":["nicolasmontairflow04@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True
}

# definir el DAG
dag = DAG(
    # nombre del DAG
    "BTC-Proyecto_Final",
    # argumentos del diccionario anterior
    default_args=default_args,
    # descripcion del flujo de trabajo
    description="Precio de BTC de los ultimos 30 dias",
    # instrucciones de programacion
    # el DAG se ejecutara cada 1 dia una vez implementado
    schedule="5 0 * * *",  # Ejecutar diariamente a la medianoche
)

#tarea 1 
# define la tarea llamada ejecutar_extraccion y llama a la funcion extraer
ejecutar_extraccion = PythonOperator(
    # id de la tarea
    task_id="extraer",
    # lo que realizara la tarea en este caso ejecutar la funcion extraer
    python_callable=extraer,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)

#tarea 2
# define la tarea llamada ejecutar_transformacion y llama a la funcion transformar
ejecutar_transformacion = PythonOperator(
    # id de la tarea
    task_id="transformar",
    # lo que realizara la tarea en este caso ejecutar la funcion transformar
    python_callable=transformar,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)

#tarea 3
# define la tarea llamada ejecutar_carga y llama a la funcion cargar
ejecutar_carga = PythonOperator(
    # id de la tarea
    task_id="cargar",
    # lo que realizara la tarea en este caso ejecutar la funcion transformar
    python_callable=cargar,
    # la tarea se asigna al DAG definido anteriormente 
    dag=dag,
)

#tarea 4
enviar_correo= PythonOperator(
    task_id="enviar_correo_electronico",
    python_callable=verificar_alerta,
    dag=dag,
)

# flujo de trabajo
ejecutar_extraccion >> ejecutar_transformacion >> enviar_correo >> ejecutar_carga