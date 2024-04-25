import pandas as pd
import numpy as np
import oracledb
import os
import requests
import warnings
from datetime import datetime, timedelta

class OracleDBConnection:
    def __init__(self, username, password, dsn):
        self.username = username
        self.password = password
        self.dsn = dsn
        self.conn = None

    def connect(self):
        directorio_actual = os.path.dirname(__file__)
        nueva_ruta = directorio_actual.replace("autonomia", "instantclient_21_12")
        print("Nueva ruta del cliente de Oracle:", nueva_ruta)

        try:
            oracledb.init_oracle_client(lib_dir=nueva_ruta)
            self.conn = oracledb.connect(user=self.username, password=self.password, dsn=self.dsn)
            print("Conexión a la base de datos establecida correctamente")
        except oracledb.DatabaseError as e:
            raise Exception(f"Error al conectar a Oracle: {e}")

    def execute_query(self, query):
        try:
            print("Inicio de consulta en BD")
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)
            column_names = [row[0] for row in cursor.description]
            result = cursor.fetchall()
            print("Final de consulta en BD")
            return column_names, result
        except oracledb.DatabaseError as e:
            raise Exception(f"Error al ejecutar la consulta: {e}")

    def close(self):
        if self.conn:
            self.conn.close()


# Parámetros de conexión a la base de datos
username = ''
password = ''
host = ''
port = 
service_name = ''
dsn = f"{host}:{port}/{service_name}"

# Configuración de Telegram
TELEGRAM_BOT_TOKEN = ''
TELEGRAM_CHAT_ID = ''

def enviar_mensaje_telegram(mensaje):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': mensaje
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Mensaje enviado a Telegram.")
        else:
            print("Error al enviar el mensaje a Telegram:", response.text)
    except Exception as e:
        print("Error al enviar el mensaje a Telegram:", e)

def consulta():
    try:
        db = OracleDBConnection(username, password, dsn)
        query = f"""SELECT 
	                 i.TICKETID,
                     i.RUTA_TKT,
                     i.INTERNALPRIORITY,
                     i.DESCRIPTION
                   FROM
                     CGU.INCIDENT i 
                   WHERE
                     i.CREATIONDATE >= TO_DATE('{(datetime.now() - timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')
                   AND
                     i.RUTA_TKT LIKE '%FIJOS%'
                   AND 
                     i.INTERNALPRIORITY = '1' """
        column_names, result = db.execute_query(query)

        # Imprimir los resultados para verificar si se obtienen correctamente
        print("Resultados de la consulta:")
        if result:
            print("Se encontraron coincidencias.")
            for row in result:
                ticket_id, ruta_tkt, priority, description = row
                mensaje = f"Ticket ID: {ticket_id}\nRuta: {ruta_tkt}\nPrioridad: {priority}\nDescripción: {description}"
                enviar_mensaje_telegram(mensaje)
        else:
            print("No se encontraron coincidencias.")

        df_result = pd.DataFrame(columns=column_names, data=result)
        return df_result
    except Exception as e:
        print("Error durante la consulta:", e)
        return None

# Deshabilitar las advertencias de RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Ejecutar la tarea de consulta y mensajes
p = consulta()
if p is not None:
    print("Resultados de la consulta:")
    print(p)
else:
    print("No se obtuvieron resultados de la consulta.")
