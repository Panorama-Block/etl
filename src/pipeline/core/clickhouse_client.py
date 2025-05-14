import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
  
def execute_sql_script(script_path):
    client = get_client()
    with open(script_path, 'r') as file:
        sql_script = file.read()
        
    sql_commands = sql_script.strip().split(';')
        
    for command in sql_commands:
        command = command.strip()
        
        if command:
            try:
                client.command(command)
            except Exception as e:
                print(f"Erro ao executar o comando: {command}\nErro: {e}")

def insert_data(client, table_name, df):
    try:
        print(table_name)
        print(df)
        client.insert_df(table_name, df)
    except Exception as e:
        print("Error in Insert data in ClickHouse", e)