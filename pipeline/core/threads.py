import threading
import pandas as pd
import os

import core.minio_client as minio_client
import core.clickhouse_client as clickhouse_client
import core.data_processing as data_processing

def transform_and_save(filename):  
  thread = threading.Thread(target=pipeline, args=(filename,))
  thread.start()
  print("Pipeline thread started")
  
def pipeline(filename):
  parquet_path = f"temp/downloaded_{filename}"
  
  res = minio_client.download_file("data", filename, parquet_path)
  
  df = pd.read_parquet(parquet_path)
  
  # validation = data_processing.validate_file(filename, df)
  
  # if not validation:
  #   return {"error": "Validation fail"}, 400
  
  df_prepared = data_processing.prepare_dataframe_for_insert(filename, df)
  
  client = clickhouse_client.get_client()
  clickhouse_client.insert_data(client, 'working_data', df_prepared)
  
  try:
    os.remove(parquet_path)
    print(f"File temp deleted with success")
  except Exception as e:
      print(f"Error to delete file '{parquet_path}': {e}")
          
  return {"ok": "Success to save the data"}