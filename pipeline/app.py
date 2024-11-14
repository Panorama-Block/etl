from flask import Flask, request, jsonify
import json
import pandas as pd
import os

from datetime import datetime
# import schedule

import core.minio_client as minio_client
import core.clickhouse_client as clickhouse_client
import core.data_processing as data_processing
import core.threads as threads

app = Flask(__name__)

# Create bucket if not exist
minio_client.create_bucket('data')

# Create table if not exist
create_table_sql = os.path.join("sql/create_table_temp1.sql")
clickhouse_client.execute_sql_script(create_table_sql)

# # Create table_gold if not exist
# create_table_gold_sql = os.path.join("sql/create_table_gold.sql")
# clickhouse_client.execute_sql_script(create_table_gold_sql)

# # Create views if not exist
# create_view_sql = os.path.join("sql/create_view.sql")
# clickhouse_client.execute_sql_script(create_view_sql)

# schedule.every().day.at("02:00").do(pipeline_etl.pipeline())

@app.errorhandler(404)
def not_found(error):
  return jsonify({"error": "Endpoint not found"}), 404
  
@app.route('/ingestion-csv', methods=['POST'])
def ingestion_csv():
  if 'file' not in request.files:
    return jsonify({"error": "No file found"}), 400
  
  for file in request.files.getlist('file'):
    if file and file.filename.endswith('.csv'):
      filename = file.filename.replace(".csv", datetime.now().strftime('_%m_%d_%Y_%H_%M_%S.csv')) 
      print(filename)
      temp_folder = 'temp'
      csv_path = os.path.join(temp_folder, filename)
      
      try:
        os.makedirs(temp_folder, exist_ok=True)
        
        file.save(csv_path)

        data_processing.convert_to_parquet(filename, temp_folder)
        
        parquet_filename = filename.replace(".csv", ".parquet")
        print(csv_path)
        parquet_path = os.path.join(temp_folder, parquet_filename)
        print(parquet_path)
        res = minio_client.upload_file('data', parquet_path, parquet_filename)
        # print(res)
        
        threads.transform_and_save(parquet_filename)
      except Exception as e:
        print(e)
        
      try:
          os.remove(csv_path)
          parquet_path = csv_path.replace(".csv", ".parquet")
          os.remove(parquet_path)
          print(f"File temp deleted with success")
      except Exception as e:
          print(f"Error to delete file '{csv_path}': {e}")
          
      return jsonify({"message": "File converted and uploaded with success!"}), 200
    
    else:
      return jsonify({"error": "Format not allowed"}), 400

@app.route('/ingestion-json', methods=['POST'])
def ingestion_json():
  data = request.get_json()
  
  if 'file' not in request.files:
    return jsonify({"error": "No file found"}), 400
  
  for file in request.files.getlist('file'):
    if file and file.filename.endswith('.json'):
      filename = file.filename.replace(".json", datetime.now().strftime('_%m_%d_%Y_%H_%M_%S.json')) 
      print(filename)
      temp_folder = 'temp'
      json_path = os.path.join(temp_folder, filename)
      
      try:
        os.makedirs(temp_folder, exist_ok=True)
        
        file.save(json_path)

        data_processing.convert_to_parquet(filename, temp_folder)
        
        parquet_filename = filename.replace(".json", ".parquet")
        print(json_path)
        parquet_path = os.path.join(temp_folder, parquet_filename)
        print(parquet_path)
        res = minio_client.upload_file('data', parquet_path, parquet_filename)
        # print(res)
        
        threads.transform_and_save(parquet_filename)
      except Exception as e:
        print(e)
        
      try:
          os.remove(json_path)
          parquet_path = json_path.replace(".json", ".parquet")
          os.remove(parquet_path)
          print(f"File temp deleted with success")
      except Exception as e:
          print(f"Error to delete file '{json_path}': {e}")
          
      return jsonify({"message": "File converted and uploaded with success!"}), 200
    
    else:
      return jsonify({"error": "Format not allowed"}), 400

  
  return data

@app.route('/bucket', methods=['POST'])
def create_bucket():
  data = request.get_json()
  
  if not data or 'bucket_name' not in data:
    return jsonify({"error": "Insert the name of the bucket"}), 400

  else:
    print(data['bucket_name'])
    res = minio_client.create_bucket(data['bucket_name'])
    return res

@app.route('/bucket', methods=['GET'])
def get_all_buckets():
  res = minio_client.list_buckets()
  return res

@app.route('/save-data', methods=['POST'])
def save_data():
  data = request.get_json()
  filename = data['filename']
  
  parquet_path = f"temp/downloaded_{filename}"
  
  res = minio_client.download_file("data", filename, parquet_path)
  
  df = pd.read_parquet(parquet_path)
  validation = data_processing.validate_file(filename, df)
  
  if not validation:
    return {"error": "Validation fail"}, 400
  
  df_prepared = data_processing.prepare_dataframe_for_insert(filename, df)
  
  client = clickhouse_client.get_client()
  res = clickhouse_client.insert_data(client, 'working_data', df_prepared)
  
  try:
    os.remove(parquet_path)
    print(f"File temp deleted with success")
  except Exception as e:
      print(f"Error to delete file '{parquet_path}': {e}")      
  
  return {"ok": "Success to save the data"}

if __name__ == '__main__':
  app.run(host="0.0.0.0", port=5000)