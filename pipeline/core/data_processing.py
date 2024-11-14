import re
import pandas as pd
from datetime import datetime
import os

def prepare_dataframe_for_insert(filename, df):
  df['date_ingestion'] = datetime.now()
  df['data_row'] = df.apply(lambda row: row.to_json(), axis=1)
  df['tag'] = remove_file_pattern(filename).replace(".parquet", "")
  
  return df[['date_ingestion', 'data_row', 'tag']]

def convert_to_parquet(filename, temp_folder):
  if filename.endswith(".csv"):
    csv_path = os.path.join(temp_folder, filename)
    parquet_path = os.path.join(temp_folder, filename.replace(".csv", ".parquet"))
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False)
    print(f"Convertido: {csv_path} -> {parquet_path}")
  
  elif filename.endswith(".json"):
    json_path = os.path.join(temp_folder, filename)
    parquet_path = os.path.join(temp_folder, filename.replace(".json", ".parquet"))
    df = pd.read_json(json_path)
    df.to_parquet(parquet_path, index=False)
    print(f"Convertido: {json_path} -> {parquet_path}")
    
def remove_file_pattern(filename):
  pattern = r'(_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}_\d{2})\.parquet$'
  filename = re.sub(pattern, '.parquet', filename)
  return filename