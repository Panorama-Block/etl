import re
from datetime import datetime
import os
from pyspark.sql.functions import current_timestamp, to_json, struct, lit

def prepare_dataframe_for_insert(filename, df, spark):
    # Convert all columns to a single JSON string column
    columns_to_json = [col for col in df.columns]
    df = df.select(
        current_timestamp().alias('date_ingestion'),
        to_json(struct(*columns_to_json)).alias('data_row'),
        lit(remove_file_pattern(filename).replace(".parquet", "")).alias('tag')
    )
    
    return df

def convert_to_parquet(spark, filename, temp_folder):
    if filename.endswith(".csv"):
        csv_path = os.path.join(temp_folder, filename)
        parquet_path = os.path.join(temp_folder, filename.replace(".csv", ".parquet"))
        df = spark.read.option("header", "true").csv(csv_path)
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Convertido: {csv_path} -> {parquet_path}")
    
    elif filename.endswith(".json"):
        json_path = os.path.join(temp_folder, filename)
        parquet_path = os.path.join(temp_folder, filename.replace(".json", ".parquet"))
        df = spark.read.json(json_path)
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Convertido: {json_path} -> {parquet_path}")
    
def remove_file_pattern(filename):
    pattern = r'(_\d{2}_\d{2}_\d{4}_\d{2}_\d{2}_\d{2})\.parquet$'
    filename = re.sub(pattern, '.parquet', filename)
    return filename