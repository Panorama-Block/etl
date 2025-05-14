#!/usr/bin/env python3
import subprocess
import os
import logging

logging.basicConfig(level=logging.INFO)

def submit_spark_job():
    # Define the path to the spark_job.py on the shared volume accessible by Spark.
    app_resource = "/opt/spark-apps/spark_job.py"
    
    # Build the spark-submit command.
    spark_submit_cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",  # Use your Spark master service name and port.
        "--deploy-mode", "client",  # Alternatively, you can use "cluster" mode.
        app_resource
    ]
    
    # Optionally, you can pass environment variables required by the job.
    env = os.environ.copy()
    env["MINIO_URL"] = os.environ.get("MINIO_URL", "http://minio:9000")
    env["MINIO_USER"] = os.environ.get("MINIO_USER", "minio_user")
    env["MINIO_PASSWORD"] = os.environ.get("MINIO_PASSWORD", "minio_password")
    env["MINIO_BUCKET"] = os.environ.get("MINIO_BUCKET", "data")
    # Add other variables as needed.
    
    logging.info("Submitting Spark job to Spark cluster...")
    process = subprocess.Popen(spark_submit_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        logging.error(f"Spark job failed: {stderr}")
    else:
        logging.info(f"Spark job completed successfully: {stdout}")

if __name__ == "__main__":
    submit_spark_job()
