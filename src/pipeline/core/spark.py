from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, array, lit, current_timestamp, monotonically_increasing_id, count, sum
from pyspark.sql.types import StringType, ArrayType, StructType, BooleanType, IntegerType
import json
import logging
import sys
from dataclasses import dataclass
from typing import List, Optional, Dict
from .minio_client import client
from io import BytesIO

# Get logger for this module
logger = logging.getLogger(__name__)

@dataclass
class SparkArgs:
    """Arguments for Spark operations"""
    file_name: str
    bucket: str
    minio_url: str
    minio_user: str
    minio_password: str
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str
    additional_args: Optional[Dict] = None

def run_spark_chains(args: SparkArgs) -> tuple:
    try:
        # Initialize a SparkSession
        spark = SparkSession.builder \
            .appName("ChainsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
            .config("spark.hadoop.fs.s3a.committer.name", "directory") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .getOrCreate()
        
        # Set log level to ERROR to reduce noise
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}", exc_info=True)
        return None, []

    spark_version = spark.version
    logger.info(f"Spark version: {spark_version}")

    try:
        # Get the parquet file from MinIO
        logger.info(f"Fetching parquet file from MinIO bucket: {args.bucket}")
        response = client.get_object(args.bucket, args.file_name)
        
        if response is None:
            logger.error("Failed to get object from MinIO")
            return spark_version, []
        
        # Read the data
        logger.info("Successfully retrieved file from MinIO, reading data...")
        data = response.read()
        if not data:
            logger.error("Received empty data from MinIO")
            return spark_version, []
            
        logger.info(f"Data size: {len(data)} bytes")

        # Create a temporary file to store the parquet data
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file.write(data)
            temp_file_path = temp_file.name

        try:
            # Read parquet with Spark from the temporary file
            logger.info("Attempting to read parquet data with Spark...")
            df = spark.read.parquet(temp_file_path)
            
            # Log schema information
            logger.info(f"DataFrame schema: {df.schema}")
            
            # Log column information
            columns = df.columns
            logger.info(f"Columns found: {columns}")
            
            if not columns:
                logger.error(f"No columns found in DataFrame. Schema: {df.schema}")
                return spark_version, []

            # Log sample data
            logger.info(f"First row of data: {df.first()}")
            
            # Log row count
            row_count = df.count()
            logger.info(f"Total rows in DataFrame: {row_count}")

            # Map columns to match ClickHouse table schema
            df = df.select(
                col("chainId").alias("chain_id"),
                col("chainName").alias("name"),
                col("description").alias("description"),
                col("rpcUrl").alias("rpc_url"),
                col("wsUrl").alias("ws_url"),
                col("explorerUrl").alias("explorer_url"),
                col("vmId").cast("string").alias("vm_info"),
                col("networkToken.symbol").alias("native_token"),
                col("enabledFeatures").cast("string").alias("features"),
                lit("active").alias("status"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            )

            # Read existing chains from ClickHouse
            logger.info("Reading existing chains from ClickHouse...")
            existing_chains = spark.read \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "chains") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .load()

            # Get list of existing chain_ids
            existing_chain_ids = existing_chains.select("chain_id").rdd.flatMap(lambda x: x).collect()
            logger.info(f"Found {len(existing_chain_ids)} existing chains")

            # Split DataFrame into new and existing chains
            new_chains = df.filter(~col("chain_id").isin(existing_chain_ids))
            existing_chains_to_update = df.filter(col("chain_id").isin(existing_chain_ids))

            # Write new chains
            if new_chains.count() > 0:
                logger.info(f"Writing {new_chains.count()} new chains...")
                new_chains.write \
                    .format("jdbc") \
                    .option("url", args.clickhouse_url) \
                    .option("dbtable", "chains") \
                    .option("user", args.clickhouse_user) \
                    .option("password", args.clickhouse_password) \
                    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                    .mode("append") \
                    .save()
                logger.info("Successfully wrote new chains to ClickHouse")

            # Update existing chains
            if existing_chains_to_update.count() > 0:
                logger.info(f"Updating {existing_chains_to_update.count()} existing chains...")
                # For each existing chain, update the record
                for row in existing_chains_to_update.collect():
                    update_query = f"""
                    ALTER TABLE chains
                    UPDATE 
                        name = '{row.name}',
                        description = '{row.description}',
                        rpc_url = '{row.rpc_url}',
                        ws_url = '{row.ws_url}',
                        explorer_url = '{row.explorer_url}',
                        vm_info = '{row.vm_info}',
                        native_token = '{row.native_token}',
                        features = '{row.features}',
                        status = '{row.status}',
                        updated_at = now()
                    WHERE chain_id = '{row.chain_id}'
                    """
                    spark.sql(update_query)
                logger.info("Successfully updated existing chains in ClickHouse")

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        logger.error(f"Error processing chains: {e}", exc_info=True)
        columns = []

    spark.stop()
    return spark_version, columns

def sanitize_for_jdbc(df):
    # Expande os campos de networkToken
    df = df.withColumn("networkToken_name", col("networkToken.name")) \
           .withColumn("networkToken_symbol", col("networkToken.symbol")) \
           .withColumn("networkToken_decimals", col("networkToken.decimals").cast("int")) \
           .withColumn("networkToken_logoUri", col("networkToken.logoUri")) \
           .withColumn("networkToken_description", col("networkToken.description")) \
           .drop("networkToken")

    # Sanitiza campo enabledFeatures (converte array<string> em string JSON)
    array_to_json_udf = udf(lambda x: json.dumps(x) if x else "[]")
    df = df.withColumn(
        "enabledFeatures",
        when(col("enabledFeatures").isNull(), "[]").otherwise(array_to_json_udf(col("enabledFeatures")))
    )

    # Converte booleanos para inteiros (UInt8 no ClickHouse)
    for boolean_field in ["isTestnet", "private"]:
        df = df.withColumn(boolean_field, when(col(boolean_field) == True, 1).otherwise(0))

    return df

def run_spark_blocks(args: SparkArgs) -> tuple:
    logger.info(f"Running spark_blocks with args: {args}")
    
    try:
        spark = SparkSession.builder \
            .appName("BlocksProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
            .config("spark.hadoop.fs.s3a.committer.name", "directory") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .getOrCreate()
        
        # Set log level to ERROR to reduce noise
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}", exc_info=True)
        return None, []
    
    spark_version = spark.version
    logger.info(f"Spark version: {spark_version}")

    # Validate bucket and file path
    if not args.bucket or not args.file_name:
        logger.error("Bucket or file_name is empty")
        return spark_version, []

    try:
        # Get the parquet file from MinIO
        logger.info(f"Fetching parquet file from MinIO bucket: {args.bucket}")
        response = client.get_object(args.bucket, args.file_name)
        
        if response is None:
            logger.error("Failed to get object from MinIO")
            return spark_version, []
        
        # Read the data
        logger.info("Successfully retrieved file from MinIO, reading data...")
        data = response.read()
        if not data:
            logger.error("Received empty data from MinIO")
            return spark_version, []
            
        logger.info(f"Data size: {len(data)} bytes")

        # Create a temporary file to store the parquet data
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file.write(data)
            temp_file_path = temp_file.name

        try:
            # Read parquet with Spark from the temporary file
            logger.info("Attempting to read parquet data with Spark...")
            df = spark.read.parquet(temp_file_path)
            
            # Log schema information
            logger.info(f"DataFrame schema: {df.schema}")
            
            # Log column information
            columns = df.columns
            logger.info(f"Columns found: {columns}")
            
            if not columns:
                logger.error(f"No columns found in DataFrame. Schema: {df.schema}")
                return spark_version, []

            # Log sample data
            logger.info(f"First row of data: {df.first()}")
            
            # Log row count
            row_count = df.count()
            logger.info(f"Total rows in DataFrame: {row_count}")

            # Calculate TPS (Transactions Per Second)
            # We'll use txCount and blockTimestamp to calculate TPS
            # For this example, we'll assume 1 second per block for simplicity
            # In a real scenario, you might want to calculate this based on actual block times
            df = df.withColumn("tps_avg", col("txCount").cast("float"))
            df = df.withColumn("tps_peak", col("txCount").cast("float"))  # Using same value for peak in this example

            # Map columns to match ClickHouse table schema
            df = df.select(
                col("blockNumber").cast("long").alias("block_number"),
                col("chainId").alias("chain_id"),
                col("blockHash").alias("block_hash"),
                col("parentHash").alias("parent_hash"),
                col("blockTimestamp").cast("long").alias("timestamp"),
                col("txCount").cast("int").alias("tx_count"),
                col("gasUsed").cast("long").alias("gas_used"),
                col("gasLimit").cast("long").alias("gas_limit"),
                col("baseFee").cast("long").alias("base_fee_per_gas"),
                col("tps_avg"),
                col("tps_peak")
            )

            # Write to blocks table
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "blocks") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info(f"Successfully wrote data to ClickHouse blocks table")

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        logger.error(f"Error processing blocks: {e}", exc_info=True)
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_erc20_transfers(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("ERC20Processor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
            .config("spark.hadoop.fs.s3a.committer.name", "directory") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .getOrCreate()
        
        # Set log level to ERROR to reduce noise
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}", exc_info=True)
        return None, []

    spark_version = spark.version
    logger.info(f"Spark version: {spark_version}")

    try:
        # Get the parquet file from MinIO
        logger.info(f"Fetching parquet file from MinIO bucket: {args.bucket}")
        response = client.get_object(args.bucket, args.file_name)
        
        if response is None:
            logger.error("Failed to get object from MinIO")
            return spark_version, []
        
        # Read the data
        logger.info("Successfully retrieved file from MinIO, reading data...")
        data = response.read()
        if not data:
            logger.error("Received empty data from MinIO")
            return spark_version, []
            
        logger.info(f"Data size: {len(data)} bytes")

        # Create a temporary file to store the parquet data
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file.write(data)
            temp_file_path = temp_file.name

        try:
            # Read parquet with Spark from the temporary file
            logger.info("Attempting to read parquet data with Spark...")
            df = spark.read.parquet(temp_file_path)
            
            # Log schema information
            logger.info(f"DataFrame schema: {df.schema}")
            
            # Log column information
            columns = df.columns
            logger.info(f"Columns found: {columns}")
            
            if not columns:
                logger.error(f"No columns found in DataFrame. Schema: {df.schema}")
                return spark_version, []

            # Log sample data
            logger.info(f"First row of data: {df.first()}")
            
            # Log row count
            row_count = df.count()
            logger.info(f"Total rows in DataFrame: {row_count}")

            # Map columns to match ClickHouse table schema
            df = df.select(
                monotonically_increasing_id().alias("id"),
                col("transactionHash").alias("tx_hash"),
                col("logIndex").cast("long").alias("log_id"),
                col("tokenAddress").alias("token_address"),
                col("fromAddress").alias("from_address"),
                col("toAddress").alias("to_address"),
                col("value").cast("decimal(38,0)").alias("amount_raw")
            )

            # Write to erc20_transfers table
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "erc20_transfers") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info(f"Successfully wrote data to ClickHouse erc20_transfers table")

            # Update token statistics
            token_stats = df.groupBy("token_address").agg(
                count("*").alias("transfer_count"),
                sum("amount_raw").alias("total_volume")
            )
            
            token_stats.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "tokens") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info(f"Successfully updated token statistics in ClickHouse")

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        logger.error(f"Error processing ERC20 transfers: {e}", exc_info=True)
        columns = []

    spark.stop()
    return spark_version, columns

def clean_blocks(df):
    # Add your block cleaning logic here
    return df

def calculate_block_metrics(df):
    # Add your metrics calculation logic here
    return df

def run_spark_transactions(args: SparkArgs) -> tuple:
    raise NotImplementedError("Not implemented")

def run_spark_logs(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("LogsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
            .config("spark.hadoop.fs.s3a.committer.name", "directory") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
            .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
            .getOrCreate()
        
        # Set log level to ERROR to reduce noise
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logger.error(f"Error initializing Spark session: {e}", exc_info=True)
        return None, []

    spark_version = spark.version
    logger.info(f"Spark version: {spark_version}")

    try:
        # Get the parquet file from MinIO
        logger.info(f"Fetching parquet file from MinIO bucket: {args.bucket}")
        response = client.get_object(args.bucket, args.file_name)
        
        if response is None:
            logger.error("Failed to get object from MinIO")
            return spark_version, []
        
        # Read the data
        logger.info("Successfully retrieved file from MinIO, reading data...")
        data = response.read()
        if not data:
            logger.error("Received empty data from MinIO")
            return spark_version, []
            
        logger.info(f"Data size: {len(data)} bytes")

        # Create a temporary file to store the parquet data
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
            temp_file.write(data)
            temp_file_path = temp_file.name

        try:
            # Read parquet with Spark from the temporary file
            logger.info("Attempting to read parquet data with Spark...")
            df = spark.read.parquet(temp_file_path)
            
            # Log schema information
            logger.info(f"DataFrame schema: {df.schema}")
            
            # Log column information
            columns = df.columns
            logger.info(f"Columns found: {columns}")
            
            if not columns:
                logger.error(f"No columns found in DataFrame. Schema: {df.schema}")
                return spark_version, []

            # Log sample data
            logger.info(f"First row of data: {df.first()}")
            
            # Log row count
            row_count = df.count()
            logger.info(f"Total rows in DataFrame: {row_count}")

            # Map columns to match ClickHouse table schema
            df = df.select(
                monotonically_increasing_id().alias("id"),
                col("transactionHash").alias("tx_hash"),
                col("logIndex").cast("int").alias("log_index"),
                col("address").alias("address"),
                col("topics").getItem(0).alias("topic0"),
                col("topics").getItem(1).alias("topic1"),
                col("topics").getItem(2).alias("topic2"),
                col("topics").getItem(3).alias("topic3"),
                col("data").alias("data"),
                lit(0).cast("int").alias("removed")
            )

            # Write to logs table
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "logs") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()

            logger.info(f"Successfully wrote data to ClickHouse logs table")

        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        
    except Exception as e:
        logger.error(f"Error processing logs: {e}", exc_info=True)
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_erc20(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("ERC20Processor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to erc20_transfers table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "erc20_transfers") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing ERC20 transfers: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_erc721(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("ERC721Processor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to erc721_transfers table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "erc721_transfers") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing ERC721 transfers: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_erc1155(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("ERC1155Processor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to erc1155_transfers table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "erc1155_transfers") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing ERC1155 transfers: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_internal_tx(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("InternalTxProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to internal_transactions table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "internal_transactions") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing internal transactions: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_metrics(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("MetricsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Process different types of metrics
        if "activity" in args.bucket:
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "activity_metrics") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()
        elif "performance" in args.bucket:
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "performance_metrics") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()
        elif "gas" in args.bucket:
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "gas_metrics") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()
        elif "cumulative" in args.bucket:
            df.write \
                .format("jdbc") \
                .option("url", args.clickhouse_url) \
                .option("dbtable", "cumulative_metrics") \
                .option("user", args.clickhouse_user) \
                .option("password", args.clickhouse_password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .mode("append") \
                .save()


    except Exception as e:
        logger.info(f"Error processing metrics: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_metrics_activity(args: SparkArgs) -> tuple:
    return run_spark_metrics(args)

def run_spark_metrics_performance(args: SparkArgs) -> tuple:
    return run_spark_metrics(args)

def run_spark_metrics_gas(args: SparkArgs) -> tuple:
    return run_spark_metrics(args)

def run_spark_metrics_cumulative(args: SparkArgs) -> tuple:
    return run_spark_metrics(args)

def run_spark_subnets(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("SubnetsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to subnets table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "subnets") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing subnets: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_blockchains(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("BlockchainsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.jars.packages", "org.apache.httpcomponents.client5:httpclient5:5.2.1") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns
        
        logger.info(f"columns: {columns}")

        # Map columns to match ClickHouse table schema
        df = df.select(
            col("blockchainId").alias("blockchain_id"),
            col("subnetId").alias("subnet_id"),
            col("vmId").alias("vm_id"),
            lit(None).cast("string").alias("name"),  # Default to NULL if not present
            current_timestamp().alias("created_at")  # Use current timestamp as default
        )

        # Write to blockchains table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "blockchains") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing blockchains: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_validators(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("ValidatorsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to validators table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "validators") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing validators: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_delegators(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("DelegatorsProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to delegators table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "delegators") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing delegators: {e}")
        columns = []

    spark.stop()
    return spark_version, columns

def run_spark_bridges(args: SparkArgs) -> tuple:
    try:
        spark = SparkSession.builder \
            .appName("BridgesProcessor") \
            .master("local[*]") \
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_url) \
            .config("spark.hadoop.fs.s3a.access.key", args.minio_user) \
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_password) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
    except Exception as e:
        logger.info(f"Error initializing Spark session: {e}")
        return None, []

    spark_version = spark.version
    s3_path = f"s3a://{args.bucket}/{args.file_name}"

    try:
        df = spark.read.parquet(s3_path)
        columns = df.columns

        # Write to teleporter_tx table
        df.write \
            .format("jdbc") \
            .option("url", args.clickhouse_url) \
            .option("dbtable", "teleporter_tx") \
            .option("user", args.clickhouse_user) \
            .option("password", args.clickhouse_password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()

    except Exception as e:
        logger.info(f"Error processing bridge transactions: {e}")
        columns = []

    spark.stop()
    return spark_version, columns