from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
from datetime import datetime
from google.cloud import bigquery

# parse arguments
parser = argparse.ArgumentParser(description='Process Google Cloud parameters.')
parser.add_argument('--projectid', required=True, help='Google Cloud Project ID')
parser.add_argument('--bq_dataset', required=True, help='BigQuery Dataset Name')
parser.add_argument('--bucket', required=True, help='Google Cloud Storage Bucket Name')
parser.add_argument('--input_date', required=True, help='Input date (YYYY-MM-DD)')
parser.add_argument('--frequency', required=True, help='Frequency of price data: 15m, 1h, 4h, 1d')
args = parser.parse_args()

# metadata
PROJECT_ID = args.projectid
BQ_DATASET = args.bq_dataset
BUCKET = args.bucket
INPUT_DATE = args.input_date
FREQUENCY=args.frequency
# jar for spark
bucket_jar_folder = f"gs://{BUCKET}/jars"
jar_spark_bigquery = "spark-3.5-bigquery-0.42.1.jar"
jar_gcs_connector = "gcs-connector-hadoop3-latest.jar"
# paths
btc_transactions_gcs_path = f"gs://{BUCKET}/btc_transactions/{INPUT_DATE}/btc_transactions_{INPUT_DATE}*.parquet"
btc_prices_gcs_path = f"gs://{BUCKET}/btc_prices/{INPUT_DATE}/btc_prices_{INPUT_DATE}_{FREQUENCY}.parquet"
# tables
tbl_transactions = "fact_transactions"
tbl_prices = "dim_market_price"

# create spark session
spark = SparkSession.builder \
    .appName("DE Zoomcamp Crypto") \
    .config("spark.jars", f"{bucket_jar_folder}/{jar_spark_bigquery},{bucket_jar_folder}/{jar_gcs_connector}") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

# read data from gcs
df_transactions = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(btc_transactions_gcs_path)
df_prices = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(btc_prices_gcs_path)

# standardize column names
transactions_col_mapping = {
    "hash": "transaction_id",
}
prices_col_mapping = {
    "Open time": "open_time",
    "Open": "open_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Volume": "volume",
    "Close time": "close_time",
    "Quote asset volume": "quote_asset_volume",
    "Number of trades": "number_of_trades",
    "Taker buy base asset volume": "taker_buy_base_asset_volume",
    "Taker buy quote asset volume": "taker_buy_quote_asset_volume",
    "Ignore": "ignore"
}
df_transactions = df_transactions.select([F.col(c).alias(transactions_col_mapping.get(c, c)) for c in df_transactions.columns])
df_prices = df_prices.select([F.col(c).alias(prices_col_mapping.get(c, c)) for c in df_prices.columns])

# in case we need to add new cryptos
df_transactions = df_transactions.withColumn("cryptocurrency", F.lit("BTC"))
df_prices = df_prices.withColumn("cryptocurrency", F.lit("BTC"))

# prepare dim_market_price table
df_dim_market_price = df_prices.select("cryptocurrency", "open_time", "open_price", "high_price", "low_price", "close_price", "volume", "number_of_trades")
# add column that represents the month
df_dim_market_price = df_dim_market_price.withColumn("timestamp_month", F.trunc(F.col("open_time"), "MM"))

# prepare fact_transactions table
df_fact_transactions = df_transactions.select("cryptocurrency", "transaction_id", "block_timestamp", "block_timestamp_month", "input_value", "output_value", "fee")
# add column that represents the 15m interval
df_fact_transactions = df_fact_transactions.withColumn("block_timestamp_15m", F.from_unixtime(F.floor(F.unix_timestamp("block_timestamp") / 900) * 900))
df_fact_transactions = df_fact_transactions.withColumn("block_timestamp_15m", F.col("block_timestamp_15m").cast("timestamp"))
# divide the input_value, output_value and fee by 10^8 to get the actual value
df_fact_transactions = df_fact_transactions.withColumn("input_value", F.col("input_value") / 100000000)
df_fact_transactions = df_fact_transactions.withColumn("output_value", F.col("output_value") / 100000000)
df_fact_transactions = df_fact_transactions.withColumn("fee", F.col("fee") / 100000000)
# lookup the dollar value of the transaction
df_fact_transactions = df_fact_transactions.join(df_prices.select("open_time", "close_price"), df_fact_transactions.block_timestamp_15m == df_prices.open_time, how="left")
df_fact_transactions = df_fact_transactions.withColumn("input_value_usd", F.col("close_price") * F.col("input_value"))
df_fact_transactions = df_fact_transactions.withColumn("output_value_usd", F.col("close_price") * F.col("output_value"))
df_fact_transactions = df_fact_transactions.withColumn("fee_usd", F.col("close_price") * F.col("fee"))
df_fact_transactions = df_fact_transactions.drop("open_time", "close_price")

# now convert all types of all the columns in fact_transactions and dim_market_price
# first convert for the fact_transactions table
df_fact_transactions = df_fact_transactions.withColumn("cryptocurrency", F.col("cryptocurrency").cast("string"))
df_fact_transactions = df_fact_transactions.withColumn("transaction_id", F.col("transaction_id").cast("string"))
df_fact_transactions = df_fact_transactions.withColumn("block_timestamp", F.col("block_timestamp").cast("timestamp"))
df_fact_transactions = df_fact_transactions.withColumn("block_timestamp_15m", F.col("block_timestamp_15m").cast("timestamp"))
df_fact_transactions = df_fact_transactions.withColumn("block_timestamp_month", F.trunc(F.col("block_timestamp"), "MM"))
# df_fact_transactions = df_fact_transactions.withColumn("block_timestamp_month", F.col("block_timestamp_month").cast("date"))
df_fact_transactions = df_fact_transactions.withColumn("input_value", F.col("input_value").cast("double"))
df_fact_transactions = df_fact_transactions.withColumn("output_value", F.col("output_value").cast("double"))
df_fact_transactions = df_fact_transactions.withColumn("fee", F.col("fee").cast("double"))
df_fact_transactions = df_fact_transactions.withColumn("input_value_usd", F.col("input_value_usd").cast("double"))
df_fact_transactions = df_fact_transactions.withColumn("output_value_usd", F.col("output_value_usd").cast("double"))
df_fact_transactions = df_fact_transactions.withColumn("fee_usd", F.col("fee_usd").cast("double"))
# and convert for the dim_market_price table
df_dim_market_price = df_dim_market_price.withColumn("cryptocurrency", F.col("cryptocurrency").cast("string"))
df_dim_market_price = df_dim_market_price.withColumn("open_time", F.col("open_time").cast("timestamp"))
df_dim_market_price = df_dim_market_price.withColumn("open_price", F.col("open_price").cast("double"))
df_dim_market_price = df_dim_market_price.withColumn("high_price", F.col("high_price").cast("double"))
df_dim_market_price = df_dim_market_price.withColumn("low_price", F.col("low_price").cast("double"))
df_dim_market_price = df_dim_market_price.withColumn("close_price", F.col("close_price").cast("double"))
df_dim_market_price = df_dim_market_price.withColumn("volume", F.col("volume").cast("double"))
df_dim_market_price = df_dim_market_price.withColumn("number_of_trades", F.col("number_of_trades").cast("integer"))
df_dim_market_price = df_dim_market_price.withColumn("timestamp_month", F.col("timestamp_month").cast("date"))

# delete existing records for the given INPUT_DATE from BigQuery
input_date = datetime.strptime(INPUT_DATE, '%Y-%m-%d')
first_day_of_month = input_date.replace(day=1).strftime('%Y-%m-%d')
client = bigquery.Client(project=PROJECT_ID)
# first the fact_transactions table
query = f"""
    DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{tbl_transactions}`
    WHERE block_timestamp_month = '{first_day_of_month}' AND
        DATE(block_timestamp) = '{INPUT_DATE}'
"""
query_job = client.query(query)
query_job.result()  # Waits for the job to finish
# then the dim_market_price table
query = f"""
    DELETE FROM `{PROJECT_ID}.{BQ_DATASET}.{tbl_prices}`
    WHERE timestamp_month = '{first_day_of_month}' AND
        DATE(open_time) = '{INPUT_DATE}'
"""
query_job = client.query(query)
query_job.result()  # Waits for the job to finish

# write data to bigquery
df_fact_transactions.write \
    .format("bigquery") \
    .option("table", f"{PROJECT_ID}:{BQ_DATASET}.{tbl_transactions}") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()
df_dim_market_price.write \
    .format("bigquery") \
    .option("table", f"{PROJECT_ID}:{BQ_DATASET}.{tbl_prices}") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save()

# stop the session
spark.stop()
