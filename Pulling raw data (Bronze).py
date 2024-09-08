# Databricks notebook source
# MAGIC %md
# MAGIC ###Libs

# COMMAND ----------

import tweepy
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType
from requests.exceptions import HTTPError
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/user/hive/warehouse/bronze_tweets.db/raw_tweets")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Keys

# COMMAND ----------

consumer_key = "buvR1ZAQAgLE2K1lKllLUeKc0"
consumer_secret = "1kLmNLdJ45Cq69JD99zg2Mhsj1qytUdHnV1LAql8ES5yjK1LRh"
access_token = "1102561094143492096-egRhRF60iwZdR6JCt0yyS9Skh1Eo6n"
access_token_secret = "YjFlGlJEdwhyiwiqWuctfg4IVNbe4noCOm0CNTNjpCsAO"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAJtApAEAAAAAJD8GQaV2f4ppLjafysgPX3BTCfM%3DqZmNDYzhnhexQ2IpdAD6X1w5PFSxqliFcA0UC8g5UgFhjYWKZ0"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating spark dataframe, pulling tweets from API

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

client = tweepy.Client(bearer_token)

schema = StructType([
    StructField("tweet_id", LongType(), True),
    StructField("author_id", LongType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("text", StringType(), True)
])

tweets = spark.createDataFrame([], schema=schema)

# COMMAND ----------

# Get the tweets
#response = client.search_recent_tweets("Covid lang:en", tweet_fields=["created_at","author_id"], max_results=100)
#tweets = response.data

## Create a list of dictionaries from the tweets
#tweets_list = [{"tweet_id": int(tweet.id), "author_id": int(tweet.author_id), "created_at": tweet.created_at, "text": tweet.text} for tweet in tweets]

## Create a DataFrame from the list of dictionaries
#df = spark.createDataFrame(tweets_list, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Converting dataframe schema and writing to 'raw_tweets' lake

# COMMAND ----------

tweets = tweets.withColumn("created_at", col("created_at").cast("string"))

# COMMAND ----------

tweets.write.format("parquet").mode("overwrite").saveAsTable("bronze_tweets.raw_tweets")

# COMMAND ----------


