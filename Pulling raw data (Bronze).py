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

consumer_key = "consumer_key"
consumer_secret = "consumer_secret"
access_token = "access_token"
access_token_secret = "access_token_secret"
bearer_token = "bearer_token"

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


