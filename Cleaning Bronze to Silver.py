# Databricks notebook source
from pyspark.sql.functions import concat, lit
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/user/hive/warehouse/bronze_tweets.db/raw_tweets")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

def is_retweet(df):
    def check_retweet(text):
        return text.startswith('RT')

    spark = SparkSession.builder.getOrCreate()
    check_retweet_udf = udf(check_retweet, BooleanType())

    df_with_retweet_flag = df.withColumn('is_retweet', check_retweet_udf(df['text']))

    return df_with_retweet_flag

df = is_retweet(df)

# COMMAND ----------

df = df.withColumn('tweet_url',
                   concat(
                       lit('https://www.twitter.com/'),
                       df['author_id'],
                       lit('/status/'),
                       df['tweet_id']
                         )
                    )

# COMMAND ----------

from afinn import Afinn 

afinn = Afinn()

# COMMAND ----------

def add_status(text):
    score=afinn.score(text)
    if(score<0):
        return 'Negative'
    elif(score==0):
        return 'Neutral'
    else:
        return 'Positive'

# COMMAND ----------

add_status_udf = udf(add_status,StringType())

# COMMAND ----------

sentiment_df = df.withColumn('sentiment',add_status_udf(df['text']))

# COMMAND ----------

sentiment_df.write.mode('append').format('parquet').saveAsTable('silver_tweets.sentiments')
