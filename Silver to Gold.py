# Databricks notebook source
df = spark.read.parquet('dbfs:/user/hive/warehouse/silver_tweets.db/sentiments')

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO gold_tweets.gold_sentiments (DATE, negative_tweets, neutral_tweets, positive_tweets)
# MAGIC SELECT 
# MAGIC     current_date(),
# MAGIC     COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) AS negative_tweets,
# MAGIC     COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) AS neutral_tweets,
# MAGIC     COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_tweets
# MAGIC FROM 
# MAGIC     silver_tweets.sentiments;
# MAGIC

# COMMAND ----------

# MAGIC %sql drop table if exists gold_tweets.gold_sentiments

# COMMAND ----------

# MAGIC %sql create table if not exists gold_tweets.gold_sentiments (
# MAGIC   date DATE,
# MAGIC   negative_tweets int,
# MAGIC   neutral_tweets int,
# MAGIC   positive_tweets int
# MAGIC )

# COMMAND ----------

html = '<h1 style=text-align:center;>Tweet Sentiments</h1>'

displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_tweets.gold_sentiments

# COMMAND ----------


