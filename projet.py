from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Project").master("local[*]").getOrCreate()

data_file = "Data/full.csv"

df = spark.read.csv(data_file, multiLine=True, header=True, escape='"')

df_one = df.groupby("repo") \
	.agg(count("commit") \
	.alias("CountOfCommit")) \
	.filter("repo is not NULL") \
	.orderBy(desc("CountOfCommit")).limit(10)

print("Question 1")
df_one.show()

df_two = df.where(col("repo") =="apache/spark").groupby("author").agg(count("commit").alias("CountOfCommit")).orderBy(desc("CountOfCommit")).limit(1)


print("Question 2")
df_two.show()

df_three = df.select("author","commit","repo",to_date(col("date").substr(5,20),"M d H:mm:ss yyyy").alias("date_format")) \
	.where(col("repo") == "apache/spark") \
	.where(months_between(current_date(),col("date_format")) <= 24).groupby("author").agg(count("commit") \
	.alias("CountOfCommit")).orderBy(desc("CountOfCommit")).limit(10)

print("Question 3")
df_three.show()

file = open("Data/archive/englishST.txt", "r")
list_words = []
	
for line in file:
    stripped_line = line.strip()
    line_list = stripped_line.split()
    list_words.append(stripped_line)
	
file.close()
	
df_four = df.withColumn('word', explode(split(col('message'), ' '))) \
    .where(col('word').isin(*list_words) == False) \
    .where(col('word') != '') \
    .groupBy('word') \
    .agg(count("word").alias("CountOfWords")) \
    .orderBy(desc("CountOfWords")).limit(10)

print("Question 4")
df_four.show()
