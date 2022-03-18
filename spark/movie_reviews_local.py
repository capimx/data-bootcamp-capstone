#import pyspark
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains
#from pyspark.sql import 
#from pyspark import SparkContext, SparkConf
#import sys
from pyspark.sql import SparkSession
import os
import sys

#from pyspark.context import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable


if __name__ == '__main__':

    spark = SparkSession.builder\
            .master("local")\
            .appName("MovieReviews")\
            .getOrCreate()

    dfReviews = spark.read.format('csv').options(header='true', inferSchema='true').load("/Users/mauricio.caballero/Documents/de-app/capstone/data/movie_review.csv")

    print(dfReviews.printSchema())

    tokenizer = Tokenizer(inputCol="review_str", outputCol="review_tokens")
    remover = StopWordsRemover(inputCol="review_tokens", outputCol="filtered_review_tokens")
    #remover.setInputCol("")
    #remover.setOutputCol("")
    remover.loadDefaultStopWords('english')
    dfReviews = tokenizer.transform(dfReviews)
    dfReviews = remover.transform(dfReviews)
    dfReviews = dfReviews.withColumn("positive_review", array_contains(dfReviews.filtered_review_tokens,"good").cast('integer') )
    columns_to_write = ["cid","positive_review"]
    #df.select(columns_to_write).write.csv("s3a://deb-silver/movie_positive_reviews.csv")

    dynamic_f_cleaned = dfReviews.select(columns_to_write)

    #datasink2 = spark.write_dynamic_frame.from_options(frame = dynamic_f_cleaned, connection_type = "s3", connection_options = {"path": "s3://deb-silver//positive_reviews"}, format = "parquet")
    dynamic_f_cleaned.write.mode("overwrite").parquet("/Users/mauricio.caballero/Documents/de-app/capstone/data/output-parquet.parquet")
    dynamic_f_cleaned.write.mode("overwrite").csv("/Users/mauricio.caballero/Documents/de-app/capstone/data/output-csv.csv")
    dynamic_f_cleaned.show()
    dynamic_f_cleaned.printSchema()
    spark.stop()

""" dynamic_f = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://deb-bronze"]}, format = "csv", format_options={
        "withHeader": True,
        "separator": ","
    })

#spark.read.option("header",True).csv("s3a://deb-bronze/movie_review.csv")
#df.printSchema()
df = dynamic_f.toDF()

tokenizer = Tokenizer(inputCol="review_str", outputCol="review_tokens")
remover = StopWordsRemover(inputCol="review_tokens", outputCol="filtered_review_tokens")
#remover.setInputCol("")
#remover.setOutputCol("")
remover.loadDefaultStopWords('english')
df = tokenizer.transform(df)
df = remover.transform(df)

df = df.withColumn("positive_review", pyspark.sql.functions.array_contains(df.filtered_review_tokens,"good").cast('integer') )
columns_to_write = ["cid","positive_review"]
#df.select(columns_to_write).write.csv("s3a://deb-silver/movie_positive_reviews.csv")

dynamic_f_cleaned = DynamicFrame.fromDF(df.select(columns_to_write), glueContext,"positive_reviews")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = dynamic_f_cleaned, connection_type = "s3", connection_options = {"path": "s3://deb-silver//positive_reviews"}, format = "parquet")
job.commit()
#df.select(columns_to_write).write.parquet("c:/test.parquet",mode="overwrite") """