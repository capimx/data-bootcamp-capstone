from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import os

if __name__ == '__main__':
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

    spark = SparkSession.builder\
            .master("local")\
            .appName("LogReviews")\
            .getOrCreate()

    df = spark.read.format('com.databricks.spark.xml')\
    .options(rootTag='reviewlog')\
    .options(rowTag='log')\
    .load("/Users/mauricio.caballero/Documents/de-app/capstone/data/log_reviews.csv")
    df.show(truncate=False)
    df.printSchema()

    df.write.mode("overwrite").csv("s3://wz-de-academy-mau-stage-data/processed-logs.csv")

    df.write.mode("overwrite").parquet("s3://wz-de-academy-mau-stage-data/processed-logs.parquet")
