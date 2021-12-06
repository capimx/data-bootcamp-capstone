import pyspark
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.sql import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

#spark = SparkSession.builder\
#        .master("local")\
#        .appName("PySparkTutorial")\
#        .getOrCreate()

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dynamic_f = glueContext.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": ["s3://deb-bronze"]}, format = "csv", format_options={
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
#df.select(columns_to_write).write.parquet("c:/test.parquet",mode="overwrite")
