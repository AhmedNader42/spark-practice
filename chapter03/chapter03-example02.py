from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col, date_format
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

"""_summary_
    This chapter covers the following:
        - Running production applications with spark-submit
        - Datasets: type-safe APIs for structured data
        - Structured Streaming
        - Machine learning and advanced analytics
        - Resilient Distributed Datasets(RDD): Sparks low level APIs
        - SparkR
        - The third-party package ecosystem
"""

# Spark-Submit example
# ./bin/spark-submit --master local ./examples/src/main/python/pi.py 100


# Structured Streaming
flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/retail-data/by-day/"
)

staticDataFrame = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(flightDataPath + "*.csv/")
)

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema


# Machine learning and advanced analytics
staticDataFrame.printSchema()
preppedDataFrame = (
    staticDataFrame.na.fill(0)
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)
)

trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")

print("Training count: " + str(trainDataFrame.count()))
print("Test count: " + str(testDataFrame.count()))

indexer = StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")

encoder = (
    OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")
)

vectorAssembler = (
    VectorAssembler()
    .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])
    .setOutputCol("features")
)

transformationPipeline = Pipeline().setStages([indexer, encoder, vectorAssembler])

fittedPipeline = transformationPipeline.fit(trainDataFrame)

transformedTraining = fittedPipeline.transform(trainDataFrame)

transformedTraining.cache()

kmeans = KMeans().setK(20).setSeed(1)

kmModel = kmeans.fit(transformedTraining)
kmModel.computeCost(transformedTraining)

transformedTest = fittedPipeline.transform(testDataFrame)
kmModel.computeCost(transformedTest)

# RDD
spark.sparkContext.parallelize([Row(1), Row(2), Row(3)]).toDF()
