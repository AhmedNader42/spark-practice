from pyspark.sql.functions import window, desc, col
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

print(staticSchema)

spark.conf.set("spark.sql.shuffle.partitions", "5")
# Normal
staticDataFrame.selectExpr(
    "CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate"
).groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day")).sum(
    "total_cost"
).orderBy(
    desc("sum(total_cost)")
).show(
    5
)

# Streaming way
streamingDataFrame = (
    spark.readStream.schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load(flightDataPath + "*.csv/")
)
print(streamingDataFrame.isStreaming)

purchaseByCustomerPerHour = (
    streamingDataFrame.selectExpr(
        "CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate"
    )
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
)

purchaseByCustomerPerHour.writeStream.format("memory").queryName(
    "customer_purchases"
).outputMode("complete").start()

spark.sql(
    """
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
"""
).show(5)
