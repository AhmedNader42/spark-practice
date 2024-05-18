from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 5)

path = "/home/ahmed/Documents/github/Spark-The-Definitive-Guide-master"

static = spark.read.json(path + "/data/activity-data")

streaming = (
    spark.readStream.schema(static.schema)
    .option("maxFilesPerTrigger", 10)
    .json(path + "/data/activity-data")
)

streaming.printSchema()

withEventTime = streaming.selectExpr(
    "*", "cast(cast(Creation_Time as double) / 1000000000 as timestamp) as event_time"
)

from pyspark.sql.functions import window, col


# withEventTime.groupBy(
#     window(col("event_time"), "10 minutes")
# ).count().writeStream.queryName("payment_per_window").format("console").outputMode(
#     "complete"
# ).start().awaitTermination()


# withEventTime.groupBy(
#     window(col("event_time"), "10 minutes", "5 minutes")
# ).count().writeStream.queryName("payments_per_window").format("console").outputMode(
#     "complete"
# ).start().awaitTermination()


# Watermark
# withEventTime.withWatermark("event_time", "30 minutes").groupBy(
#     window(col("event_time"), "10 minutes", "5 minutes")
# ).count().writeStream.queryName("payments_per_window").format("console").outputMode(
#     "complete"
# ).start().awaitTermination()


# Deduplication
withEventTime.withWatermark("event_time", "5 seconds").dropDuplicates(
    ["User", "event_time"]
).groupBy("User").count().writeStream.queryName("deduplicated").format(
    "console"
).outputMode(
    "complete"
).start().awaitTermination()
