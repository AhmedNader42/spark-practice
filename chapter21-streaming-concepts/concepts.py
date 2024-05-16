from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 5)

path = "/home/ahmed/Documents/github/Spark-The-Definitive-Guide-master"

static = spark.read.json(path + "/data/activity-data")
dataSchema = static.schema


# This initializes the streaming object to read from the path
# for the data with the option specifying only 1 file per trigger.
streaming = (
    spark.readStream.schema(dataSchema)
    .option("maxFilesPerTrigger", 1)
    .json(path + "/data/activity-data")
)

activityCounts = streaming.groupBy("gt").count()

activityQuery = (
    activityCounts.writeStream.queryName("activity_counts")
    .format("memory")
    .outputMode("complete")
    # .start()
)


# activityQuery.awaitTermination()

from time import sleep

# for x in range(5):
#     spark.sql("SELECT * FROM activity_counts").show()
#     sleep(1)

from pyspark.sql.functions import expr

# Transformations
simpleTransform = (
    streaming.withColumn("stairs", expr("gt like '%stairs%'"))
    .where("stairs")
    .where("gt is not null")
    .select("gt", "model", "arrival_time", "creation_time")
    .writeStream.queryName("simple_transform")
    .format("memory")
    .outputMode("append")
    # .start()
)


# for x in range(10):
#     spark.sql("SELECT * FROM simple_transform").show()
#     sleep(1)

# Aggregations, Note that it is still not supported to have 2 levels of aggregations
deviceModelStats = (
    streaming.cube("gt", "model")
    .avg()
    .drop("avg(Arrival_Time)")
    .drop("avg(Creation_Time)")
    .drop("avg(Index)")
    .writeStream.queryName("device_counts")
    .format("memory")
    .outputMode("complete")
    # .start()
)

# for x in range(5):
#     spark.sql("SELECT * FROM device_counts").show()
#     sleep(1)

# JOINS, Only supported currently is static with streaming and not streaming X streaming
historicalAgg = static.groupBy("gt", "model").avg()
deviceModelStats = (
    streaming.drop("Arrival_Time", "Creation_Time", "Index")
    .cube("gt", "model")
    .avg()
    .join(historicalAgg, ["gt", "model"])
    .writeStream.queryName("device_counts")
    .format("memory")
    .outputMode("complete")
    .start()
)

# KAFKA
# Subscribe to 1 topic
df1 = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "topic1")
    .load()
)

# Subscribe to multiple topics
df2 = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscribe", "topic1,topic2")
    .load()
)

# Subscribe to a pattern
df3 = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    .option("subscibePattern", "topic.*")
    .load()
)

# Writing to Kafka
df1.selectExpr(
    "topic", "CAST(key AS STRING)", "CAST(value AS STRING)"
).writeStream.format("kafka").option(
    "kafka.bootstrap.servers", "host1:port1,host2:port2"
).option(
    "checkpointLocation", "/to/HDFS-compatible/dir"
).start()
df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format(
    "kafka"
).option("kafka.bootstrap.servers", "host1:port1,host2:port2").option(
    "checkpointLocation", "/to/HDFS-compatible/dir"
).option(
    "topic", "topic1"
).start()
