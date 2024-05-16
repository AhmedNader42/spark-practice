from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 5)

path = "/home/ahmed/Documents/github/Spark-The-Definitive-Guide-master"

static = spark.read.json(path + "/data/activity-data")
dataSchema = static.schema

# Sockets
socketDF = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)


# WHEN data is output (Triggers)
streaming = (
    spark.readStream.schema(dataSchema)
    .option("maxFilesPerTrigger", 1)
    .json(path + "/data/activity-data")
)

activityCounts = streaming.groupBy("gt").count()

activityCounts.writeStream.trigger(processingTime="5 seconds").format(
    "console"
).outputMode("complete").start()


activityCounts.awaitTermination()

# Once trigger
activityCounts.writeStream.trigger(once=True).format("console").outputMode(
    "complete"
).start()
