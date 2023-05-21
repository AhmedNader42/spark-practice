from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()


# Broadcast Variables, Immutable variables that can be shared with executors
# in order to avoid resending or deserializing the value with each task.
my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(
    " "
)
words = spark.sparkContext.parallelize(my_collection, 2)

# If we want to supplement our list of words with other information from maybe
# a dictionary as following then we would need to store it as a broadcast variable
# technically, This can be considered as a right-join
# The reason we don't store the value in the code is that it can be a large value like
# a big lookup table or large ML model. It can be inefficient if you use it in a closure
# because it must be deserialized on the worker nodes once per task.
# Also, If we use the variable in multiple spark actions and jobs it will be re-sent
# to the worker every time with every job.

supplementalData = {"Spark": 1000, "Definitive": 200, "Big": -300, "Simple": 100}

suppBroadcast = spark.sparkContext.broadcast(supplementalData)

# We can later access this value by referencing the broadcast variable
suppBroadcast.value

words.map(lambda word: (word, suppBroadcast.value.get(word, 0))).sortBy(
    lambda wordPair: wordPair[1]
).toDF().show()

# Accumulators
# They provide a mutable variable that a spark cluster can update on a per-row basis.
# can be used for debugging or to create low-level aggregations.
flights = spark.read.parquet(
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/flight-data/parquet/2010-summary.parquet"
)
accChina = spark.sparkContext.accumulator(0)


def accChinaFunc(flight_row):
    destination = flight_row["DEST_COUNTRY_NAME"]
    origin = flight_row["ORIGIN_COUNTRY_NAME"]
    if destination == "China":
        accChina.add(flight_row["count"])
    if origin == "China":
        accChina.add(flight_row["count"])


flights.foreach(lambda flight_row: accChinaFunc(flight_row))
print(accChina.value)
