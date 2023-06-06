from pyspark.sql.functions import desc
from pyspark.sql.functions import max
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()
myRange = spark.range(1000).toDF("number")

divisBy2 = myRange.where("number % 2 = 0")
divisBy2.count()

flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/flight-data/csv/"
)

flightData2015 = (
    spark.read.option("inferSchema", "true")
    .option("header", "true")
    .csv(flightDataPath + "2015-summary.csv")
)

flightData2015.show()
flightData2015.take(3)

flightData2015.sort("count").explain()

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)

flightData2015.createOrReplaceTempView("flight_data_2015")

sqlWay = spark.sql(
    """
                   SELECT DEST_COUNTRY_NAME, COUNT(1)
                   FROM FLIGHT_DATA_2015
                   GROUP BY DEST_COUNTRY_NAME
"""
)

dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

dataFrameWay.explain()
sqlWay.explain()

spark.sql("SELECT max(count) from flight_data_2015").take(1)


print(flightData2015.select(max("count")).take(1))

# SQL way for top 5 destinations
maxSql = spark.sql(
    """
                   SELECT DEST_COUNTRY_NAME, SUM(COUNT) AS DESTINATION_TOTAL
                   FROM FLIGHT_DATA_2015
                   GROUP BY DEST_COUNTRY_NAME
                   ORDER BY SUM(COUNT) DESC
                   LIMIT 5
                   """
)

maxSql.show()

# Dataframe way
flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed(
    "sum(count)", "destination_total"
).sort(desc("destination_total")).limit(5).show()
flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed(
    "sum(count)", "destination_total"
).sort(desc("destination_total")).limit(5).explain()
