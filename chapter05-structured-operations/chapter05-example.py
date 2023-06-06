from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column, expr, lit


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/flight-data/"
)

df = spark.read.format("json").load(flightDataPath + "json/2015-summary.json")

df.printSchema()


myManualSchema = StructType(
    [
        StructField("DEST_COUNTRY_NAME", StringType(), True),
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
        StructField("count", StringType(), False, metadata={"hello": "world"}),
    ]
)
df = (
    spark.read.format("json")
    .schema(myManualSchema)
    .load(flightDataPath + "json/2015-summary.json")
)
df.printSchema()

# Columns and expressions
"""
        To Spark, columns are logical constructions that simply represent a value 
    computed on a per-record basis by means of an expression. This means that to have 
    a real value for a column, we need to have a row; and to have a row, we need to have
    a DataFrame. You cannot manipulate an individual column outside the context of 
    a DataFrame; you must use Spark transformations within a DataFrame to modify the 
    contents of a column.
"""

col("someColumnName")
column("someColumnName")

# df.col("count")


# Columns are just expressions
(((col("someCol") + 5) * 200) - 6) < col("otherCol")


expr("(((someCol + 5) * 200) - 6) < otherCol")

# Programmatically accessing Dataframe columns
print(spark.read.format("json").load(flightDataPath + "json/2015-summary.json").columns)

print(df.first())

# Creating Rows
myRow = Row("Hello", None, 1, False)
print(myRow[0])
print(myRow[2])


# When working with dataframes there are fundamental operations. We can
"""
    - Add rows or columns
    - Remove rows or columns
    - Transform a row into a column (or vice versa)
    - Change the order of rows based on the values in columns
"""

# Creating dataframes
df = spark.read.format("json").load(flightDataPath + "json/2015-summary.json")
df.createOrReplaceTempView("dfTable")


myManualSchema = StructType(
    [
        StructField("some", StringType(), True),
        StructField("col", StringType(), True),
        StructField("names", LongType(), False),
    ]
)
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()


df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)


df.select(
    expr("DEST_COUNTRY_NAME"), col("DEST_COUNTRY_NAME"), column("DEST_COUNTRY_NAME")
).show(2)


# Select with Alias
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

# Select Expression

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

df.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(2)

# Aggregation
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# Literals

df.select(expr("*"), lit(1).alias("One")).show(2)

# Adding columns
df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)

# Rename columns
print(df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns)

# Reserved characters and keywords
dfWithLongColName = df.withColumn("This Long Column-Name", expr("ORIGIN_COUNTRY_NAME"))
print(dfWithLongColName.columns)

dfWithLongColName.selectExpr(
    "`This Long Column-Name`", "`This Long Column-Name` as `new col`"
).show(2)
dfWithLongColName.createOrReplaceTempView("dfTableLong")
print(dfWithLongColName.select(expr("`This Long Column-Name`")).columns)

# Dropping columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

# Changing column type (Casting)
df.withColumn("count2", col("count").cast("long"))

# Filtering columns
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)

# Unique rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
df.select("ORIGIN_COUNTRY_NAME").distinct().count()

# Sampling rows
seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())

# Random splits
dataFrames = df.randomSplit([0.25, 0.75], seed)
print(dataFrames[0].count() > dataFrames[1].count())


# Concatenating and appending Rows (Union)
"""
            WARNING
    Unions are currently performed based on location, not on the schema. This means that
    columns will not automatically line up the way you think they might
"""

schema = df.schema
newRows = [
    Row("New Country", "Other Country", 5),
    Row("New Country 2", "Other Country 3", 1),
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF).where("count = 1").where(
    col("ORIGIN_COUNTRY_NAME") != "United States"
).show()


# Sorting rows
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# Specify sort direction explicitly

df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# To improve performance sort within the partitions first before applying
# transformations
spark.read.format("json").load(
    flightDataPath + "json/2015-summary.json"
).sortWithinPartitions("count")


# Limit
df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()

# Repartition and coalesce

print(df.rdd.getNumPartitions())
df.repartition(5)
print(df.rdd.getNumPartitions())
df.repartition(col("DEST_COUNTRY_NAME"))
df.repartition(5, col("DEST_COUNTRY_NAME"))
print(df.rdd.getNumPartitions())


df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)


# Collecting rows to the driver
collectDF = df.limit(10)
collectDF.take(5)  # take works with an Integer count
collectDF.show()  # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()
collectDF.toLocalIterator()

"""WARNING
        Any collection of data to the driver can be a very expensive operation! If you 
    have a large dataset and call collect, you can crash the driver. If you use 
    toLocalIterator and have very large partitions, you can easily crash the driver node
    and lose the state of your application. This is also expensive because we can 
    operate on a one-by-one basis, instead of running computation in parallel.
"""
