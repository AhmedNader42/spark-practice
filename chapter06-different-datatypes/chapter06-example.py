from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/retail-data/by-day/"
)


df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(flightDataPath + "2010-12-01.csv")
)

df.printSchema()
df.createOrReplaceTempView("dfTable")

df.select("*").show()

# Converting types to Spark types
from pyspark.sql.functions import lit, col

df.select(lit(5), lit("five"), lit(5.0))

# Boolean types
df.where(col("InvoiceNo") != 536365).select("InvoiceNo", "Description").show(5, False)

df.where("InvoiceNo = 536365").show(5, False)
df.where("InvoiceNo <> 536365").show(5, False)

from pyspark.sql.functions import instr

priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

DOTCodeFilter = col("StockCode") == "DOT"

df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)).where(
    "isExpensive"
).select("unitPrice", "isExpensive").show(5)


from pyspark.sql.functions import expr

df.withColumn("isExpensive", expr("NOT UnitPrice <= 250")).where("isExpensive").select(
    "Description", "UnitPrice"
).show(5)


# Working with numbers
from pyspark.sql.functions import expr, pow

fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
df.selectExpr(
    "CustomerId", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity"
).show(
    2
)  # Same as above

from pyspark.sql.functions import lit, round, bround

## Round number
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

from pyspark.sql.functions import corr

df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

## Summary statistics
df.describe().show()

## Exact values
from pyspark.sql.functions import count, mean, stddev_pop, min, max

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
print(df.stat.approxQuantile("UnitPrice", quantileProbs, relError))
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

## Adding unique ID
from pyspark.sql.functions import monotonically_increasing_id

df.select(monotonically_increasing_id()).show(2)


# Working with Strings

from pyspark.sql.functions import initcap

df.select(initcap(col("Description"))).show()

## Upper and lower case
from pyspark.sql.functions import lower, upper

df.select(
    col("Description"), lower(col("Description")), upper(lower(col("Description")))
).show(2)

## Trimming
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim

df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("     HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp"),
).show(2)

## Regular expressions
from pyspark.sql.functions import regexp_replace

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
    regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
    col("Description"),
).show()

## Encode character as another character
from pyspark.sql.functions import translate

df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(2)

## Match based on certain regex
from pyspark.sql.functions import regexp_extract

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

df.select(
    regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
    col("Description"),
).show(10)

from pyspark.sql.functions import instr

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite).where(
    "hasSimpleColor"
).select("Description").show(3, False)


from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]


def color_locator(column, color_string):
    return (
        locate(color_string.upper(), column).cast("boolean").alias("is_" + color_string)
    )


selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*"))  # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red")).select(
    "Description"
).show(3, False)

# Dates and timestamps
from pyspark.sql.functions import current_date, current_timestamp

dateDF = (
    spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
)
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

## Adding and subtracting dates
from pyspark.sql.functions import date_add, date_sub

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

## Date diff
from pyspark.sql.functions import datediff, months_between, to_date

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(
    datediff(col("week_ago"), col("today"))
).show(1)
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"), to_date(lit("2017-05-22")).alias("end")
).select(months_between(col("start"), col("end"))).show(1)

from pyspark.sql.functions import to_date, lit

spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(
    1
)

## If parsing of a timestamp fails then it is returned as NULL
dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)

from pyspark.sql.functions import to_date

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
)
cleanDateDF.createOrReplaceTempView("dateTable2")

from pyspark.sql.functions import to_timestamp

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
