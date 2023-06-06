from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    date_add,
    date_sub,
    months_between,
    datediff,
    lit,
    to_timestamp,
    to_date,
    current_date,
    current_timestamp,
    expr,
    locate,
    instr,
    regexp_extract,
    translate,
    regexp_replace,
    ltrim,
    rtrim,
    rpad,
    lpad,
    trim,
    lower,
    upper,
    initcap,
    monotonically_increasing_id,
    corr,
    round,
    bround,
    pow,
    col,
    udf,
    struct,
    split,
    size,
    array_contains,
    explode,
    create_map,
    get_json_object,
    json_tuple,
    to_json,
    from_json,
)
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    StringType,
)

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

df.select(lit(5), lit("five"), lit(5.0))

# Boolean types
df.where(col("InvoiceNo") != 536365).select("InvoiceNo", "Description").show(5, False)

df.where("InvoiceNo = 536365").show(5, False)
df.where("InvoiceNo <> 536365").show(5, False)


priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

DOTCodeFilter = col("StockCode") == "DOT"

df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)).where(
    "isExpensive"
).select("unitPrice", "isExpensive").show(5)


df.withColumn("isExpensive", expr("NOT UnitPrice <= 250")).where("isExpensive").select(
    "Description", "UnitPrice"
).show(5)


# Working with numbers

fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)
df.selectExpr(
    "CustomerId", "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity"
).show(
    2
)  # Same as above


## Round number
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)


df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

## Summary statistics
df.describe().show()

## Exact values

colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
print(df.stat.approxQuantile("UnitPrice", quantileProbs, relError))
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

## Adding unique ID

df.select(monotonically_increasing_id()).show(2)


# Working with Strings


df.select(initcap(col("Description"))).show()

## Upper and lower case

df.select(
    col("Description"), lower(col("Description")), upper(lower(col("Description")))
).show(2)

## Trimming

df.select(
    ltrim(lit("    HELLO    ")).alias("ltrim"),
    rtrim(lit("    HELLO    ")).alias("rtrim"),
    trim(lit("     HELLO    ")).alias("trim"),
    lpad(lit("HELLO"), 3, " ").alias("lp"),
    rpad(lit("HELLO"), 10, " ").alias("rp"),
).show(2)

## Regular expressions

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
    regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
    col("Description"),
).show()

## Encode character as another character

df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(2)

## Match based on certain regex

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"

df.select(
    regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
    col("Description"),
).show(10)


containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite).where(
    "hasSimpleColor"
).select("Description").show(3, False)


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

dateDF = (
    spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
)

dateDF.printSchema()

## Adding and subtracting dates

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

## Date diff

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(
    datediff(col("week_ago"), col("today"))
).show(1)

dateDF.select(
    to_date(lit("2016-01-01")).alias("start"), to_date(lit("2017-05-22")).alias("end")
).select(months_between(col("start"), col("end"))).show(1)


spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(
    1
)

## If parsing of a timestamp fails then it is returned as NULL
dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)


dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
)
cleanDateDF.createOrReplaceTempView("dateTable2")


cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()

# Coalesce

df.select(coalesce(col("Description"), col("CustomerId"))).show()

# ifnull, nullif, nvl, nvl2

"""
    SELECT 
        ifnull(null, 'return_value') 
        // 'return_value' because first value is null
        nullif('value', 'value') 
        // null because both values match
        nvl(null, 'return_value') 
        // 'return_value' because 1st value is null, Otherwise would return 1st value
        nvl2('not_null', 'return_value', 'else_value') 
        // 'return_value' because 1st value is not null, Otherwise return 'else_value'
"""

# drop null values
print("************************************************************")
print(df.count())  # 3108
print("************************************************************")
print(df.na.drop("any").count())  # 1968 # Drop if any of the column values has a null
print("************************************************************")
print(df.na.drop("all").count())  # 3108, Because all the columns values have to be null

df.na.drop("all", subset=["StockCode", "InvoiceNo"])
print(
    df.na.drop("all", subset=["StockCode", "InvoiceNo"]).count()
)  # 3108, Because all the specified columns values have to be null

# fill
df.na.fill("All the null values become this string")
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
fill_cols_values = {
    "StockCode": 5,
    "Description": "No value",
}  # You can use this to specify different fill value for different columns
df.na.fill(fill_cols_values)


# replace, You can use it for more than null values
df.na.replace([""], ["Unknown"], "Description")

# Complex Types
## struct

complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

complexDF.select(
    "complex.Description"
).show()  # You need to use the dot notation in order to access elements
complexDF.select(col("complex").getField("Description")).show()  # Similar way

complexDF.select("complex.*")  # Get all values from struct

# Arrays


## split

df.select(split(col("Description"), " ")).show()

df.select(split(col("Description"), " ").alias("array_col")).selectExpr(
    "array_col[0]"
).show(2)

## size
df.select(size(split(col("Description"), " "))).show(2)

## array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

## explode
df.withColumn("splitted", split(col("Description"), " ")).withColumn(
    "exploded", explode(col("splitted"))
).select("Description", "InvoiceNo", "exploded").show(truncate=False)

## map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(
    2, False
)

df.select(
    create_map(col("Description"), col("InvoiceNo")).alias("complex_map")
).selectExpr("complex_map['WHITE METAL LANTERN']").show(
    2, False
)  # You can access elements like arrays

df.select(
    create_map(col("Description"), col("InvoiceNo")).alias("complex_map")
).selectExpr("explode(complex_map)").show(2, False)

# JSON

jsonDF = spark.range(1).selectExpr(
    """
    '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString 
"""
)

jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),
    json_tuple(col("jsonString"), "myJSONKey"),
).show(2, False)

## to_json, Used to convert a struct to a JSON object

df.selectExpr("(InvoiceNo, Description) as myStruct").select(
    to_json(col("myStruct"))
).show(truncate=False)

## from_json, Convert from JSON to struct. Will need to define a schema

parseSchema = StructType(
    (
        StructField("InvoiceNo", StringType(), True),
        StructField("Description", StringType(), True),
    )
)
df.selectExpr("(InvoiceNo, Description) as myStruct").select(
    to_json(col("myStruct")).alias("newJSON")
).select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)

# UDF

udfExampleDF = spark.range(5).toDF("num")


def power3(double_value):
    return double_value**3


power3(2.0)
power3udf = udf(power3)
udfExampleDF.select(power3udf(col("num"))).show(2, False)

# register udf, Can be used in SQL. Also, You should register the function in scala
# before using in Python (More in notes)
"""
    // In Scala
    spark.udf.register("power3", power3(_:Double):Double) 
"""
# udfExampleDF.selectExpr("power3(num)").show(2)


spark.udf.register("power3py", power3, DoubleType())
udfExampleDF.selectExpr("power3py(num)").show(2, False)
