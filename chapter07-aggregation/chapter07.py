from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, desc, max, dense_rank, rank
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    count,
    countDistinct,
    approxCountDistinct,
    first,
    last,
    min,
    sum,
    sumDistinct,
    avg,
    expr,
    var_pop,
    stddev_pop,
    var_samp,
    stddev_samp,
    skewness,
    kurtosis,
    corr,
    covar_pop,
    covar_samp,
    collect_list,
    collect_set,
)


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/retail-data/all/"
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(flightDataPath + "*.csv")
    .coalesce(5)
)
df.cache()
df.createOrReplaceTempView("dfTable")
df.count() == 541909  # To execute the caching action

# count
df.select(count("StockCode")).show()  # 541909, Warning: Will count all the nulls

# count distinct

df.select(countDistinct("StockCode")).show()

# approx count distinct, When working with large datasets, Oftentimes the exact count is
# irrelevant to a certain degree of accuracy
df.select(
    approxCountDistinct("StockCode", 0.1)
).show()  # 3364, Because we specified a big maximum estimation error

# first and last
df.select(first("StockCode"), last("StockCode")).show()

# min and max
df.select(min("StockCode"), max("Quantity")).show()

# sum
df.select(sum("Quantity")).show()

# sum distinct
df.select(sumDistinct("Quantity")).show()  # 29310

# avg
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"),
).selectExpr(
    "total_purchases/total_transactions", "avg_purchases", "mean_purchases"
).show()


# variance and stdv
df.select(
    var_pop("Quantity"),
    var_samp("Quantity"),
    stddev_pop("Quantity"),
    stddev_samp("Quantity"),
).show()

# skewness and kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# covariance and correlation
df.select(
    corr("InvoiceNo", "Quantity"),
    covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity"),
).show()

# Aggregating to complex types
df.agg(collect_set("Country"), collect_list("Country")).show()


# Grouping
df.groupBy("InvoiceNo", "CustomerId").count().show()
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"), expr("count(Quantity)")
).show()

# Grouping with maps
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"), expr("stddev_pop(Quantity)")).show()


# Windows
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

windowSpec = (
    Window.partitionBy("CustomerId", "date")
    .orderBy(desc("Quantity"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity"),
).show()

# Grouping Sets, incomplete example since grouping set function is only available in SQL
# and rollup or cube operations offer the same results
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

# rollups
rolledUpDF = (
    dfNoNull.rollup("Date", "Country")
    .agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")
).show()

# cube
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity"))).select(
    "Date", "Country", "sum(Quantity)"
).orderBy("Date").show()

# grouping metadata

# pivot
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date>'2011-12-05'").select(
    "date", "`USA_sum(Quantity)`", "`Italy_sum(Quantity)`"
).show()

# UDAF
