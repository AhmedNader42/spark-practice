from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

flightDataPath = (
    "/home/ahmed/Documents/Spark-The-Definitive-Guide-master/data/retail-data/all/"
)

person = spark.createDataFrame(
    [
        (0, "Bill Chambers", 0, [100]),
        (1, "Matei Zaharia", 1, [500, 250, 100]),
        (2, "Michael Armbrust", 1, [250, 100]),
    ]
).toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame(
    [
        (0, "Masters", "School of Information", "UC Berkeley"),
        (2, "Masters", "EECS", "UC Berkeley"),
        (1, "Ph.D.", "EECS", "UC Berkeley"),
    ]
).toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame(
    [(500, "Vice President"), (250, "PMC Member"), (100, "Contributor")]
).toDF("id", "status")
person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")


# inner joins
joinExpression = person["graduate_program"] == graduateProgram["id"]

wrongJoinExpression = person["graduate_program"] == graduateProgram["id"]

joinType = "inner"

person.join(graduateProgram, joinExpression, joinType).show()

# outer join
joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

# left_outer join
joinType = "left_outer"
person.join(graduateProgram, joinExpression, joinType).show()

# right_outer join
joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()

# left_semi join, Doesn't retrieve any values from the right table. It will only check the value exists. Think of it like a filter on the dataframe
joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

graduateProgram2 = graduateProgram.union(
    spark.createDataFrame([(0, "Masters", "Duplicated Row", "Duplicated School")])
)
graduateProgram2.createOrReplaceTempView("gradProgram2")
graduateProgram2.join(person, joinExpression, joinType).show()

# left_anti join, Opposite of the semi join. It will only keep the values that DO NOT exist in the right df
joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

# cross join, Is an INNER join with no matching condition, So it will match every row on the left ot every row on the right. Causing an explosion in the number of rows

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()
person.crossJoin(graduateProgram).show()

# joins on complex types
from pyspark.sql.functions import expr

person.withColumnRenamed("id", "presonId").join(
    sparkStatus, expr("array_contains(spark_status, id)")
).show()

# duplicated column names
gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]

person.join(gradProgramDupe, joinExpr).show()
# person.join(gradProgramDupe, joinExpr).select("graduate_program").show() # Causes an error

# Approach 1: Different join expression
person.join(gradProgramDupe, "graduate_program").select("graduate_program").show()

# Approach 2: Drop column after join
person.join(gradProgramDupe, joinExpr).drop(person["graduate_program"]).select(
    "graduate_program"
).show()

# Approach 3: Rename column before the join
gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3, joinExpr).show()
