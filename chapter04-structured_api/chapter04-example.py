from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("spark-practice").getOrCreate()

"""_summary_
    Spark is like a programming language of it's own. It has it's own type system,
    Catalyst that maintains it's own type information through planning and processing.
"""

# When calculating the following code in either Scala or Python. The addition is actually performed in Spark
df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


# This results in an array of Row objects
spark.range(2).collect()

# Instantiating Spark types
from pyspark.sql.types import ByteType

b = ByteType()

"""
        This logical plan only represents a set of abstract transformations that do not refer to executors or
    drivers, it’s purely to convert the user’s set of expressions into the most optimized version. It
    does this by converting user code into an unresolved logical plan. This plan is unresolved
    because although your code might be valid, the tables or columns that it refers to might or might
    not exist. Spark uses the catalog, a repository of all table and DataFrame information, to resolve
    columns and tables in the analyzer. The analyzer might reject the unresolved logical plan if the
    required table or column name does not exist in the catalog. If the analyzer can resolve it, the
    result is passed through the Catalyst Optimizer, a collection of rules that attempt to optimize the
    logical plan by pushing down predicates or selections. Packages can extend the Catalyst to
    include their own rules for domain-specific optimizations.
"""


"""
        After successfully creating an optimized logical plan, Spark then begins the physical planning
    process. The physical plan, often called a Spark plan, specifies how the logical plan will execute
    on the cluster by generating different physical execution strategies and comparing them through
    a cost model, as depicted in Figure 4-3. An example of the cost comparison might be choosing
    how to perform a given join by looking at the physical attributes of a given table (how big the
    table is or how big its partitions are).
"""
