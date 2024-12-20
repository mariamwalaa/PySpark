import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when, lit

# Initialize Spark session
spark = SparkSession.builder.appName("PandasToPySparkPractice").getOrCreate()

# Sample data
data = {
    "Name": ["Alice", "Bob", "Charlie", "David", "Eva"],
    "Age": [25, 30, 35, 40, None],
    "Salary": [50000, 60000, None, 80000, 90000]
}

# Step 1: Create a Pandas DataFrame
pandas_df = pd.DataFrame(data)
print("Initial Pandas DataFrame:")
print(pandas_df)

# Step 2: Convert Pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)
print("\nConverted to PySpark DataFrame:")
spark_df.show()

# Step 3: Perform some PySpark transformations (e.g., fill nulls and calculate mean salary)
spark_df = spark_df.fillna({"Age": 0, "Salary": 0})
salary_mean = spark_df.select(mean(col("Salary"))).collect()[0][0]

# Add a new column "Above_Avg_Salary"
spark_df = spark_df.withColumn("Above_Avg_Salary", when(col("Salary") > salary_mean, lit(True)).otherwise(lit(False)))
print("\nTransformed PySpark DataFrame:")
spark_df.show()

# Step 4: Convert PySpark DataFrame back to Pandas DataFrame
pandas_df = spark_df.toPandas()
print("\nConverted back to Pandas DataFrame:")
print(pandas_df)

# Step 5: Perform a Pandas transformation (e.g., filter rows with Age > 30)
pandas_filtered = pandas_df[pandas_df["Age"] > 30]
print("\nFiltered Pandas DataFrame (Age > 30):")
print(pandas_filtered)

# Step 6: Convert the filtered Pandas DataFrame back to PySpark DataFrame
spark_filtered_df = spark.createDataFrame(pandas_filtered)
print("\nFiltered PySpark DataFrame:")
spark_filtered_df.show()

# Stop the Spark session
spark.stop()
