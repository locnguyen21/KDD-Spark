from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Create a Spark session
spark = SparkSession.builder.appName("OneHotEncodingExample").getOrCreate()

# Sample data
data = [("Alice", "A"), ("Bob", "B"), ("Catherine", "A"), ("David", "B"), ("Eve", "C")]
columns = ["Name", "Category"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
print("Original DataFrame:")
df.show()

indexer = StringIndexer(inputCol="Category", outputCol="CategoryIndex")
df_indexed = indexer.fit(df).transform(df)

# Show the DataFrame with indexed column
print("DataFrame with indexed column:")
df_indexed.show()

encoder = OneHotEncoder(inputCol="CategoryIndex", outputCol="CategoryVec")
df_encoded = encoder.fit(df_indexed).transform(df_indexed)

# Show the final DataFrame with one-hot encoded column
print("DataFrame with one-hot encoded column:")
df_encoded.show()