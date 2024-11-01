import arff
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import sqlite3
import pandas as pd

def load_arff_to_pandasDF(file_path):
# Load file ARFF
  with open(file_path, 'r') as f:
    data = arff.load(f)

# Trích xuất data và attribute 
  data_list = data['data']
  attribute_names = [attr[0] for attr in data['attributes']]

# Chuyển đổi sang pandas DataFrame
  df = pd.DataFrame(data_list, columns=attribute_names)
  return df

#def check_null(spark_df):
#  null_counts = {}
#  for col in spark_df.columns:
#    null_count = spark_df.filter(df[col].isNull()).count()
#    null_counts[col] = null_count
#  return null_counts