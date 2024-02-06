from pyspark.sql import SparkSession
def main():
 spark = SparkSession.builder \
 .appName("word") \
 .master("local[*]") \
 .getOrCreate()ls