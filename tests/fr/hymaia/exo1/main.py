from pyspark.sql import SparkSession
def main():
 spark = SparkSession.builder \
 .appName("wordcount") \
 .master("local[*]") \
 .getOrCreate()


#* Que signifie local[*] ?
#* Pourquoi avoir choisi local[*] et non pas simplement local ?l