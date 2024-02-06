import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    # 1. On définit master à local[*]
    # 2. On donne un nom à notre job Spark : `wordcount`
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.csv("/home/bania2001/spark-handson/src/resources/exo1/data.csv", header=True, inferSchema=True)
    df.show()

     #Appeler la fonction wordcount avec le bon nom de colonne
    result_df = wordcount(df, "text")  # Remplacez "votre_colonne" par le nom réel de votre colonne

    #Écrire le résultat dans data/exo1/output au format parquet, partitionné par "count"
    output_path = "data/exo1/output"
    result_df.write.partitionBy("count").parquet(output_path)

    # 5. Pour exécuter le code : 
    # (Cela peut être réalisé en exécutant le script fr.hymaia.exo1.main.py dans votre IDE ou en utilisant spark-submit)
    # Lire les données Parquet
    parquet_data = spark.read.parquet("data/exo1/output")
    # Afficher le contenu du DataFrame
    parquet_data.show()

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .agg(f.count('*').alias('count'))



#if __name__ == "__main__":
 #   main()
