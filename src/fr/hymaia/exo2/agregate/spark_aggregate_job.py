from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, asc
import os

def main():
    # Créer la Spark session
    spark = SparkSession.builder.appName("AggregateJob").getOrCreate()

    # Lecture du fichier résultat du premier job Spark
    df_clean_with_departement = spark.read.parquet("spark-handson/src/data/exo2/clean.parquet")
    df_sorted_population =  calculate_population_by_departement( df_clean_with_departement)
    
    # Chemin du dossier de output
    output_folder = "spark-handson/src/data/exo2/aggregate.csv"

    # Écriture du résultat
    df_sorted_population.write.csv(output_folder, header=True, mode="overwrite")

    # Arrêt de la session Spark
    spark.stop()

def calculate_population_by_departement(df):
    df_population_by_departement= df.groupBy("departement").agg(count("name").alias("nb_people"))
    # Tri du résultat par département et nombre de personnes
    df_sorted_population = df_population_by_departement.orderBy(desc("nb_people"), asc("departement"))
    return df_sorted_population
if __name__ == "__main__":
    main()
