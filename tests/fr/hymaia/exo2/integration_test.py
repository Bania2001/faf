import unittest
from pyspark.sql import SparkSession
from tests.fr.hymaia.spark_test_case import spark 
from pyspark.sql.functions import count, desc, asc
import os
from src.fr.hymaia.exo2.spark_clean_job import filter1, jointure, ajoutColonne 
from src.fr.hymaia.exo2.agregate.spark_aggregate_job import calculate_population_by_departement

class IntegrationTest(unittest.TestCase):
    def test_integration(self):
        # Chemin des fichiers CSV
        file1_path="spark-handson/src/resources/exo2/clients_bdd.csv"
        file2_path="spark-handson/src/resources/exo2/city_zipcode.csv"
        
        # Lecture des fichiers CSV
        df1 = spark.read.csv(file1_path, header=True, inferSchema=True)
        df2 = spark.read.csv(file2_path, header=True, inferSchema=True)

        # Filtrage des données du premier DataFrame
        df1_filtered = filter1(df1)

        # Jointure des DataFrames
        df_joined = jointure(df1_filtered, df2)

        # Ajout de la colonne de département
        df_with_department = ajoutColonne(df_joined)

        # Chemin de sortie pour les résultats du premier job
        output_path_first_job = "spark-handson/src/data/exo2/clean.parquet"

        # Ecriture du résultat du premier job dans un fichier Parquet
        df_with_department.write.mode("overwrite").parquet(output_path_first_job)

        # Chemin de lecture du résultat du premier job pour le deuxième job
        input_path_second_job = output_path_first_job

        # Lecture du resultat du premier job
        df_clean_with_department = spark.read.parquet(input_path_second_job)

        # Calcul de la population par departement pour le deuxième job
        df_population_by_department = calculate_population_by_departement(df_clean_with_department)

        # Chemin de sortie pour les résultats du deuxième job
        output_path_second_job = "spark-handson/src/data/exo2/aggregate.csv"

        # Écriture du résultat du deuxième job dans un CSV
        df_population_by_department.write.csv(output_path_second_job, header=True, mode="overwrite")

        # Vérification que les fichiers de sortie existent 
        self.assertTrue(os.path.exists(output_path_first_job))
        self.assertTrue(os.path.exists(output_path_second_job))


if __name__ == "__main__":
    unittest.main()
