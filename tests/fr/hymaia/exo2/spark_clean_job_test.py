import unittest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row 
from tests.fr.hymaia.spark_test_case import spark 
import sys

from src.fr.hymaia.exo2.spark_clean_job import filter1, jointure, ajoutColonne 
from src.fr.hymaia.exo2.agregate.spark_aggregate_job import calculate_population_by_departement

class SparkScriptTest(unittest.TestCase):
    def test_filter_function(self):
        # Given
        df1_data = [Row(name='Alice', age=25, zip='20190'), Row(name='Bob', age=16, zip='67890'), Row(name='Mimi', age=26, zip='20190'), Row(name='Jack', age=20, zip='67890')]
        df1 = spark.createDataFrame(df1_data)
        # When
        # Test d'erreur : passer un DataFrame vide à la fonction filter
        with self.assertRaises(ValueError):
            filter1(spark.createDataFrame([], schema=df1.schema))
        # When
        filtered_df = filter1(df1)
        expected_filtered_df = spark.createDataFrame([Row(name='Alice', age=25, zip='20190'), Row(name='Mimi', age=26, zip='20190'), Row(name='Jack', age=20, zip='67890')])

        # Then
        self.assertEqual(filtered_df.collect(), expected_filtered_df.collect())

    def test_jointure_function(self):
        # Given
        df1_data = [Row(name='Alice', age=25, zip='20190'), Row(name='Mimi', age=26, zip='20190'), Row(name='Jack', age=20, zip='67890')]
        df2_data = [Row(zip='20190', city='CityA'), Row(zip='67890', city='CityB')]
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        df1.show()

        # When
        joined_df = jointure(df1, df2)
        expected_joined_df = spark.createDataFrame([Row( name='Mimi', age=26, zip='20190',city='CityA'), Row(name='Alice', age=25, zip='20190',city='CityA'), Row(name='Jack', age=20,zip='67890', city='CityB')])
        #Mon test renvoie des erreurs car il respecte pas l'ordre de dataframe 
        #du coup j'ai opté pour cette méthode pour le probleme d'ordre des lignes dans le datafram joind_df
        # Then
        # Convertir les collectes en ensembles pour ignorer l'ordre
        joined_set = set(map(tuple, joined_df.collect()))
        expected_set = set(map(tuple, expected_joined_df.collect()))
        # Vérifier l'égalité des ensembles
        self.assertEqual(joined_set, expected_set)


    def test_ajoutColonne_function(self):
        # Given
        df_data = [Row(zip='20190', name='Alice', age=25, city='CityA'), Row(zip='20190', name='Mimi', age=26, city='CityA'), Row(zip='67890', name='Jack', age=20, city='CityB')]
        df = spark.createDataFrame(df_data)

        # When
        result_with_department_df = ajoutColonne(df)
        expected_result_df = spark.createDataFrame([Row(zip='20190', name='Alice', age=25, city='CityA', departement='2B'), Row(zip='20190', name='Mimi', age=26, city='CityA', departement='2B'), Row(zip='67890', name='Jack', age=20, city='CityB', departement='67')])

        # Then
        self.assertEqual(result_with_department_df.collect(), expected_result_df.collect())

    def test_calculate_population_by_departement_function(self):
        # Given
        df_data = [Row(zip='20190', name='Alice', age=25, city='CityA', departement='2B'), Row(zip='20190', name='Mimi', age=26, city='CityA', departement='2B'), Row(zip='67890', name='Jack', age=20, city='CityB', departement='67')]
        df = spark.createDataFrame(df_data)

        # When
        population_per_departamant_desc = calculate_population_by_departement(df)
        expected_data = [Row(departement='2B', nb_people=2), Row(departement='67', nb_people=1)]
        expected_df = spark.createDataFrame(expected_data)

        # Then
        self.assertEqual(population_per_departamant_desc.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
