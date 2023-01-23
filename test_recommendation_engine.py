from unittest import TestCase
from pyspark.sql import SparkSession
from typing import Dict
from recommendation_engine import RecommendationEngine


class TestRecommendationEngine(TestCase):
    def setUp(self):
        self.spark_session = (
            SparkSession.builder.master("local[2]")
            .appName("recommendation_engine")
            .getOrCreate()
        )
        self.df = self.spark_session.read.json("test_recommendation_engine.json")
        self.recommended_num = 2
        self.sku_name = "sku-188"
        self.engine = RecommendationEngine(
            sku_name=self.sku_name,
            df=self.df,
            recommend_num=self.recommended_num,
            spark_session=self.spark_session,
        )

    def tearDown(self):
        self.spark_session.stop()

    def test_extract_article_attributes(self):
        # Testing if return value is having Dict[str, str] type
        actual_attributes = self.engine.extract_article_attributes()
        self.assertIsInstance(actual_attributes, Dict) and all(
            isinstance(key, str) for key in actual_attributes.keys()
        ) and all(isinstance(value, str) for value in actual_attributes.values())

        # Testing if amount of extracted attributes is valid
        actual_attributes_length = len(actual_attributes)
        expected_attributes_length = 10
        self.assertEqual(actual_attributes_length, expected_attributes_length)

    def test_get_potential_articles_for_recommendation(self):
        # Testing if returning DataFrame is having proper columns
        df_actual = self.engine.df_for_recommendation
        actual_columns = df_actual.columns
        expected_columns = ["sku", "attributes", "attribute_matches", "num_of_matches"]
        self.assertEqual(actual_columns, expected_columns)

        # Testing if there is no given article sku inside of the returning DataFrame
        sku_to_list = df_actual.rdd.map(lambda x: x.sku).collect()
        self.assertNotIn(self.sku_name, sku_to_list)

    def test_get_matching_statistics(self):
        # Testing if returning DataFrame is having proper columns
        df_stats_actual = self.engine.get_matching_statistics()
        actual_columns = df_stats_actual.columns
        expected_columns = ["num_of_matches", "count", "running_total"]
        self.assertEqual(actual_columns, expected_columns)

    def test_get_recommendations_limits(self):
        # Testing if returning tuple object got proper length
        actual_tuple_length = len(self.engine.get_recommendations_limits())
        expected_tuple_length = 2
        self.assertEqual(actual_tuple_length, expected_tuple_length)

    def test_get_recommendations(self):
        # Testing if returning articles list are valid
        df = self.engine.get_recommendations()
        actual_sku_list = df.rdd.map(lambda x: x.sku).collect()
        expected_sku_list = ["sku-189", "sku-185"]
        self.assertEqual(actual_sku_list, expected_sku_list)
