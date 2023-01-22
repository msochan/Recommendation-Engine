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
        self.engine = RecommendationEngine(
            sku_name="sku-188",
            df=self.df,
            recommend_num=self.recommended_num,
            spark_session=self.spark_session,
        )

    def tearDown(self):
        self.spark_session.stop()

    def test_extract_article_attributes(self):
        actual = self.engine.extract_article_attributes()
        self.assertIsInstance(actual, Dict) and all(
            isinstance(key, str) for key in actual.keys()
        ) and all(isinstance(value, str) for value in actual.values())
        self.assertEqual(len(actual), 10)

    def test_get_potential_articles_for_recommendation(self):
        pass
