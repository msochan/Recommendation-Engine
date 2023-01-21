from unittest import TestCase
from pyspark.sql import SparkSession
from typing import Dict
import main


class TestRecommendationEngine(TestCase):
    def setUp(self):
        self.sku = "sku-188"
        self.spark = (
            SparkSession.builder.master("local[2]")
            .appName("recommendation_engine")
            .getOrCreate()
        )
        self.df = self.spark.read.json("small-test-data.json")

    def tearDown(self):
        self.spark.stop()

    def test_extract_item_attributes(self):
        actual = main.extract_article_attributes(self.sku, self.df)
        self.assertIsInstance(actual, Dict) and all(
            isinstance(key, str) for key in actual.keys()
        ) and all(isinstance(value, str) for value in actual.values())
        self.assertEqual(len(actual), 10)

    def test_calculate_potential_candidates(self):
        pass
