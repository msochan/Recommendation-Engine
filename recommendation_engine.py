import logger
import sys
from typing import Dict
from pyspark.sql.functions import col, desc, sum, udf, size
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.window import Window


class RecommendationEngine:
    def __init__(
        self,
        sku_name: str,
        df: DataFrame,
        recommend_num: int,
        spark_session: SparkSession,
    ) -> None:
        self.sku_name = sku_name
        self.recommend_num = recommend_num
        self.spark_session = spark_session
        self.df = df
        self.df_for_recommendation = self.get_potential_articles_for_recommendation()

    # Extracting atributes from given article
    def extract_article_attributes(self) -> Dict[str, str]:
        try:
            article_attributes = (
                self.df.filter(self.df.sku == self.sku_name)
                .select("attributes")
                .first()
                .attributes
            )
        except AttributeError:
            logger.logger.error(
                f"Article with SKU= {self.sku_name} not found in the input .json file!"
            )
            self.spark_session.stop()
            sys.exit()
        else:
            return article_attributes.asDict()

    # Returning DataFrame of articles that will be use for further recommendations excluding sku from input article
    def get_potential_articles_for_recommendation(self) -> None:
        input_article_attributes = self.extract_article_attributes()

        df_for_recommendation = (
            self.df.filter(self.df.sku != self.sku_name)
            .withColumn(
                "attribute_matches",
                self.attributes_matcher_wrapper(input_article_attributes)(
                    col("attributes")
                ),
            )
            .withColumn("num_of_matches", size(col("attribute_matches")))
            .select(["sku", "attributes", "attribute_matches", "num_of_matches"])
        )

        return df_for_recommendation

    # UDF that can be applied on column to find matches for every row (it seems that PySpark in DataFrame API doesn't have map function)
    def attributes_matcher_wrapper(self, article_attributes):
        def attributes_matcher_UDF(row):
            row_to_dict = row.asDict()
            attribute_matches = {
                key: value
                for key, value in row_to_dict.items()
                if key in article_attributes and article_attributes[key] == value
            }

            return sorted(attribute_matches.keys())

        return udf(attributes_matcher_UDF, returnType=ArrayType(StringType()))

    # Returning DataFrame with following columns: number of matches, count and running_total for matching attributes
    def get_matching_statistics(self) -> DataFrame:
        window = Window.orderBy(desc(col("num_of_matches"))).rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        df_stats = (
            self.df_for_recommendation.groupBy(col("num_of_matches"))
            .count()
            .withColumn("running_total", sum("count").over(window))
        )

        df_stats.show()

        return df_stats

    def get_recommendations_limits(self) -> Row:

        df_stats_to_rows = (
            self.get_matching_statistics()
            .orderBy(desc(col("num_of_matches")))
            .collect()
        )

        top_recommendation_limit_row = None
        for row in df_stats_to_rows:
            # print(row)
            # top
            if row.running_total <= self.recommend_num:
                top_recommendation_limit_row = row
            if row.running_total > self.recommend_num:
                after_top_recommendation_limit_row = row
                break

        return Row(
            top_limit=top_recommendation_limit_row,
            bottom_limit=after_top_recommendation_limit_row,
        )

    def get_recommendations(self):
        limit_row = self.get_recommendations_limits()

        top_limit = limit_row.top_limit
        bottom_limit = limit_row.bottom_limit

        top_recommendations = self.df_for_recommendation.filter(
            col("num_of_matches") >= top_limit.num_of_matches
        )

        num_of_additional_articles = (
            min(self.recommend_num, self.df_for_recommendation.count())
            - top_limit.running_total
        )

        if num_of_additional_articles > 0:
            additional_recommendations = (
                self.df_for_recommendation.filter(
                    col("num_of_matches") == bottom_limit.num_of_matches
                )
                .orderBy(col("attribute_matches"))
                .limit(num_of_additional_articles)
            )
            df_recommendations = top_recommendations.union(additional_recommendations)
        else:
            df_recommendations = top_recommendations

        # col("sku") remove later - this is only for reproducibility purposes
        return df_recommendations.orderBy(
            desc(col("num_of_matches")), col("attribute_matches"), col("sku")
        )
