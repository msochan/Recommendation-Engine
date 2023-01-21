import logger
import argparse
import sys
from typing import Dict
from pyspark.sql.functions import col, desc, sum, udf, size
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

# Extracting atributes from given article
def extract_article_attributes(article_sku: str, df: DataFrame) -> Dict[str, str]:
    attributes = (
        df.filter(df.sku == article_sku).select("attributes").first().attributes
    )
    return attributes.asDict()


# Returning DataFrame of articles that will be use for further recommendations
def get_potential_article_candidates(sku: str, articles: DataFrame) -> DataFrame:
    input_item_attributes = extract_article_attributes(sku, articles)

    df_candidates = (
        articles.filter(articles.sku != sku)
        .withColumn(
            "attribute_matches",
            attributes_matcher_wrapper(input_item_attributes)(col("attributes")),
        )
        .withColumn("num_of_matches", size(col("attribute_matches")))
    )
    return df_candidates


# refactor to-do
def calculate_recommendations(candidates: DataFrame, recommend_num: int) -> DataFrame:
    matching_counts = cal_amount_of_matching_attributes(candidates)

    select_all_items_until_count = None
    for match in matching_counts.orderBy(desc(col("amount"))).collect():
        if match.running_total <= recommend_num:
            select_all_items_until_count = match
    print(select_all_items_until_count)

    number_of_additional_items_needed = 0
    if select_all_items_until_count:
        number_of_additional_items_needed = (
            min(recommend_num, candidates.count())
            - select_all_items_until_count.running_total
        )

    # print(f"CHUJ {number_of_additional_items_needed}")

    count_where_select_is_needed = 0
    for match in matching_counts.orderBy((col("amount"))).collect():
        if match.running_total > recommend_num:
            count_where_select_is_needed = match.amount
    # print(f"KUTAS {count_where_select_is_needed}")

    logger.logger.info(
        f"Select {number_of_additional_items_needed} additional articles "
        + f"with {count_where_select_is_needed} matching attributes based on given sorting criterion."
    )

    # for amount = 5, where DataFrame (5,2)
    filtering_value = select_all_items_until_count["amount"]
    # print(filtering_value)
    itemsWithoutSelection = candidates.filter(col("num_of_matches") >= filtering_value)
    # itemsWithoutSelection.show()

    if number_of_additional_items_needed > 0:
        additionalItems = (
            candidates.filter(col("num_of_matches") == count_where_select_is_needed)
            .orderBy(col("attribute_matches"))
            .limit(number_of_additional_items_needed)
        )
        unsortedRecommendations = itemsWithoutSelection.union(additionalItems)
    else:
        unsortedRecommendations = itemsWithoutSelection

    # unsortedRecommendations.show()

    # eventually in a situation when there is the same amount of matches within multiple rows
    # then we could sort at the end by column "sku" or column "attributes"
    return unsortedRecommendations.orderBy(
        desc(col("num_of_matches")), col("attribute_matches"), col("sku")
    )


# UDF that can be applied on column to find matches for every row (it seems that PySpark in DataFrame API doesn't have map function)
def attributes_matcher_wrapper(attributes):
    def attributes_matcher_UDF(row):
        row_to_dict = row.asDict()
        attribute_matches = {
            key: value
            for key, value in row_to_dict.items()
            if key in attributes and attributes[key] == value
        }

        return sorted(attribute_matches.keys())

    return udf(attributes_matcher_UDF, returnType=ArrayType(StringType()))


# refactor to-do
def cal_amount_of_matching_attributes(candidates: DataFrame) -> DataFrame:
    window = Window.orderBy(desc(col("num_of_matches"))).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    candidate_statistics = (
        candidates.groupBy(col("num_of_matches"))
        .count()
        .withColumn("running_total", sum("count").over(window))
        .select(col("num_of_matches").alias("amount"), col("running_total"))
    )

    candidate_statistics.show()

    return candidate_statistics


# refactor to-do
def main(params):
    # local[2] for 2 cores
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("recommendation_engine")
        .getOrCreate()
    )
    sku_name = params.sku_name
    json_file_path = params.json_file
    num = params.num

    df = spark.read.json(json_file_path)
    # engine = RecomendationEngine(sku_name, df, num)
    # engine.get_recommendations().show()
    potential_candidates = get_potential_article_candidates(sku_name, df)
    calculate_recommendations(potential_candidates, recommend_num=num).show(num)

    spark.stop()


# refactor to-do
if __name__ == "__main__":
    # Adding argument to be passed in the terminal
    parser = argparse.ArgumentParser(
        description="Calulations of similar SKUs for given article name within given json file"
    )
    parser.add_argument("--sku_name", help="input product SKU name", type=str)
    parser.add_argument("--json_file", help="input .json file path", type=str)
    parser.add_argument("--num", help="how many products should be found", type=int)

    # print(list(sys.argv))

    # Checking if required keyword arguments were provided as the second parameter argv[1] + third parameter argv[2]
    if len(sys.argv) > 3:
        args = parser.parse_args()
        main(args)
    else:
        print(
            "Program needs to be run with 3 keyword arguments '--sku_name', '--json_file', and '--num'"
        )
