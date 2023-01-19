import logger
import argparse
import sys
from typing import Dict
from pyspark.sql.functions import col, desc, sum, udf, size
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

# refactoring done
def extract_item_attributes(item_sku: str, df: DataFrame) -> Dict[str, str]:
    attributes = df.filter(df.sku == item_sku).select("attributes").first().attributes
    return attributes.asDict()


def attributes_matcher_wrapper(attributes):
    def attributes_matcher_UDF(row):
        row_to_dict = row.asDict()
        matching_attributes = {
            key: value
            for key, value in row_to_dict.items()
            if key in attributes and attributes[key] == value
        }

        return sorted(matching_attributes.keys())

    return udf(attributes_matcher_UDF, returnType=ArrayType(StringType()))


def calculate_potential_candidates(sku: str, items: DataFrame) -> DataFrame:
    attributes_of_given_item = extract_item_attributes(sku, items)

    candidates_for_recommendation = (
        items.filter(items.sku != sku)
        .withColumn(
            "matching_attributes",
            attributes_matcher_wrapper(attributes_of_given_item)(col("attributes")),
        )
        .withColumn("matching_count", size(col("matching_attributes")))
    )
    return candidates_for_recommendation


def cal_amount_of_matching_attributes(candidates: DataFrame) -> DataFrame:
    window = Window.orderBy(desc(col("matching_count"))).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    candidate_statistics = (
        candidates.groupBy(col("matching_count"))
        .count()
        .withColumn("running_total", sum("count").over(window))
        .select(col("matching_count").alias("amount"), col("running_total"))
    )

    candidate_statistics.show()

    return candidate_statistics


def get_reccomendations(candidates: DataFrame, recommend_num: int) -> DataFrame:
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
        f"Select {number_of_additional_items_needed} additional items "
        + f"with {count_where_select_is_needed} matching attributes based on given sorting criterion."
    )

    # for amount = 5, where DataFrame (5,2)
    filtering_value = select_all_items_until_count["amount"]
    # print(filtering_value)
    itemsWithoutSelection = candidates.filter(col("matching_count") >= filtering_value)
    # itemsWithoutSelection.show()

    if number_of_additional_items_needed > 0:
        additionalItems = (
            candidates.filter(col("matching_count") == count_where_select_is_needed)
            .orderBy(col("matching_attributes"))
            .limit(number_of_additional_items_needed)
        )
        unsortedRecommendations = itemsWithoutSelection.union(additionalItems)
    else:
        unsortedRecommendations = itemsWithoutSelection

    # unsortedRecommendations.show()

    return unsortedRecommendations.orderBy(
        desc(col("matching_count")), col("matching_attributes")
    )


def main(params):

    sku_name = params.sku_name
    json_file_path = params.json_file
    num = params.num
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("recommendation_engine")
        .getOrCreate()
    )

    df = spark.read.json(json_file_path)
    potential_candidates = calculate_potential_candidates(sku_name, df)
    get_reccomendations(potential_candidates, recommend_num=num).show(num)


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
