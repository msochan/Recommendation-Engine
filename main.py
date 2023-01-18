import logger
import argparse
import sys
from pyspark.sql.functions import col, desc, sum, udf, size
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.window import Window


def get_attributes(sku, df):
    extracted_row = df.filter(df.sku == sku).select("attributes").collect()[0][0]
    return extracted_row.asDict()


def match_attributes_helper(example):
    def match_attributes_udf(row):
        parsed_row = row.asDict()
        matching_attributes = {
            key: value
            for key, value in parsed_row.items()
            if key in example and example[key] == value
        }

        return sorted(matching_attributes.keys())

    return udf(match_attributes_udf, returnType=ArrayType(StringType(), False))


def calculate_potential_candidates(sku, items, recommend_num):
    attributes_of_given_item = get_attributes(sku, items)
    match_attributes_UDF = match_attributes_helper(attributes_of_given_item)

    candidates_for_recommendation = (
        items.filter(items.sku != sku)
        .withColumn("matching_attributes", match_attributes_UDF(col("attributes")))
        .withColumn("matching_count", size(col("matching_attributes")))
        .sort(desc(col("matching_count")))
    )

    get_reccomendations(candidates_for_recommendation, recommend_num).show()


def cal_amount_of_matching_attributes(candidates):
    window = Window.orderBy(desc(col("matching_count"))).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    candidate_statistics = (
        candidates.groupBy(col("matching_count"))
        .count()
        .withColumn("cumsum", sum("count").over(window))
        .select(col("matching_count").alias("count"), col("cumsum"))
    )
    candidate_statistics.show()
    return candidate_statistics


def get_reccomendations(candidates, recommend_num):
    matchung_counts = cal_amount_of_matching_attributes(candidates)
    matchung_counts.show()

    # DF count 5,2 cumsum
    selectAllItemsUntilCount = matchung_counts.orderBy(desc(col("count"))).filter(
        col("cumsum") <= recommend_num
    )
    print(type(selectAllItemsUntilCount))

    numberOfAdditionalItemsNeeded = (
        min(recommend_num, candidates.count())
        - selectAllItemsUntilCount.collect()[0]["cumsum"]
        or 0
    )

    print(min(recommend_num, candidates.count()))
    print(selectAllItemsUntilCount.collect()[0]["cumsum"])
    print(numberOfAdditionalItemsNeeded)

    countWhereSelectionIsNeeded = (
        matchung_counts.orderBy(desc(col("count"))).filter(
            col("cumsum") > recommend_num
        )
    ).collect()[0]["count"] or max(matchung_counts.orderBy(desc(col("count"))))[0][
        "count"
    ]

    print(countWhereSelectionIsNeeded)

    logger.logger.info(
        f"Select {numberOfAdditionalItemsNeeded} additional items "
        + f"with {countWhereSelectionIsNeeded} matching attributes based on given sorting criterion."
    )

    # for count = 5, where DataFrame (5,2)
    filtering_value = selectAllItemsUntilCount.collect()[0]["count"]
    itemsWithoutSelection = candidates.filter(col("matching_count") >= filtering_value)
    # itemsWithoutSelection.show()

    if numberOfAdditionalItemsNeeded > 0:
        additionalItems = (
            candidates.filter(col("matching_count") == countWhereSelectionIsNeeded)
            .orderBy(col("matching_attributes"))
            .limit(numberOfAdditionalItemsNeeded)
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
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("recommendation_engine")
        .getOrCreate()
    )

    df = spark.read.json(json_file_path)
    calculate_potential_candidates(sku_name, df, recommend_num=10)


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
