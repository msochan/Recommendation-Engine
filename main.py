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
def extract_article_attributes(
    article_sku: str, df: DataFrame, spark_session: SparkSession
) -> Dict[str, str]:
    try:
        attributes = (
            df.filter(df.sku == article_sku).select("attributes").first().attributes
        )
    except AttributeError:
        logger.logger.error(
            f"Article with SKU= {article_sku} not found in the input .json file!"
        )
        spark_session.stop()
        sys.exit()
    else:
        return attributes.asDict()


# Returning DataFrame of articles that will be use for further recommendations excluding sku from input article
def get_potential_articles_for_recommendation(
    article_sku: str, df: DataFrame, spark_session: SparkSession
) -> DataFrame:
    input_article_attributes = extract_article_attributes(
        article_sku, df, spark_session
    )

    df_articles = (
        df.filter(df.sku != article_sku)
        .withColumn(
            "attribute_matches",
            attributes_matcher_wrapper(input_article_attributes)(col("attributes")),
        )
        .withColumn("num_of_matches", size(col("attribute_matches")))
        .select(["sku", "attributes", "attribute_matches", "num_of_matches"])
    )

    df_articles.show()
    return df_articles


# refactor to-do - mozna zrobic z tego dwie funkcje zeby bylo przejrzysciej
def calculate_recommendations(candidates: DataFrame, recommend_num: int) -> DataFrame:
    matching_counts = get_matching_statistics(candidates)

    select_all_items_until_count = None
    for match in matching_counts.orderBy(desc(col("num_of_matches"))).collect():
        if match.running_total <= recommend_num:
            select_all_items_until_count = match
    # print(select_all_items_until_count)

    number_of_additional_items_needed = 0
    if select_all_items_until_count:
        number_of_additional_items_needed = (
            min(recommend_num, candidates.count())
            - select_all_items_until_count.running_total
        )

    # print(f"CHUJ {number_of_additional_items_needed}")

    count_where_select_is_needed = 0
    for match in matching_counts.orderBy((col("num_of_matches"))).collect():
        if match.running_total > recommend_num:
            count_where_select_is_needed = match.num_of_matches
    # print(f"KUTAS {count_where_select_is_needed}")

    logger.logger.info(
        f"Select {number_of_additional_items_needed} additional articles "
        + f"with {count_where_select_is_needed} matching attributes based on given sorting criterion."
    )

    ######## ponizej poczatek drugiej funkcji

    # for num_of_matches = 5, where DataFrame (5,2)
    filtering_value = select_all_items_until_count["num_of_matches"]
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

    # eventually in a situation when there is the same num_of_matches of matches within multiple rows
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


# Returning DataFrame with following columns: number of matches, count and running_total for matching attributes
def get_matching_statistics(df_articles: DataFrame) -> DataFrame:
    window = Window.orderBy(desc(col("num_of_matches"))).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    df_stats = (
        df_articles.groupBy(col("num_of_matches"))
        .count()
        .withColumn("running_total", sum("count").over(window))
    )

    df_stats.show()

    return df_stats


def main(params):
    # local[2] for 2 cores
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("recommendation_engine")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    sku_name = params.sku_name
    json_file_path = params.json_file
    num = params.num

    df = spark.read.json(json_file_path)
    # engine = RecomendationEngine(sku_name, df, num)
    # engine.get_recommendations().show()
    potential_candidates = get_potential_articles_for_recommendation(
        sku_name, df, spark
    )
    calculate_recommendations(potential_candidates, recommend_num=num).show(num)

    spark.stop()


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
        logger.logger.error(
            "Program needs to be run with 3 keyword arguments '--sku_name', '--json_file', and '--num'"
        )
