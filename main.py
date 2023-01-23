import logger
import argparse
import sys
from pyspark.sql import SparkSession
from recommendation_engine import RecommendationEngine


def main(params):
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

    engine = RecommendationEngine(sku_name, df, num, spark)
    engine.get_recommendations().show()

    spark.stop()


if __name__ == "__main__":
    # Adding argument to be passed in the terminal
    parser = argparse.ArgumentParser(
        description="Calulations of similar SKUs for given article name within given json file"
    )
    parser.add_argument("--sku_name", help="input product SKU name", type=str)
    parser.add_argument("--json_file", help="input .json file path", type=str)
    parser.add_argument(
        "--num", help="how many recommendations for article should be found", type=int
    )

    # Checking if required keyword arguments were provided as the second parameter argv[1] + third parameter argv[2]
    if len(sys.argv) > 3:
        args = parser.parse_args()
        main(args)
    else:
        logger.logger.error(
            "Program needs to be run with 3 keyword arguments '--sku_name', '--json_file', and '--num'"
        )
