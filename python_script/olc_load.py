import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, to_date, year
import logging
import olc_query


def read_olc_dump():
    """
    Reads the OL data dump from S3, perform data cleansing and projects specific columns.
    :return: Spark dataframe with clean OL data records.
    """
    olc_dump_df = spark.read.json("C:\\Users\\Chirag_Singhal\\Downloads\\ol_cdump.json")
    cleansing_filters = ["title is not null and title != ''",
                         "authors is not null",
                         "publish_date is not null and publish_date != ''",
                         "number_of_pages is not null and number_of_pages > 20"
                         ]
    filter_separator = " and "
    projected_columns = ["title", "genres", "authors.key as authors", "copyright_date",
                         "languages.key as languages", "latest_revision", "url", "number_of_pages", "publish_date",
                         "publish_country", "series", "publishers", "isbn_10", "isbn_13"]

    olc_clean_df = olc_dump_df.filter(filter_separator.join(cleansing_filters)).selectExpr(projected_columns)
    return olc_clean_df


def transform_olc_dump(source_olc_df):
    """
    Transforms author and publishing year columns and add to the source dataframe.
    :param source_olc_df: Spark dataframe with clean OL data records
    :return:
    """
    author_transformed_df = source_olc_df.withColumn("author", source_olc_df.authors.getItem(0)).drop("authors")
    publish_year_transformed_df = author_transformed_df.withColumn("publish_year",
                                                                   publish_year(author_transformed_df.publish_date)). \
        drop("publish_date").filter("publish_year between 1950 and 2022")
    return publish_year_transformed_df


def publish_year(col, formats=("MMMM dd, yyyy", "yyyy", "MMMM yyyy", "MMMM d, yyyy")):
    """
    Creates a new publish year column by transforming multiple date formats to year
    :param col: Input column
    :param formats: Date formats to transform
    :return: New column for publishing year
    """
    return coalesce(*[year(to_date(col, f)) for f in formats])


def save_df_to_table(final_df):
    """
    Saves the final spark dataframe into hive table
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    final_df.write().mode("overwrite").saveAsTable(target_table)


if __name__ == '__main__':
    target_table = "my_db.OLC_TABLE"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    spark = SparkSession.builder.master("local").appName("olc_dump").enableHiveSupport().getOrCreate()
    start_time = time.time()
    logger.info("Data Load Started at: {} ".format(start_time))

    olc_df = read_olc_dump()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    transformed_olc_df = transform_olc_dump(olc_df)
    save_df_to_table(transformed_olc_df)
    end_time = time.time()

    logger.info("Data Load completed at: {} ".format(end_time))
    logger.info("Total time taken to load the data: {} seconds".format(end_time-start_time))
    # olc_query.data_query(transformed_olc_df)

