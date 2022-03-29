from pyspark.sql.functions import avg, desc, countDistinct, explode, row_number, rank, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import olc_load


def data_query(final_df):
    """
    Executes the query based on the user input by query number.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    spark = SparkSession.builder.master("local").appName("olc_dump").enableHiveSupport().getOrCreate()
    print("Please enter the query number you want to run (1-6)")
    print("1 - All Harry Potter books")  # done
    print("2 - The book with the most pages")  # done
    print("3 - Top 5 authors with most written books")  # done
    print("4 - Top 5 genres with most books")
    print("5 - The avg. number of pages")  # done
    print("6 - Per publish year, get the number of authors that published at least one book")
    query_number = int(input())
    if query_number == 1:
        print("All Harry Potter in the dump are: ")
        harry_potter_books(final_df)
    elif query_number == 2:
        print("The book with most number pages is: ")
        book_with_most_pages(final_df, spark)
    elif query_number == 3:
        print("Top 5 authors with most written books are: ")
        top_five_authors(final_df)
    elif query_number == 4:
        print("Top 5 genres with most books are: ")
        top_five_genres(final_df)
    elif query_number == 5:
        print("The average number of pages in the data set are: ")
        avg_number_pages(final_df)
    elif query_number == 6:
        print("Number of authors that published at least one book (for each publish year): ")
        num_authors_per_publish_year(final_df)
    else:
        print("You have entered an invalid query ID, please enter a value between 1-6")


def harry_potter_books(final_df):
    """
    Finds all the Harry Potter books from the OL data dump.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    final_df.filter("lower(title) like '%harry potter%'").show(100, truncate=False)
    data_query(final_df)


def book_with_most_pages(final_df, spark):
    """
    Finds the book with most number of pages from the OL data dump.
    :param spark: Spark Session
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    window_spec = Window.orderBy(final_df.number_of_pages.desc())
    final_df.withColumn("rank", row_number().over(window_spec)).filter("rank=1").\
        select("title", "number_of_pages").show(truncate=False)

    # Alternate method
    # Used spark sql here because in order to use dataframe for this type of subquery
    # we will need collect the value first and then use in the filter
    final_df.createOrReplaceTempView("final_temp_table")
    spark.sql("select title, number_of_pages from final_temp_table where number_of_pages = "
              "(select max(number_of_pages) from final_temp_table)").show(truncate=False)
    data_query(final_df)


def top_five_authors(final_df):
    """
    Finds the top 5 authors with most number of books written.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    top_5_author = final_df.groupBy("author_id").count().alias("count")
    window_spec = Window.orderBy(top_5_author['count'].desc())
    top_5_author.withColumn("rank", dense_rank().over(window_spec)).filter("rank < 6").drop("rank").show()

    #Alternate Method
    final_df.groupBy("author_id").count().sort(desc("count")).limit(5).show()
    data_query(final_df)


def top_five_genres(final_df):
    """
    Finds the top 5 genres with most number of books.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    top_5_genres = final_df.withColumn("genre", explode(final_df.genres)).groupBy("genre").count().alias("count")
    window_spec = Window.orderBy(top_5_genres['count'].desc())
    top_5_genres.withColumn("rank", dense_rank().over(window_spec)).filter("rank < 6").drop("rank").show()\

    # Alternate Method
    final_df.withColumn("genre", explode(final_df.genres)).groupBy("genre").count().sort(desc("count")).limit(5).show(
        truncate=False)
    data_query(final_df)


def avg_number_pages(final_df):
    """
    Finds the average number of pages from the complete OL data dump.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    final_df.agg(avg("number_of_pages").cast(IntegerType()).alias("average_pages")).show()
    data_query(final_df)


def num_authors_per_publish_year(final_df):
    """
    Finds the number of authors that published at least one book, per publish year.
    :param final_df: Final spark dataframe with clean records and transformed columns.
    :return:
    """
    final_df.groupBy("publish_year").agg(countDistinct("author_id")).sort(desc("publish_year")).show(100, truncate=False)
    data_query(final_df)
