CREATE EXTERNAL TABLE IF NOT EXISTS my_db.OLC_TABLE
(
    title String,
    genres Array<String>,
    copyright_date String,
    languages Array<String>,
    latest_revision Int,
    url Array<String>,
    number_of_pages Int,
    publish_country String,
    series Array<String>,
    publishers Array<String>,
    isbn_10 Array<String>,
    isbn_13 Array<String>,
    author_id String,
    publish_year Int,
)
row format delimited
fields terminated by ','
stored as Parquet ;