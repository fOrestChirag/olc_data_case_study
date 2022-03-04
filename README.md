# Case study - OL Data
This repository contains a case study based on the open library data dump
There are 2 tasks in this case study
1. Load the olc dump data json file from S3, clean and transform, and save it in a hive table - **python_script/olc_load.py**
2. Write 6 queries to perform 6 different tasks - **python_script/olc_query.py**

**Assumptions about the data load:**
1. olc_dump file is periodically dropped in a S3 location.
2. There are no updates in subsequent files, only additions are allowed

**Data cleansing and transformation rules:**
1. Title should not be null or empty string.
2. Authors should not be null.
3. Publish date is not null or empty string.
4. Number of pages is not null and greater than 20.
5. Only the books published between 1950 and 2022 are included. 
6. Author is in the first position of the author array and inside key field.
7. Publish date is present in multiple formats, in order to maintain uniformity only publish year is loaded.





