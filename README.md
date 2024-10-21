## Authors:
* Angad Brar, zqq4hx
* Varun Togaru, wbz9mn

## One Pager

### Reflection on Building a Data Processing Utility

Building this data processing utility brought several challenges. The biggest challenge was JSON-to-SQL conversion. JSON data, especially from APIs like GitHub, often includes nested structures like dictionaries and lists. When trying to insert this data into SQL, errors like "Error binding parameter: type 'list' is not supported" occurred. To fix this, we had to flatten the nested fields, converting them into JSON strings so they could be stored in SQL without issues.

SQL conversion was tricky overall. SQL requires a rigid relational format, meaning nested fields and unsupported data types from JSON had to be flattened or transformed before insertion. This required adjustments to both how we modified and stored the data. Without flattening, errors such as "trailing data" would appear, making the process more complex than expected.

Fetching data from URLs and files was straightforward once the error handling was in place. The use of requests for pulling data and standard file reading for local data worked without issues. Detecting the format—CSV or JSON—was also simple thanks to the built-in functions provided by pandas and json.

The SQL conversion process turned out to be the most difficult. SQL requires a strict structure, unlike flexible formats like JSON. Nested fields had to be flattened, and handling inconsistent data types became a challenge during SQL insertion, where each column must follow specific rules. These issues didn't surface as much with CSV but were critical for SQL.

This utility will be helpful in future data projects. Automating the process of fetching, cleaning, and storing data makes handling large datasets easier. The ability to convert between JSON, CSV, and SQL while modifying columns as needed adds flexibility that would save time in many data-driven tasks. It could streamline ETL processes, making data ready for analysis without manually handling format conversions.

The project highlighted the complexities of converting JSON to SQL and flattening data structures. While this was the most challenging part, it also provided the most value in learning how to handle diverse data types. Despite these challenges, the utility proved to be a solid tool for automating data preparation tasks, which will be valuable in future work.