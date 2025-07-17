# Quack Cluster API Tutorial

Welcome to the Quack Cluster API! This document provides a guide on how to send queries and understand the scope of supported SQL operations.

---

## 1. How to Query the API

Querying the service is straightforward. You send an HTTP **POST** request with your SQL query in a JSON payload to the `/query` endpoint.

### Endpoint

`POST /query`

### Request Body

The request body must be a JSON object containing a single key, `sql`, whose value is the SQL query you want to execute.

```json
{
  "sql": "SELECT * FROM your_table WHERE condition = 'value';"
}
````

### Response Formats (Optional)

You can specify the response format using the `format` query parameter.

  * `?format=json` (Default): Returns results as a standard JSON array.
  * `?format=arrow`: Returns results as a binary Apache Arrow stream, which is ideal for high-performance clients and data science applications.

### Example with cURL

Here is a complete example of how to query for all data from the `users` table and receive a JSON response.

```bash
curl -X POST "[http://127.0.0.1:8000/query?format=json](http://127.0.0.1:8000/query?format=json)" \
-H "Content-Type: application/json" \
-d '{
    "sql": "SELECT name, city FROM \"users\" ORDER BY name;"
}'
```

-----

## 2\. Query Scope & SQL Capabilities

The API leverages the **DuckDB SQL dialect**. You can query collections of data files (like Parquet or CSV) by referring to them as tables. The system supports glob patterns to query multiple files at once (e.g., `"data_part_*.parquet"`).

Below is a summary of supported SQL features, confirmed through testing.

### ✅ Basic Operations

  * **`SELECT`**: Select all (`*`) or specific columns.
  * **`FROM`**: Specify tables, including file glob patterns (e.g., `"data_part_*"`).
  * **`WHERE`**: Filter rows based on conditions.
  * **`GROUP BY`**: Group rows for aggregate functions.
  * **`ORDER BY`**: Sort the result set.
  * **`LIMIT` / `OFFSET`**: Paginate results.

### ✅ Aggregations & Grouping

  * **Aggregate Functions**: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`.
  * **`HAVING`**: Filter groups after aggregation.

### ✅ Joins & Set Operations

  * **`INNER JOIN`**: Select matching records from two tables.
  * **`LEFT JOIN`**: Select all records from the left table and matched records from the right.
  * **`FULL OUTER JOIN`**: Select all records when there is a match in either the left or right table.
  * **`UNION ALL`**: Combine the result sets of two or more `SELECT` statements (including duplicates).

### ✅ Advanced SQL Features

  * **Subqueries**: Nest a `SELECT` statement inside another statement (e.g., in `FROM` or `WHERE IN (...)`).
  * **Common Table Expressions (CTEs)**: Use the `WITH` clause to define temporary, named result sets.
  * **Window Functions**: Perform calculations across a set of table rows (e.g., `SUM(...) OVER (PARTITION BY ...)`).
  * **Conditional Logic**: Use `CASE...WHEN...THEN...ELSE...END` statements.
  * **Advanced `SELECT` Syntax**:
      * `SELECT DISTINCT`: Return only unique values.
      * `SELECT DISTINCT ON (...)`: Return the first row for each unique group.
      * `SELECT * EXCLUDE (...)`: Select all columns except specified ones.
      * `SELECT * REPLACE (...)`: Replace a column's value with a new expression.
      * `SELECT COLUMNS('<regex>')`: Select columns that match a regular expression.
  * **Date/Time Functions**: Functions like `DATE_TRUNC` to manipulate date/time values.

-----

## 3\. Error Handling

The API will return standard HTTP status codes to indicate the outcome of a request.

  * **`400 Bad Request`**: Your SQL query has a syntax error. The response body will contain details.
  * **`404 Not Found`**: A table or file specified in your query does not exist.
  * **`200 OK`**: The query was successful. An empty result set is still a success.

