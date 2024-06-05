# SQC

SQL Queries to Collections

# Example

```python
ad_data = [
    {"date": "2023-01-01", "campaign_id": 11, "spend": 100, "clicks": 1000},
    {"date": "2023-01-02", "campaign_id": 11, "spend": 200, "clicks": 1500},
    {"date": "2023-01-02", "campaign_id": 22, "spend": 150, "clicks": 1100},
    {"date": "2023-01-03", "campaign_id": 22, "spend": 50, "clicks": 750},
    {"date": "2023-01-04", "campaign_id": 22, "spend": 300, "clicks": 1050},
]

campaign_data = [
    {"id": 11, "name": "First Campaign"},
    {"id": 22, "name": "Second Campaign"},
]


query = """
SELECT
    ad_data.campaign_id AS `Campaign ID`,
    campaign_data.name AS `Campaign Name`,
    SUM(spend) AS `Spend, $`,
    SUM(clicks) AS `Clicks`
FROM ad_data
JOIN campaign_data
    ON ad_data.campaign_id = campaign_data.id
WHERE spend >= 100
GROUP BY
    ad_data.campaign_id,
    campaign_data.name
"""

import sqc

result = sqc.query(query, {
    "ad_data": ad_data,
    "campaign_data": campaign_data,
})

assert result == [
    {'Campaign ID': 11, 'Campaign Name': 'First Campaign', 'Spend, $': 300, 'Clicks': 2500},
    {'Campaign ID': 22, 'Campaign Name': 'Second Campaign', 'Spend, $': 450, 'Clicks': 2150},
]
```

# Supported features

[] `SELECT`
    - [x] Wildcard, e.g. `SELECT *`
    - [x] Aliases, e.g. `SELECT column AS new_name`
    - [x] Escaping, e.g. `SELECT "Column name with whitespace"`
    - [] `DISTINCT`
[] `WHERE`
    - [x] Comparison (=, <, >, <=, >=, !=)
    - [x] Arithmetic (+, -, *, /, %)
    - [x] `AND`
    - [x] `OR`
    - [x] `NOT`
    - [] `LIKE`
    - [] `IS NULL`
    - [] `IN`
[] `CASE .. WHEN`
[] `ORDER BY`
[] `LIMIT`
[] `OFFSET`
[] `INNER JOIN`
[] `LEFT JOIN`
[] `RIGHT JOIN`
[] `CROSS JOIN`
[] `GROUP BY`
[] `HAVING`
[] `CTE`
[] `UNION`
[] Combining Queries
    - [] `UNION [ALL]`
    - [] `INTERSECT [ALL]`
    - [] `EXCEPT [ALL]`
[] Subqueries
[] Aggregate Functions:
    [x] `COUNT`
    [x] `SUM`
    [] `AVG`
    [] `MIN`
    [] `MAX`
    [] `MEAN`
    [] `WEIGHTED_AVG`
    [] ...
[] Window functions:
    [] `ROW_NUMBER`
    [] `RANK`
    [] ...
[] Query Parameters, e.g. `SELECT * FROM table WHERE date > $1`
[] Nested data source
[] Prepared queries
[] UDF
