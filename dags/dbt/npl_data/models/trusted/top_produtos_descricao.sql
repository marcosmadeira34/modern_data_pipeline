WITH produtos AS (
    SELECT 
        product_id,
        product_category_name,
        product_description_lenght
    FROM {{ source('dbt-bigquery-452812-q7', 'olist_products_dataset') }}
    ORDER BY product_description_lenght DESC
    LIMIT 5
)

SELECT * FROM produtos
