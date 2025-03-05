WITH peso_medio AS (
    SELECT ROUND(AVG(product_weight_g), 2) AS peso_medio_geral
    FROM {{ source('dbt-bigquery-452812-q7', 'olist_products_dataset') }}
),

produtos_pesados AS (
    SELECT 
        p.product_id,
        p.product_category_name,
        p.product_weight_g,
        pm.peso_medio_geral
    FROM {{ source('dbt-bigquery-452812-q7', 'olist_products_dataset') }} p
    CROSS JOIN peso_medio pm
    WHERE p.product_weight_g > pm.peso_medio_geral
)

SELECT * FROM produtos_pesados

