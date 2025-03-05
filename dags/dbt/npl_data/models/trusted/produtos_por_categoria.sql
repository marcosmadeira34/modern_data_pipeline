WITH produtos AS (
    SELECT 
        product_category_name,
        COUNT(*) AS total_produtos,
        ROUND(AVG(product_weight_g), 2) AS peso_medio,
        ROUND(AVG(product_length_cm), 2) AS comprimento_medio,
        ROUND(AVG(product_height_cm), 2) AS altura_media,
        ROUND(AVG(product_width_cm), 2) AS largura_media
    FROM {{ source('dbt-bigquery-452812-q7', 'olist_products_dataset') }}
    GROUP BY product_category_name
)

SELECT * FROM produtos
ORDER BY total_produtos DESC
