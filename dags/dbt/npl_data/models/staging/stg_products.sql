-- 4️⃣ Modelo: Tabela de Stage para Normalizar Nomes de Categorias
-- 📌 Objetivo: Criar uma tabela de stage para tratar nomes de categorias (removendo underscores e colocando em maiúsculas).

WITH stage_products AS (
    SELECT 
        product_id,
        UPPER(REPLACE(product_category_name, '_', ' ')) AS product_category_name_clean,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM {{source('dbt-bigquery-452812-q7', 'olist_products_dataset') }}
)

SELECT * FROM stage_products
