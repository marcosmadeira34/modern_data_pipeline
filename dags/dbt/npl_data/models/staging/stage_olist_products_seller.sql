with products_sellers  as (
    select * from {{ ref('olist_products_seller') }}
)

select *, COALESCE(product_name_lenght, 0) < 60 AS maiority
from dbtconjuntodedados.olist_products_dataset 