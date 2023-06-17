WITH top_three_product_categories AS (
  SELECT
    products.product_category_name,
    COUNT(DISTINCT items.order_item_id) AS order_items
  FROM take_home_challenge.ecommerce.orders AS orders
  LEFT JOIN take_home_challenge.ecommerce.order_items AS items ON items.order_id = orders.order_id
  LEFT JOIN take_home_challenge.ecommerce.products AS products ON items.product_id = products.product_id
  WHERE TO_CHAR(orders.order_purchase_timestamp, 'YYYY-MM') = '2017-11'
  GROUP BY products.product_category_name
  ORDER BY order_items DESC
  LIMIT 3
),

top_three_weekly_sales AS (
  SELECT
    WEEK(orders.order_purchase_timestamp) AS week,
    products.product_category_name,
    SUM(items.price) AS weekly_gmv
  FROM take_home_challenge.ecommerce.orders AS orders
  LEFT JOIN take_home_challenge.ecommerce.order_items AS items ON items.order_id = orders.order_id
  LEFT JOIN take_home_challenge.ecommerce.products AS products ON items.product_id = products.product_id
  WHERE
    YEAR(orders.order_purchase_timestamp) = '2017'
    AND products.product_category_name IN (SELECT product_category_name FROM top_three_product_categories)
  GROUP BY week, products.product_category_name
)

SELECT
  week,
  product_category_name,
  weekly_gmv,
  SUM(weekly_gmv)
    OVER (PARTITION BY product_category_name ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS running_weekly_gmv,
  DIV0(
    weekly_gmv - LAG(weekly_gmv, 1, 0) OVER (PARTITION BY product_category_name ORDER BY week),
    LAG(weekly_gmv, 1, 0) OVER (PARTITION BY product_category_name ORDER BY week)
  ) AS weekly_gmv_growth_rate
FROM top_three_weekly_sales
ORDER BY product_category_name, week
