WITH orders_per_seller AS (
  SELECT
    DAY(order_purchase_timestamp) AS purchase_day,
    WEEK(order_purchase_timestamp) AS purchase_week,
    TO_CHAR(order_purchase_timestamp, 'YYYY-MM') AS purchase_month,
    items.seller_id,
    COUNT(DISTINCT orders.order_id) AS seller_daily_orders,
    SUM(seller_daily_orders) OVER (PARTITION BY seller_id, purchase_month) AS seller_monthly_orders,
    SUM(seller_daily_orders) OVER (PARTITION BY seller_id, purchase_week) AS seller_weekly_orders
  FROM take_home_challenge.ecommerce.orders AS orders
  LEFT JOIN take_home_challenge.ecommerce.order_items AS items ON items.order_id = orders.order_id
  WHERE purchase_month LIKE '2017%'
  GROUP BY purchase_day, purchase_week, purchase_month, items.seller_id
),

daily_active_sellers AS (
  SELECT
    purchase_month,
    purchase_day,
    COUNT(DISTINCT CASE WHEN seller_daily_orders >= 1 THEN seller_id END) AS daily_active_sellers
  FROM orders_per_seller
  GROUP BY purchase_month, purchase_day
),

weekly_active_sellers AS (
  SELECT
    purchase_month,
    purchase_week,
    COUNT(DISTINCT CASE WHEN seller_weekly_orders >= 5 THEN seller_id END) AS weekly_active_sellers
  FROM orders_per_seller
  GROUP BY purchase_month, purchase_week
)

SELECT
  monthly.purchase_month,
  COUNT(DISTINCT CASE WHEN seller_monthly_orders >= 25 THEN seller_id END) AS monthly_active_sellers,
  TO_DECIMAL(AVG(weekly_active_sellers), 18, 2) AS avg_weekly_active_sellers,
  TO_DECIMAL(AVG(daily_active_sellers), 18, 2) AS avg_daily_active_sellers
FROM
  orders_per_seller AS monthly
LEFT JOIN weekly_active_sellers AS weekly ON weekly.purchase_month = monthly.purchase_month
LEFT JOIN daily_active_sellers AS daily ON daily.purchase_month = monthly.purchase_month
GROUP BY monthly.purchase_month
ORDER BY monthly.purchase_month
