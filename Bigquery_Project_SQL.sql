-- Project for SQL


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT format_date("%Y%m", parse_date("%Y%m%d",date)) as month, sum(totals.visits) as visits, 
sum(totals.pageviews) as pageviews, sum(totals.transactions) as transactions, sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE  _table_suffix between '0101' and '0331'
GROUP BY month
ORDER BY month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT trafficSource.source as source, count(totals.visits) as total_visit, count(totals.bounces) as num_bounce, 
(count(totals.bounces)/count(totals.visits))*100 as bounce_rate 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE  _table_suffix between '0701' and '0731'
GROUP BY trafficSource.source
ORDER BY total_visit DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017

WITH month as (
SELECT "Month" as time_type,trafficSource.source as source, format_date("%Y%m", parse_date("%Y%m%d", date)) as time,
sum(totals.totalTransactionRevenue/1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source, time
ORDER BY revenue DESC),

week as (
SELECT "Week" as time_type,trafficSource.source as source, format_date("%Y%W", parse_date("%Y%m%d", date)) as time,
sum(totals.totalTransactionRevenue/1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source, time
ORDER BY revenue DESC)

SELECT * FROM month
UNION ALL
SELECT * FROM week
ORDER BY source

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

WITH purchaser_data as(
  SELECT
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  GROUP BY month),

non_purchaser_data as(
  SELECT
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix between '0601' and '0731'
  and totals.transactions is null
  GROUP BY month)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
LEFT JOIN non_purchaser_data USING(month)
ORDER BY pd.month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions>=1
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT format_date("%Y%m", parse_date("%Y%m%d", date)) as month, 
    sum(totals.totalTransactionRevenue)/sum(totals.visits) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions is not null
GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

WITH buyer_list as (
SELECT DISTINCT fullvisitorid as user_id, product.v2ProductName 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
unnest(hits) as hits,
unnest(product) as product
WHERE v2ProductName = "YouTube Men's Vintage Henley" and product.productRevenue is not null),

other_purchased_products as (
SELECT fullvisitorid as user_id, product.v2ProductName as other_purchased_products, product.productQuantity as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
unnest(hits) as hits,
unnest(product) as product
WHERE product.productRevenue is not null)

SELECT o.other_purchased_products, sum(o.quantity) as total_quantity
FROM other_purchased_products as o
INNER JOIN buyer_list on o.user_id = buyer_list.user_id
WHERE o.other_purchased_products not in ("YouTube Men's Vintage Henley")
GROUP BY o.other_purchased_products

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

WITH product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
GROUP BY 1)

SELECT
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
FROM product_view pv
JOIN add_to_cart a on pv.month = a.month
JOIN purchase p on pv.month = p.month
ORDER BY pv.month

