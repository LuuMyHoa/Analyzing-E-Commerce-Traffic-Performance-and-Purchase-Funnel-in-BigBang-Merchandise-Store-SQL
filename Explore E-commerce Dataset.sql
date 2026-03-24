/* 
PROJECT: Explore E-commerce Dataset
8 questions 
*/
--q1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT 
  format_date('%Y%m',parse_date('%Y%m%d', date)) month
  ,count(totals.visits) visits
  ,sum(totals.pageviews) pageviews
  ,sum(totals.transactions) transactions 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0101' and '0331'   --Jan, Feb and March 2017
group by 1
order by 1;                                     --order by month

--q2: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT 
  trafficSource.source source
  ,count(totals.visits) totals_visits
  ,sum(totals.bounces) total_no_of_bounces
  ,round(sum(totals.bounces)*100.0/count(totals.visits),3) bounce_rate  --Bounce_rate = num_bounce/total_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` --month:07
group by 1
order by 2 DESC;                                                        --order by total_visit DESC

--q3: Revenue by traffic source by week, by month in June 2017
with 
month_data as(
  SELECT
    "Month" time_type,
    format_date("%Y%m", parse_date("%Y%m%d", date)) time,
    trafficSource.source source,
    sum(p.productRevenue)/1000000 revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`, -- month:06 
    UNNEST (hits) hits,
    UNNEST (hits.product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
),

week_data as(
  SELECT
    "Week" time_type,
    format_date("%Y%W", parse_date("%Y%m%d", date)) time,
    trafficSource.source source,
    sum(p.productRevenue)/1000000 revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`, -- month:06 
    UNNEST (hits) hits,
    UNNEST (hits.product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
)

select * from month_data
union all
select * from week_data
order by time_type, time, revenue DESC;

--q4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
/*
step1: tính avg_pageviews_purchasers theo từng tháng
step2: tính avg_pageviews_non_purchasers theo từng tháng
step3: mapping theo tháng
*/
with 
pageviews_purchasers as(  --tính avg_pageviews_purchasers 
  SELECT 
    format_date('%Y%m', parse_date('%Y%m%d', date)) month
    ,count(distinct fullVisitorId) cnt_visitor
    ,sum(totals.pageviews) sum_pageviews_purchasers 
    ,round(sum(totals.pageviews)/count(distinct fullVisitorId),7) avg_pageviews_purchasers
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  where _table_suffix between '0601' and '0731'           --lấy dữ liệu tháng 6,7
    and totals.transactions >=1             --điều kiện 1 purchaser: totals.transactions >=1
    and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
  group by month
),

pageviews_non_purchasers as(  --tính avg_pageviews_non_purchasers
  SELECT 
    format_date('%Y%m', parse_date('%Y%m%d', date)) month
    ,count(distinct fullVisitorId) cnt_visitor
    ,sum(totals.pageviews) sum_pageviews_non_purchasers 
    ,round(sum(totals.pageviews)/count(distinct fullVisitorId),7) avg_pageviews_non_purchasers
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  where _table_suffix between '0601' and '0731'           --lấy dữ liệu tháng 6,7
    and totals.transactions is null      --điều kiện 1 non-purchaser: totals.transactions is null
    and product.productRevenue is null   --điều kiện 2 non-purchaser: productRevenue is  null
  group by month
)

select 
  p.month
  ,avg_pageviews_purchasers
  ,avg_pageviews_non_purchasers
from pageviews_purchasers p
full join pageviews_non_purchasers n
  on p.month=n.month
order by 1;

--q5: Average number of transactions per user that made a purchase in July 2017
/*
month: 7
avg_total_transactions_per_user = total transactions/ total user
purchaser: "totals.transactions >=1" and "product.productRevenue is not null"
*/
SELECT 
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  round(sum(totals.transactions)/count(distinct fullvisitorid),2) as avg_total_transactions_per_user
FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where  totals.transactions>=1              --điều kiện 1 purchaser: totals.transactions >=1
  and product.productRevenue is not null   --điều kiện 2 purchaser: productRevenue is not null
group by month;

--q6: Average amount of money spent per session. Only include purchaser data in July 2017
/*
month: 7
avg_spend_per_session = total revenue/ total visit
purchaser: "totals.transactions IS NOT NULL" and "product.productRevenue is not null"
To shorten the result, productRevenue should be divided by 1000000
*/
SELECT 
  format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
  round(sum(product.productRevenue)/power(10,6)/count(totals.visits),2) avg_spend_per_session 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, --month:07
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where totals.transactions is not null     --điều kiện 1 purchaser: totals.transactions is not null  
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
group by 1;

--q7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
/*
step1: tìm list buyer 
thỏa điều kiện: purchaser + đã mua product "YouTube Men's Vintage Henley"

step2: join list buyer 
select productname, sum(quantity) group by productname
where thoả: điều kiện purchaser, other product
order by quantity desc

purchaser: "totals.transactions >=1" and "product.productRevenue is not null"
*/
with buyer_list as(   -- tìm list buyer 
SELECT 
  distinct fullVisitorId
  ,product.v2ProductName 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, --lấy dữ liệu 7
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where totals.transactions >=1             --điều kiện 1 purchaser: totals.transactions >=1  
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
  and product.v2ProductName = "YouTube Men's Vintage Henley" --chuỗi có 'nên dùng" thay thế
)

SELECT 
  product.v2ProductName other_purchased_products
  ,sum(product.productQuantity) quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, --lấy dữ liệu 7
  UNNEST (hits) hits,
  UNNEST (hits.product) product
join buyer_list using(fullVisitorId)
where 1=1 -- để dễ kiểm tra các điều kiện bằng cách cmt/
  and totals.transactions >=1             --điều kiện 1 purchaser: totals.transactions >=1  
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
  and product.v2ProductName != "YouTube Men's Vintage Henley" --other product
group by 1
order by 2 desc;

--q8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.Add_to_cart_rate = number product add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
/*
Hint 1: hits.eCommerceAction.action_type = '2' is view product page; hits.eCommerceAction.action_type = '3' is add to cart; hits.eCommerceAction.action_type = '6' is purchase
Hint 2: Add condition "product.productRevenue is not null"  for purchase to calculate correctly
Hint 3: To access action_type, you only need unnest hits
*/
with product_data as(
SELECT 
  format_date('%Y%m',parse_date('%Y%m%d', date)) month
  ,count(case when ecommerceaction.action_type = '2' then product.v2ProductName end) num_product_view
  ,count(case when ecommerceaction.action_type = '3' then product.v2ProductName end) num_addtocart
  ,count(case when ecommerceaction.action_type = '6' and product.productRevenue is not null then product.v2ProductName  
          end) num_purchase       --thêm điều kiện "product.productRevenue is not null" 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where _table_suffix between '0101' and '0331'        -- lấy dữ liệu tháng 1, 2, 3
group by 1
order by 1
)

select 
  *
  ,round(num_addtocart*100.0/num_product_view,2) add_to_cart_rate --Add_to_cart_rate = number product add to cart/number product view.
  ,round(num_purchase*100.0/num_product_view,2) purchase_rate     --Purchase_rate = number product purchase/number product view.
from product_data
order by 1;

