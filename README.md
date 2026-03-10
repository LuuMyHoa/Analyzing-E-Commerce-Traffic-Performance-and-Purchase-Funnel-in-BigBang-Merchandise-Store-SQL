# Explore E-commerce Dataset

## Table of Contents

* [Business Objectives](#business-objectives)
* [Dataset](#dataset)
* [SQL Analysis](#sql-analysis)
* [Insights](#insights)
* [Recommendations](#recommendations)

## Business Objectives
This project analyzes Google Analytics e-commerce session data to evaluate website performance and customer behavior.

The analysis aims to:

* Measure key e-commerce performance metrics such as visits, pageviews, transactions, and revenue.

* Evaluate marketing channel effectiveness by analyzing traffic sources and bounce rates.

* Understand customer behavior in the purchase funnel (product view → add to cart → purchase).

* Provide data-driven insights that support optimization of marketing strategies, website experience, and revenue growth.

## Dataset
Google BigQuery Public Dataset: ```bigquery-public-data.google_analytics_sample.ga_sessions```

Some fields are nested and repeated, therefore UNNEST() is used in SQL queries to extract product-level information.

The table below summarizes the key fields used in this analysis.

| Field | Description |
|------|-------------|
| fullVisitorId | Unique visitor identifier |
| date | Session date |
| totals.visits | Number of sessions |
| totals.pageviews | Number of pageviews |
| totals.transactions | Number of transactions |

## SQL Analysis
The project answers several key business questions using SQL in Google BigQuery.

**1. Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month).**

```
SELECT 
  format_date('%Y%m',parse_date('%Y%m%d', date)) month
  ,count(totals.visits) visits
  ,sum(totals.pageviews) pageviews
  ,sum(totals.transactions) transactions 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
where _table_suffix between '0101' and '0331'   --Jan, Feb and March 2017
group by 1
order by 1;                                     --order by month
```

**2. Bounce rate per traffic source in July 2017 (order by total_visit DESC).**

```
SELECT 
  trafficSource.source source
  ,count(totals.visits) totals_visits
  ,sum(totals.bounces) total_no_of_bounces
  ,round(sum(totals.bounces)*100.0/count(totals.visits),3) bounce_rate  --Bounce_rate = num_bounce/total_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` --month:07
group by 1
order by 2 DESC;                                                        --order by total_visit DESC
```

**3. Revenue by traffic source by week, by month in June 2017.**

```
with revenue_week as(
  SELECT 
    format_date('%Y%m', parse_date('%Y%m%d', date)) month
    ,format_date('%Y%W', parse_date('%Y%m%d', date)) week
    ,trafficSource.source source
    ,product.productRevenue revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`, -- month:06 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
)

select 
  'Week'time_type
  ,week
  ,source 
  ,round(sum(revenue)/1000000,4) revenue
from revenue_week
group by 1,2,3
having revenue is not null

union all

select 
  'Month'time_type
  ,month
  ,source 
  ,round(sum(revenue)/1000000,4) revenue
from revenue_week
group by 1,2,3
having revenue is not null
order by 3,1,2;
```

**4. Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.**

```
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
inner join pageviews_non_purchasers n
  on p.month=n.month
order by 1;
```

**5. Average number of transactions per user that made a purchase in July 2017.**

```
/*
month: 7
Avg_total_transactions_per_user = total transactions/ total user
purchaser: "totals.transactions >=1" and "product.productRevenue is not null"
*/
with call_mon7 as(
SELECT 
  sum(totals.transactions) total_trans
  ,count(distinct fullVisitorId) total_user   -- fullVisitorId field is user id
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, --month:07
UNNEST (hits) hits,
UNNEST (hits.product) product
where totals.transactions >=1             --điều kiện 1 purchaser: totals.transactions >=1
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
)

select
  '201707' Month
  ,(total_trans/total_user) Avg_total_transactions_per_user 
from call_mon7;
```

**6. Average amount of money spent per session. Only include purchaser data in July 2017.**

```
/*
month: 7
avg_spend_per_session = total revenue/ total visit
purchaser: "totals.transactions IS NOT NULL" and "product.productRevenue is not null"
To shorten the result, productRevenue should be divided by 1000000
*/
with call_mon7 as(
SELECT 
  count(totals.visits) total_visit
  ,sum(product.productRevenue)/1000000 total_revenue   
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, --month:07
UNNEST (hits) hits,
UNNEST (hits.product) product
where totals.transactions is not null     --điều kiện 1 purchaser: totals.transactions is not null  
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
)

select
  '201707' Month
  ,round(total_revenue /total_visit,2) avg_spend_per_session 
from call_mon7;
```

**7. Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.**

```
/*
step1: tìm list user 
thỏa điều kiện: purchaser + đã mua product "YouTube Men's Vintage Henley"

step2: productname, sum(quantity) group by productname
where thoả: điều kiện purchaser, nằm trong list user, other product
order by quantity desc

purchaser: "totals.transactions >=1" and "product.productRevenue is not null"
*/
with call_users as(   -- tìm list user
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
where 1=1 -- để dễ kiểm tra các điều kiện bằng cách cmt/
  and totals.transactions >=1             --điều kiện 1 purchaser: totals.transactions >=1  
  and product.productRevenue is not null  --điều kiện 2 purchaser: productRevenue is not null
  and product.v2ProductName != "YouTube Men's Vintage Henley" --other product
  and fullVisitorId in (select fullVisitorId from call_users) -- nằm trong list user
group by 1
order by 2 desc,1;
```

**8. Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. The output should be calculated in product level.**

```
/*
Hint 1: hits.eCommerceAction.action_type = '2' is view product page; hits.eCommerceAction.action_type = '3' is add to cart; hits.eCommerceAction.action_type = '6' is purchase
Hint 2: Add condition "product.productRevenue is not null"  for purchase to calculate correctly
Hint 3: To access action_type, you only need unnest hits
*/
with raw_data as(
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
  month
  ,num_product_view
  ,num_addtocart
  ,num_purchase
  ,round(num_addtocart*100.0/num_product_view,2) add_to_cart_rate --Add_to_cart_rate = number product add to cart/number product view.
  ,round(num_purchase*100.0/num_product_view,2) purchase_rate     --Purchase_rate = number product purchase/number product view.
from raw_data
order by 1;
```
## Insights
* Certain traffic sources generate high traffic but high bounce rates, indicating low engagement quality.

* Purchasers tend to have significantly higher pageviews, suggesting deeper browsing behavior before purchase.

* The conversion funnel reveals drop-offs between product view and add-to-cart, indicating potential UX or pricing issues.

* Some products are frequently purchased together, revealing cross-selling opportunities.

## Recommendations
* Improve marketing channel efficiency: Focus investment on traffic sources with lower bounce rates and higher conversion performance.

* Optimize product pages: Improve product descriptions, images, and pricing to increase add-to-cart rates.

* Implement cross-selling strategies: Recommend related products during checkout to increase average order value.

* Enhance website engagement: Improve navigation and product discovery to encourage more pageviews and deeper browsing.

