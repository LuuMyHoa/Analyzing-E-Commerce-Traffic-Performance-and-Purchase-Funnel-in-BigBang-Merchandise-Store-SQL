# Explore E-commerce Dataset

## Table of Contents

* [Business Problem](#business-problem)
* [Dataset](#dataset)
* [Analysis](#analysis)
* [Insights](#insights)
* [Recommendations](#recommendations)

## Business Problem
This project analyzes Google Analytics e-commerce session data to evaluate website performance and customer behavior. 

The analysis aims to: 

* Measure key e-commerce performance metrics such as visits, pageviews, transactions, and revenue.

* Evaluate marketing channel effectiveness by analyzing traffic sources and bounce rates.

* Understand customer behavior in the purchase funnel (product view → add to cart → purchase).

* Provide data-driven insights that support optimization of marketing strategies, website experience, and revenue growth.

## Dataset
Google BigQuery Public Dataset: ```bigquery-public-data.google_analytics_sample.ga_sessions```

Some fields are nested and repeated, therefore UNNEST() is used in SQL queries to extract product-level information. 

The table below summarizes the key fields used in this analysis. The complete dataset schema can be found [hear](https://support.google.com/analytics/answer/3437719?hl=en).

| Field | Type | Description | 
|------|------|-------------| 
| fullVisitorId | STRING | The unique visitor ID. | 
| date | STRING | The date of the session in YYYYMMDD format. | 
| totals | RECORD | This section contains aggregate values across the session. |
| totals.bounces | INTEGER | Total bounces (for convenience). For a bounced session, the value is 1, otherwise it is null. |
| totals.hits | INTEGER | Total number of hits within the session. | 
| totals.pageviews | INTEGER | Total number of pageviews within the session. |
| totals.visits | INTEGER | The number of sessions (for convenience). This value is 1 for sessions with interaction events. The value is null if there are no interaction events in the session. |
| totals.transactions | INTEGER | Total number of ecommerce transactions within the session. | 
| trafficSource.source | STRING | The source of the traffic source. Could be the name of the search engine, the referring hostname, or a value of the utm_source URL parameter. | 
| hits | RECORD | This row and nested fields are populated for any and all types of hits. |
| hits.eCommerceAction | RECORD | This section contains all of the ecommerce hits that occurred during the session. This is a repeated field and has an entry for each hit that was collected. | 
| hits.eCommerceAction.action_type | STRING | The action type. Click through of product lists = 1, Product detail views = 2, Add product(s) to cart = 3, Remove product(s) from cart = 4, Check out = 5, Completed purchase = 6, Refund of purchase = 7, Checkout options = 8, Unknown = 0. | 
| hits.product | RECORD | This row and nested fields will be populated for each hit that contains Enhanced Ecommerce PRODUCT data. | 
| hits.product.productQuantity | INTEGER | The quantity of the product purchased. | 
| hits.product.productRevenue | INTEGER | The revenue of the product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000). |
| hits.product.productSKU | STRING | Product SKU. | 
| hits.product.v2ProductName | STRING | Product Name. |

## Analysis
The project answers several key business questions using SQL in Google BigQuery. 

The full SQL analysis can be viewed [here](https://console.cloud.google.com/bigquery?sq=961620975192:4679c420e9cf426f872f57eb6c3a284e).

**1. Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month).**

```sql
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

| month | visits | pageviews | transactions | 
|------|------|------|------| 
| 201701 | 64694 | 257708 | 713 |
| 201702 | 62192 | 233373 | 733 | 
| 201703 | 69931 | 259522 | 993 |

**2. Bounce rate per traffic source in July 2017 (order by total_visit DESC).**

```sql
SELECT 
  trafficSource.source source
  ,count(totals.visits) totals_visits
  ,sum(totals.bounces) total_no_of_bounces
  ,round(sum(totals.bounces)*100.0/count(totals.visits),3) bounce_rate  --Bounce_rate = num_bounce/total_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` --month:07
group by 1
order by 2 DESC;                                                        --order by total_visit DESC
```

| source | totals_visits | total_no_of_bounces | bounce_rate | 
|------|------|------|------| 
| google | 38400 | 19798 | 51.557 | 
| (direct) | 19891 | 8606 | 43.266 | 
| youtube.com | 6351 | 4238 | 66.73 |
| analytics.google.com | 1972 | 1064 | 53.955 |

**3. Revenue by traffic source by week, by month in June 2017.**

```sql
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
```

| time_type | time | source | revenue | 
|------|------|------|------|
| Month | 201706 | (direct) | 97333.619695 |
| Month | 201706 | google | 18757.17992 | 
| Month | 201706 | dfa | 8862.229996 | 
|------|------|------|------| 
| Week | 201722 | (direct) | 6888.899975 | 
| Week | 201722 | google | 2119.38999 | 
| Week | 201722 | dfa | 1670.649998 | 
|------|------|------|------|

**4. Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.**

```sql
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
```

| month | avg_pageviews_purchasers | avg_pageviews_non_purchasers | 
|------|------|------| 
| 201706 | 94.0205011 | 316.8655885 | 
| 201707 | 124.2375519 | 334.0565598 |

**5. Average number of transactions per user that made a purchase in July 2017.**

```sql
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
```

| month | avg_total_transactions_per_user | 
|------|------| 
| 201707 | 4.16 |

**6. Average amount of money spent per session. Only include purchaser data in July 2017.**

```sql
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
```

| month | avg_spend_per_session | 
|------|------| 
| 201707 | 43.86 |

**7. Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.**

```sql
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
```

| other_purchased_products | quantity |
|------|------| 
| Google Sunglasses | 20 |
| Google Women's Vintage Hero Tee Black | 7 | 
| SPF-15 Slim & Slender Lip Balm | 6 | 
| Google Women's Short Sleeve Hero Tee Red Heather | 4 |

**8. Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. The output should be calculated in product level.**

```sql
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
```

| month | num_product_view | num_addtocart | num_purchase | add_to_cart_rate | purchase_rate | 
|------|------|------|------|------|------| 
| 201701 | 25787 | 7342 | 2143 | 28.47 | 8.31 | 
| 201702 | 21489 | 7360 | 2060 | 34.25 | 9.59 | 
| 201703 | 23549 | 8782 | 2977 | 37.29 | 12.64 |

## Insights

* Website performance improved in Q1 2017. Visits increased from 64,694 (Jan) to 69,931 (Mar) (+8.1%), while transactions grew from 713 to 993 (+39%). The much faster growth in transactions suggests an improvement in conversion performance or marketing effectiveness.

* Organic search is the primary traffic driver. Google generates the highest number of visits (38,400), highlighting SEO as the main acquisition channel. However, its higher bounce rate (51.6%) compared to direct traffic (43.3%) suggests that users arriving directly are more engaged and likely more familiar with the brand.

* Direct traffic generates the majority of revenue. In June 2017, direct traffic generated ~97,333 revenue, far exceeding Google (~18,757) and other channels. This indicates that direct visitors likely represent returning customers with stronger purchase intent.

* Purchasers navigate the website more efficiently. Purchasers viewed 94-124 pages on average, while non-purchasers viewed 316-334 pages. This suggests buyers find products more quickly, whereas non-buyers may struggle with product discovery or lack purchase intent.

* The purchase funnel became more efficient across Q1 2017. The add-to-cart rate increased from 28.47% to 37.29%, while the purchase rate rose from 8.31% to 12.64%. This indicates users were increasingly likely to progress from product view to purchase.

## Recommendations

*  Optimize SEO landing pages. Since organic search drives a large portion of traffic, improving landing page relevance, page speed, and product navigation can increase engagement and reduce bounce rates.

*  Strengthen customer retention strategies. Returning visitors tend to show stronger purchase intent. Implementing email marketing, loyalty programs, and personalized offers can encourage repeat purchases.

* Implement product recommendation strategies. Customers frequently purchase multiple items in a session. Features like “Frequently Bought Together” can increase cross-selling and average order value.

* Improve product discovery on the website. Enhancing search functionality, product categorization, and filtering options can help users find relevant products faster and reduce browsing friction.

* Continue optimizing the purchase funnel. Simplifying checkout steps, improving product page information, and testing promotional incentives can further improve conversion rates.
