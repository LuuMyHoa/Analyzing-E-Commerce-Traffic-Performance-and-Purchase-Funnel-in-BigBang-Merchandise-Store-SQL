# 📊 Analyzing E-Commerce Traffic Performance and Purchase Funnel in BigBang Merchandise Store | SQL

![Banner của tôi](banner.png)

## Table of Contents

* 📌 [Business Problem](#business-problem)
* 📂 [Dataset](#dataset)
* 🔍 [Analysis](#analysis)
* 📝 [Conclusion](#conclusion)

## 📌 Business Problem
This project uses SQL to analyze data from BigBang Merchandise Store to: 

* Measure key E-commerce performance metrics (visits, pageviews, transactions, revenue).

* Evaluate marketing channel effectiveness (traffic sources, bounce rates, revenue contribution).

* Understand customer behavior in the purchase funnel (product view → add to cart → purchase).

* Rank products by revenue and identify frequently co-purchased products.
  
* Provide data-driven insights that support optimization of marketing strategies, website experience, and revenue growth.

## 📂 Dataset
Google BigQuery Public Dataset: ```bigquery-public-data.google_analytics_sample.ga_sessions```

The dataset contains nested and repeated fields (especially at the product level), UNNEST() is used to extract detailed product interactions.

The complete dataset schema can be found [here](https://support.google.com/analytics/answer/3437719?hl=en).

<details>
  <summary>The table summarizes the key fields used in this analysis. </summary>

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

</details>

## 🔍 Analysis
The project answers several key business questions using SQL in Google BigQuery. 

The full SQL analysis can be viewed [here](https://console.cloud.google.com/bigquery?sq=961620975192:4679c420e9cf426f872f57eb6c3a284e).

**1. Total Visits, Pageviews, Transactions per month in Quarter 2 2017.**

```sql
SELECT 
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', date)) AS month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0401' AND '0630'
GROUP BY 1
ORDER BY 1;
```

| month   | visits | pageviews | transactions |
| ------- | ------ | --------- | ------------ |
| 2017-04 | 67126  | 242576    | 959          |
| 2017-05 | 65371  | 255077    | 1160         |
| 2017-06 | 63578  | 233210    | 971          |

👉🏻 Observation:
- Total visits decreased from 67K (Apr) to 63K (Jun) (~5.3% decline)
- Pageviews also declined (~3.9%) in Quarter 2 2017
- May recorded the highest transactions (1,160)
- Conversion rate peaked in May (~1.77%)

**2. Bounce Rate per Traffic Source in June 2017.**

```sql
SELECT 
  trafficSource.source,
  SUM(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_bounces,
  ROUND(SAFE_DIVIDE(SUM(totals.bounces)*100, SUM(totals.visits)), 2) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY 1
ORDER BY total_visits DESC;
```

| source               | total_visits | total_bounces | bounce_rate  |
| -------------------- | ------------ | ------------- | --------------- |
| (direct)             | 37491        | 17440         | 46.52           |
| google               | 17958        | 9497          | 52.88           |
| youtube.com          | 1838         | 1234          | 67.14           |
| analytics.google.com | 1751         | 1011          | 57.74           |
| Partners             | 1313         | 804           | 61.23           |
|------|------|------|------| 

*Top 5 rows of the full results. The complete dataset is available in the repository.*

👉🏻 Observation:
- Direct and Google account for majority of traffic (>85%)
- Bounce rate ranges:
  - Direct: ~46%
  - Google: ~53%
  - YouTube: ~67% (highest among top sources)
- Several minor sources show extremely high bounce rates (>70%)

**3. Revenue by traffic source by month, by  in Quarter 2 2017.**

```sql
WITH product_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    trafficSource.source,
    product.productRevenue / 1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0401' AND '0630'
    AND product.productRevenue IS NOT NULL
)

SELECT
  'Month' AS time_type,
  FORMAT_DATE('%Y-%m', date) AS time,
  source,
  ROUND(SUM(revenue),2) AS revenue
FROM product_data
GROUP BY 1,2,3

UNION ALL

SELECT
  'Quarter' AS time_type,
  CONCAT(CAST(EXTRACT(YEAR FROM date) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS time,
  source,
  ROUND(SUM(revenue),2) AS revenue
FROM product_data
GROUP BY 1,2,3
ORDER BY time_type DESC, time, revenue DESC;
```

| time_type | time    | source            | revenue   |
| --------- | ------- | ----------------- | --------- |
| Quarter   | 2017-Q2 | (direct)          | 293006.39 |
| Quarter   | 2017-Q2 | dfa               | 105445.32 |
| Quarter   | 2017-Q2 | google            | 80031.3   |
|------|------|------|------|
| Month     | 2017-04 | (direct)          | 99301.44  |
| Month     | 2017-04 | dfa               | 93256.75  |
| Month     | 2017-04 | google            | 28960.08  |
|------|------|------|------|

*A summary of the full results. The complete dataset is available in the repository.*

👉🏻 Observation:
- Revenue is heavily concentrated in a few sources: Direct, DFA, and Google contribute the majority
- Social sources (YouTube, Facebook) generate minimal revenue
- Revenue trends are relatively stable across months
  
**4. Average number of pageviews by purchaser type: purchasers vs non-purchasers in Q2 2017.**

```sql
SELECT
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', date)) AS month,
  ROUND(AVG(CASE WHEN totals.transactions >= 1 THEN totals.pageviews END),2) AS avg_pageviews_purchasers,
  ROUND(AVG(CASE WHEN IFNULL(totals.transactions,0) = 0 THEN totals.pageviews END),2) AS avg_pageviews_non_purchasers

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0401' AND '0630'
GROUP BY month
ORDER BY month;
```

| month  | avg_pageviews_purchasers | avg_pageviews_non_purchasers |
| ------ | ------------------------ | ---------------------------- |
| 2017-04 | 24.27                    | 3.32                         |
| 2017-05 | 22.34                    | 3.58                         |
| 2017-06 | 23.89                    | 3.36                         |

👉🏻 Insights: Purchasers browse 6-7x more pages than non-purchasers, indicating purchasers explore the site deeply before buying.

**5. Average revenue per session. Only include purchaser in Q2 2017.**

```sql
WITH session_revenue AS (
  SELECT
    CONCAT(fullVisitorId, CAST(visitId AS STRING)) AS session_key,
    FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(product.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0401' AND '0630'
    AND product.productRevenue IS NOT NULL
  GROUP BY 1,2
)

SELECT
  month,
  ROUND(AVG(revenue),2) AS avg_revenue_per_session
FROM session_revenue
GROUP BY month
ORDER BY month;
```

| month  | avg_revenue_per_session |
| ------ | ----------------------- |
| 2017-04 | 240.19                  |
| 2017-05 | 122.18                  |
| 2017-06 | 135.43                  |

👉🏻 Observation:
- Average revenue per purchasing session dropped significantly from Apr $240 to Jun $135 (~44% decline)
- Despite higher transactions in May, revenue per session is lower

**6. Calculate cohort map from product view to addtocart to purchase in Q2 2017.**

```sql
WITH product_events AS (
  SELECT
    FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', date)) AS month,
    -- action type per product event
    hits.eCommerceAction.action_type AS action_type,
    product.productRevenue AS Revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
  WHERE _TABLE_SUFFIX BETWEEN '0401' AND '0630'
)

SELECT
  month,
  COUNTIF(action_type = '2') AS product_views,
  COUNTIF(action_type = '3') AS add_to_cart,
  COUNTIF(action_type = '6'and Revenue is not null) AS purchases,
  ROUND(SAFE_DIVIDE(COUNTIF(action_type = '3'), COUNTIF(action_type = '2'))*100,2) AS add_to_cart_rate,
  ROUND(SAFE_DIVIDE(COUNTIF(action_type = '6'and Revenue is not null), COUNTIF(action_type = '2'))*100,2) AS purchase_rate
FROM product_events
GROUP BY month
ORDER BY month;
```

| month   | product_views | add_to_cart | purchases | add_to_cart_rate | purchase_rate |
| ------- | ------------- | ----------- | --------- | -------------------- | ----------------- |
| 2017-04| 24587         | 10291       | 2906      | 41.86                | 11.82             |
| 2017-05 | 25469         | 10083       | 3285      | 39.59                | 12.9              |
| 2017-06 | 22148         | 9020        | 2785      | 40.73                | 12.57             |

👉🏻 Observation: 
- Add-to-cart rate remains stable (~40%)
- Purchase rate is significantly lower (~12–13%)
- Large drop-off occurs between add-to-cart and purchase stages

**7. Ranking 5 products by revenue (June 2017)**

```sql
WITH product_revenue AS (
  SELECT
    product.v2ProductName AS product_name,
    SUM(product.productRevenue) AS total_revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
       UNNEST(hits) AS hits,
       UNNEST(hits.product) AS product
  WHERE product.productRevenue IS NOT NULL       
  GROUP BY product_name
),

ranked_products AS (
  SELECT
    product_name,
    total_revenue,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
  FROM product_revenue
)

SELECT *
FROM ranked_products
WHERE revenue_rank <= 5
ORDER BY revenue_rank, total_revenue DESC;
```

| product_name                                | total_revenue  | revenue_rank |
|--------------------------------------------|---------------|--------------|
| Google Bluetooth Headphones                 | 6421026666 | 1            |
| Google 22 oz Water Bottle                   | 5969336021 | 2            |
| Google 17oz Stainless Steel Sport Bottle   | 4454722305 | 3            |
| Google Hard Cover Journal                   | 3870915789 | 4            |
| Sport Bag                                   | 3598704997 | 5            |

👉🏻 Observation:
- Revenue is concentrated in a small number of products
- Top 5 products contribute a disproportionately large share of revenue
  
**8. Other products purchased by customers who purchased product "Google Bluetooth Headphones" in June 2017. Output should show product name and the quantity was ordered.**

```sql
WITH buyer AS (
  SELECT DISTINCT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE product.productRevenue IS NOT NULL
    AND product.v2ProductName = "Google Bluetooth Headphones"
),

other_products AS (
  SELECT
    product.v2ProductName AS other_purchased_products,
    SUM(product.productQuantity) AS quantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  JOIN buyer
    USING (fullVisitorId)
  WHERE product.productRevenue IS NOT NULL
    AND product.v2ProductName != "Google Bluetooth Headphones"
  GROUP BY 1
)

SELECT *
FROM other_products
ORDER BY quantity DESC;
```

| other_purchased_products                     | quantity |
|---------------------------------------------|----------|
| Maze Pen                                     | 200      |
| Google 22 oz Water Bottle                    | 200      |
| Collapsible Shopping Bag                     | 160      |
| 7" Dog Frisbee                               | 48       |
| Google Laptop and Cell Phone Stickers       | 41       |
|---------------------------------------------|----------|

*Top 5 rows of the full results. The complete dataset is available in the repository.*

👉🏻 Observation: Customers who purchase Google Bluetooth Headphones often buy accessories (Bottle, Bag, Stickers), indicating strong cross-sell behavior.

## 📝 Conclusion

### 📍Key Insights

- Declining Traffic Quality: Overall visits and pageviews decreased in Q2. High bounce rates in several traffic sources (especially social channels)

- Conversion Bottleneck at Bottom Funnel: Add-to-cart rate remains stable (~40%), Purchase rate significantly lower (~12%) → indicates friction in checkout stage

- Revenue Concentration Risk: Majority of revenue comes from a few channels (Direct, Google, DFA). Heavy reliance on limited acquisition sources

- High-value Users Show Deep Engagement: Purchasers view 6–7x more pages than non-purchasers → strong correlation between engagement depth and conversion

- Declining Revenue per Session: Revenue per purchasing session dropped ~44% → indicates smaller basket size or lower-value transactions
  
### 🎯 Recommendations

|Target | Priority actions | 
| ------ | ------------------------ |
|Improve Bottom-Funnel Conversion|Simplify and optimize the checkout process (fewer steps, guest checkout, UX clarity)|
|Optimize Traffic Quality|Re-evaluate low-performing traffic sources (high bounce). Improve landing page relevance|
|Reduce Channel Dependency|Diversify acquisition channels beyond Direct & Google. Invest in high-converting channels|
|Increase Average revenue per purchase session|Introduce bundling and “frequently bought together”. Set free shipping thresholds|
|Enhance Product Discovery|Help users reach relevant products faster, promote top-performing products strategically|
