-- Frequency Recency Monetary (FRM) Query____________________________________________________________________________
-- Accompanying visualisations in Tableau:
-- https://public.tableau.com/app/profile/emilis.janeliauskas/viz/FRMAnalysis_17439537448890/Dashboard1

WITH
fm_compute AS (
  SELECT
    CustomerID,
    Country,
    CAST(MAX(InvoiceDate) AS DATE) AS last_purchase_date,
    COUNT(DISTINCT InvoiceNo) AS frequency,
    CAST(SUM(Quantity * UnitPrice) AS INT) AS monetary
  FROM
    `tc-da-1.turing_data_analytics.rfm`
  WHERE CustomerID IS NOT NULL
    AND CAST(InvoiceDate AS DATE) BETWEEN CAST("2010-12-01" AS DATE) AND CAST("2011-12-01" AS DATE)
  GROUP BY 1,2
  ORDER BY 5 DESC
  ),
r_compute AS (
  SELECT 
    *,
    DATE_DIFF(CAST("2011-12-01" AS DATE), last_purchase_date, DAY) AS recency
  FROM fm_compute
  ),
percentiles_calc AS (
  SELECT 
      a.*,
      --All percentiles for MONETARY
      b.percentiles[offset(25)] AS m25, 
      b.percentiles[offset(50)] AS m50,
      b.percentiles[offset(75)] AS m75, 
      --- look into 100 percentiel
      b.percentiles[offset(100)] AS m100,   
      --All percentiles for FREQUENCY
      c.percentiles[offset(25)] AS f25, 
      c.percentiles[offset(50)] AS f50,
      c.percentiles[offset(75)] AS f75, 
      c.percentiles[offset(100)] AS f100,    
      --All percentiles for RECENCY
      d.percentiles[offset(25)] AS r25, 
      d.percentiles[offset(50)] AS r50,
      d.percentiles[offset(75)] AS r75, 
      d.percentiles[offset(100)] AS r100, 
  FROM 
      r_compute a,
      (SELECT APPROX_QUANTILES(monetary, 100) percentiles FROM r_compute) b,
      (SELECT APPROX_QUANTILES(frequency, 100) percentiles FROM r_compute) c,
      (SELECT APPROX_QUANTILES(recency, 100) percentiles FROM r_compute) d
  )
  ,
assign_scores AS (
  SELECT 
    *, 
    CAST(ROUND((f_score + m_score) / 2, 0) AS INT64) AS fm_score,
    CONCAT(r_score,f_score,m_score) AS rfm_score
    FROM (
      SELECT *, 
      CASE 
        WHEN monetary <= m25 THEN 1
        WHEN monetary <= m50 AND monetary > m25 THEN 2 
        WHEN monetary <= m75 AND monetary > m50 THEN 3 
        WHEN monetary <= m100 AND monetary > m75 THEN 4 
      END AS m_score,
      CASE 
        WHEN frequency <= f25 THEN 1
        WHEN frequency <= f50 AND frequency > f25 THEN 2 
        WHEN frequency <= f75 AND frequency > f50 THEN 3 
        WHEN frequency <= f100 AND frequency > f75 THEN 4 
      END AS f_score,
      --Recency scoring is reversed
      CASE 
        WHEN recency <= r25 THEN 4
        WHEN recency <= r50 AND recency > r25 THEN 3 
        WHEN recency <= r75 AND recency > r50 THEN 2 
        WHEN recency <= r100 AND recency > r75 THEN 1 
      END AS r_score,
      FROM percentiles_calc
      )
    )

SELECT 
  CustomerID, 
  Country,
  recency,
  frequency, 
  monetary,
  r_score,
  f_score,
  m_score,
  fm_score,
  rfm_score,
  CASE
  -- Best Customers (Champions): top recency & top fm_score
  WHEN r_score = 4 AND fm_score = 4 THEN 'Champions (Best Customers)'

  -- Loyal Customers: high but not top in both dimensions
  WHEN (r_score = 3 AND fm_score = 4)
    OR (r_score = 4 AND fm_score = 3)
  THEN 'Loyal Customers'

  -- Potential Loyalists: high recency, moderate fm_score
  WHEN (r_score = 4 AND fm_score = 2)
    OR (r_score = 3 AND fm_score = 3)
  THEN 'Potential Loyalists'

  -- Recent Customers: very recent, but low fm_score
  WHEN r_score = 4 AND fm_score = 1 THEN 'Recent Customers'

  -- Promising: moderate recency, low fm_score
  WHEN (r_score = 3 AND fm_score = 1)
  THEN 'Promising'

  -- Customers Needing Attention: mid value customer
  WHEN (r_score = 3 AND fm_score = 2)
    OR (r_score = 2 AND fm_score = 2)
    OR (r_score = 2 AND fm_score = 3)
  THEN 'Customers Needing Attention'

  -- About to Sleep: low recency, even lower fm_score
  WHEN r_score = 2 AND fm_score = 1 THEN 'About to Sleep'

  -- At Risk: low recency, but high fm_score
  WHEN (r_score = 1 AND fm_score = 3) THEN 'At Risk'

  -- Can’t Lose Them: lowest recency, highest fm_score
  WHEN (r_score = 2 AND fm_score = 4)
    OR (r_score = 1 AND fm_score = 4)  
  THEN "Can't Lose Them (Big Spenders)"

  -- Hibernating: low recency, low-mid fm_score
  WHEN r_score = 1 AND fm_score = 2 THEN 'Hibernating'

  -- Lost: lowest of the low
  WHEN r_score = 1 AND fm_score = 1 THEN 'Lost'
  END AS customer_segment
FROM assign_scores


-- Customer Lifetime Value (CVL) Query ____________________________________________________________________________
-- Accompanying visualisations in Google Sheets:
-- https://docs.google.com/spreadsheets/d/1qmKsoeNdBymfCmL97lHq0NImrrlUW5PhgUCnKdzsXXc/edit?gid=1327073488#gid=1327073488

WITH registration_cohort AS (
  SELECT
    user_pseudo_id,
    DATE_TRUNC(MIN(PARSE_DATE("%Y%m%d", event_date)), WEEK(SUNDAY)) AS registration_week
  FROM tc-da-1.turing_data_analytics.raw_events
  WHERE 
    PARSE_DATE("%Y%m%d", event_date) <= '2021-01-30'
  GROUP BY 1
), 
revenue_data AS (
  SELECT
    user_pseudo_id,
    DATE_TRUNC(PARSE_DATE("%Y%m%d", event_date), WEEK(SUNDAY)) AS purchase_week,
    purchase_revenue_in_usd
  FROM tc-da-1.turing_data_analytics.raw_events
  WHERE
    PARSE_DATE("%Y%m%d", event_date) <= '2021-01-30'
    AND purchase_revenue_in_usd > 0
),
combined_data AS (
  SELECT
    registration_cohort.user_pseudo_id AS user_id,
    registration_cohort.registration_week  AS registration_week,
    revenue_data.purchase_week AS purchase_week,
    revenue_data.purchase_revenue_in_usd AS purchase_revenue,
  FROM registration_cohort
  LEFT JOIN revenue_data
  ON registration_cohort.user_pseudo_id = revenue_data.user_pseudo_id
  ORDER BY 1 DESC
)

SELECT
  registration_week AS cohort,
  COUNT(DISTINCT user_id) AS unique_users,
  SUM(CASE WHEN purchase_week = registration_week THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_0,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 1 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_1,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 2 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_2,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 3 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_3,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 4 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_4,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 5 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_5,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 6 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_6,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 7 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_7,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 8 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_8,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 9 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_9,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 10 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_10,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 11 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_11,
  SUM(CASE WHEN purchase_week = DATE_ADD(registration_week, INTERVAL 12 WEEK) THEN purchase_revenue END) / COUNT(DISTINCT user_id) AS week_12
FROM combined_data
GROUP BY 1
ORDER BY 1