-- Longest streak for Customer_Id
-- ==============================
WITH transaction1 AS (
  SELECT DISTINCT
    "custId",
    CAST("transactionDate" AS DATE) AS "transactionDate",
    CAST('1990-01-01' AS DATE) AS "referenceDate",
    CAST("transactionDate" AS DATE) - CAST('1990-01-01' AS DATE) AS "DateDifference"
  FROM 
    "transaction"
),
date_diff_group AS (
  SELECT
    *,
    "DateDifference" - ROW_NUMBER() OVER(PARTITION BY "custId" ORDER BY "DateDifference") AS "DateDifferenceGroup"
  FROM "transaction1"
  ORDER BY "custId"
),
days_streaks AS (
  SELECT 
    "custId",
    "DateDifferenceGroup",
    COUNT(1) AS "streak"
  FROM "date_diff_group"
  GROUP BY 
    "custId",
    "DateDifferenceGroup"
),
fun_analysis AS (
  SELECT 
    "custId",
    MAX("streak") AS "longest_streak"
  FROM
    "days_streaks"
  GROUP BY
    "custId"
)
SELECT * FROM "fun_analysis" 
WHERE "custId" = 23938; 


-- Favorite Product for Customer_Id
-- ================================
WITH ranked_products AS (
  SELECT
    trans_id,
    custId,
    trans_date,
    productSold,
    unitsSold,
    SUM(unitsSold) OVER (PARTITION BY custId, productSold) AS total_cust_prod_sold
  FROM transactions
),
cust_prod_rank AS (
SELECT 
  custId AS customer_id,
  productSold,
  total_cust_prod_sold,
  DENSE_RANK() OVER(PARTITION BY custId ORDER BY total_cust_prod_sold DESC) as rnk
FROM 
  ranked_products
)
SELECT
  custId, productSold AS favourite_product
FROM 
  cust_prod_rank 
WHERE 
  rnk = 1;
