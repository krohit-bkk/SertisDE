with sorted_data AS (
  SELECT custId, transactionDate FROM sample_data GROUP BY custId, transactionDate
),
ranked_data AS (
  SELECT
    custId,
    transactionDate,
    ROW_NUMBER() OVER (PARTITION BY custId ORDER BY transactionDate) as rnk,
    ROW_NUMBER() OVER (PARTITION BY custId ORDER BY transactionDate) - transactionDate AS group_transactionDate
  FROM sorted_data
)

SELECT 
  custId,
  MAX(grp_cnt) AS max_occurrance
FROM (
  SELECT 
    custId,
    group_transactionDate,
    COUNT(group_transactionDate) AS grp_cnt
  FROM ranked_data
  GROUP BY 
    custId, 
    group_transactionDate
) x
GROUP BY 
  custId;


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
