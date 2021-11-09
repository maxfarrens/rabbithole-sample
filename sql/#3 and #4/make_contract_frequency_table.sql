CREATE OR REPLACE TABLE
  `rabbithole-analytics.intermediate_tables.contract_counts` AS (
  SELECT
    to_address AS contract,
    COUNT(`hash`) AS num_count
  FROM
    `public-data-finance.crypto_polygon.transactions`
  WHERE
    DATE(block_timestamp) BETWEEN '2021-08-25'
    AND '2021-09-10'
    AND from_address IN(
    SELECT
      address
    FROM
      `rabbithole-analytics.intermediate_tables.balancer_quest_completion`)
  GROUP BY
    to_address
  ORDER BY
    num_count DESC)