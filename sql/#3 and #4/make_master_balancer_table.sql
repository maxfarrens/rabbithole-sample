CREATE OR REPLACE TABLE
  `rabbithole-analytics.intermediate_tables.balancer_interactions` AS (
  WITH
    before AS(
    SELECT
      from_address AS address,
      COUNT(`hash`) AS num_interactions,
      MIN(DATE(block_timestamp)) AS first_interaction
    FROM
      `public-data-finance.crypto_polygon.transactions`
    WHERE
      DATE(block_timestamp) < '2021-08-26'
      AND to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
      AND from_address IN(
      SELECT
        address
      FROM
        `rabbithole-analytics.intermediate_tables.balancer_quest_completion`)
    GROUP BY
      from_address),
    after AS (
    SELECT
      from_address AS address,
      COUNT(`hash`) AS num_interactions,
      MAX(DATE(block_timestamp)) AS last_interaction
    FROM
      `public-data-finance.crypto_polygon.transactions`
    WHERE
      DATE(block_timestamp) > '2021-09-09'
      AND to_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
      AND from_address IN(
      SELECT
        address
      FROM
        `rabbithole-analytics.intermediate_tables.balancer_quest_completion`)
    GROUP BY
      from_address)
  SELECT
    questors.address AS address,
    b.num_interactions AS balancer_txns_before,
    a.num_interactions AS balancer_txns_after,
    b.first_interaction AS first_balancer_txn,
    a.last_interaction AS last_balancer_txn
  FROM
    `rabbithole-analytics.intermediate_tables.balancer_quest_completion` AS questors
  LEFT OUTER JOIN
    before AS b
  ON
    questors.address = b.address
  LEFT OUTER JOIN
    after AS a
  ON
    questors.address = a.address
  ORDER BY
    balancer_txns_after DESC)