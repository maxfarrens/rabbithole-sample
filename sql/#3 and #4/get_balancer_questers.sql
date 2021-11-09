CREATE OR REPLACE TABLE
  `rabbithole-analytics.intermediate_tables.balancer_quest_completion` AS (
  SELECT
    address
  FROM
    `rabbithole-analytics.intermediate_tables.quest_completion`
  WHERE
    quest_8 != 0)