CREATE OR REPLACE TABLE 
-- getting the table ready for analysis / visulization in python and pandas
`rabbithole-analytics.output_tables.for_pandas` AS (
WITH
  addresses AS (
  SELECT
    address
  FROM
  -- this is the csv of wallet addresses provided to me by Trevor
    `rabbithole-analytics.rabbithole_addresses.addresses`),
  -- the following pulls relevant ethereum data about the provided addresses
  eth_txns AS (
  SELECT
    from_address,
    COUNT(`hash`) AS num_txns,
    DATE(MIN(`block_timestamp`)) AS first_txn_date,
    DATE(MAX(`block_timestamp`)) AS last_txn_date,
  FROM
    `bigquery-public-data.crypto_ethereum.transactions`
  WHERE
    from_address IN (
    SELECT
      address
    FROM
      addresses)
  GROUP BY
    from_address),
  -- the following pulls relevant polygon data about the provided addresses
  poly_txns AS (
  SELECT
    from_address,
    COUNT(`hash`) AS num_txns,
    DATE(MIN(`block_timestamp`)) AS first_txn_date,
    DATE(MAX(`block_timestamp`)) AS last_txn_date,
  FROM
    `public-data-finance.crypto_polygon.transactions`
  WHERE
    from_address IN (
    SELECT
      address
    FROM
      addresses)
  GROUP BY
    from_address)
SELECT
  -- all the coalesces are used to handle / replace null values
  a.address,
  COALESCE(eth_txn.num_txns, 0) + COALESCE(poly_txn.num_txns, 0) AS num_txns,
  IF(COALESCE(eth_txn.first_txn_date, '2025-01-01') < COALESCE(poly_txn.first_txn_date, '2025-01-01'), eth_txn.first_txn_date, poly_txn.first_txn_date) AS first_txn_date,
  IF(COALESCE(eth_txn.last_txn_date, '2025-01-01') < COALESCE(poly_txn.last_txn_date, '2025-01-01'), eth_txn.last_txn_date, poly_txn.last_txn_date) AS last_txn_date,
  IF((COALESCE(eth_txn.num_txns, 0) + COALESCE(poly_txn.num_txns, 0)) = 0, NULL, IF(COALESCE(eth_txn.first_txn_date, '2025-01-01') < COALESCE(poly_txn.first_txn_date, '2025-01-01'), 'ethereum', 'polygon')) AS first_chain,
  IF((COALESCE(eth_txn.num_txns, 0) + COALESCE(poly_txn.num_txns, 0)) = 0, NULL, IF(COALESCE(poly_txn.first_txn_date, '2025-01-01') = '2025-01-01', 'ethereum', IF(COALESCE(eth_txn.first_txn_date, '2025-01-01') = '2025-01-01', 'polygon', 'both'))) AS address_chain,
  quest.first_completion_date,
  quest.NFT_1,
  quest.NFT_2,
  quest.NFT_3,
  quest.NFT_4,
  quest.NFT_5,
  quest.NFT_6,
  quest.NFT_7,
  quest.NFT_8,
  quest.NFT_9,
  quest.NFT_10,
  quest.NFT_11,
  quest.NFT_12,
  quest.NFT_13,
  quest.total_nfts
FROM
  addresses AS a
LEFT JOIN
  eth_txns AS eth_txn
ON
  a.address = eth_txn.from_address
LEFT JOIN
    poly_txns AS poly_txn
ON
  a.address = poly_txn.from_address
LEFT JOIN
  `rabbithole-analytics.intermediate_tables.full_quest_completion` AS quest
ON
  a.address = quest.address)



