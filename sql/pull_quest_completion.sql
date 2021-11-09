CREATE OR REPLACE TABLE
  `rabbithole-analytics.intermediate_tables.quest_completion` AS (
  WITH
    rabbithole_addresses AS (
    SELECT
      address
    FROM
    -- this is the csv of wallet addresses provided to me by Trevor
      `rabbithole-analytics.rabbithole_addresses.addresses`),
    nft_transfers AS (
    SELECT
    -- the following concatenations and splits are just a hack to remove a bunch of 0s from
    -- the byte form of the recipient address contained in the topics of a txn log and allow
    -- the address to be usable in other queries
      CONCAT(SPLIT(topics[
      OFFSET
          (3)],'000000000000000000000000')[
      OFFSET
        (0)], SPLIT(topics[
        OFFSET
          (3)],'000000000000000000000000')[
      OFFSET
        (1)]) AS recip_address,
      DATE(block_timestamp) AS txn_date,
    -- the following concatenations and splits are just a hack to translate the txn log data
    -- (which contains the ID of the RabbitHole NFT transferred) from bytecode to hex and finally
    -- into human-readable integer form
      CAST(CONCAT('0x000', SPLIT(`data`, '000000000000000000000000000000000000000000000000000000000000000')[
        OFFSET
          (1)]) AS INT64) AS quest_id
    FROM
      `public-data-finance.crypto_polygon.logs`
    WHERE
      CONCAT(SPLIT(topics[
        OFFSET
          (3)],'000000000000000000000000')[
      OFFSET
        (0)], SPLIT(topics[
        OFFSET
          (3)],'000000000000000000000000')[
      OFFSET
        (1)]) IN(
      SELECT
        address
      FROM
        rabbithole_addresses)
      -- the follwing is the contract address for RabbitHoleNFT on Polygon
      AND address = '0xf9481510210c13be887d200fbb03036088638d79'
      -- the following is the bytecode form of the wallet address that always calls
      -- the RabbitHoleNFT on Polygon contract to initiate NFT transfer upon quest completion
      AND topics[
    OFFSET
      (2)] = '0x000000000000000000000000b53bff904fac39383fe3c420be8bc3dde26ad40f')
  -- the following does a one-hot encoding for each quest based on whether an address has
  -- completed it or not. The result is a long-form table that has one has one wallet address per
  -- row, a 1 in the column for the quest that that wallet completed, and the corresponding
  -- completion date. Because this is long form, if one wallet has completed multiple quests
  -- that address will pop up in multiple rows, with one row per quest completed by that address.
  -- These rows will be grouped by address in another query
  SELECT
    nft.recip_address AS address,
  IF
    (nft.quest_id = 1,
      1,
      0) AS quest_1,
  IF
    (nft.quest_id = 2,
      1,
      0) AS quest_2,
  IF
    (nft.quest_id = 3,
      1,
      0) AS quest_3,
  IF
    (nft.quest_id = 4,
      1,
      0) AS quest_4,
  IF
    (nft.quest_id = 5,
      1,
      0) AS quest_5,
  IF
    (nft.quest_id = 6,
      1,
      0) AS quest_6,
  IF
    (nft.quest_id = 7,
      1,
      0) AS quest_7,
  IF
    (nft.quest_id = 8,
      1,
      0) AS quest_8,
  IF
    (nft.quest_id = 9,
      1,
      0) AS quest_9,
  IF
    (nft.quest_id = 10,
      1,
      0) AS quest_10,
  IF
    (nft.quest_id = 11,
      1,
      0) AS quest_11,
  IF
    (nft.quest_id = 12,
      1,
      0) AS quest_12,
  IF
    (nft.quest_id = 13,
      1,
      0) AS quest_13,
    nft.txn_date AS nft_transfer_date
  FROM
    nft_transfers AS nft)