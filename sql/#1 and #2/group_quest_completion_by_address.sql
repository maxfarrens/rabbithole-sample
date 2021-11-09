CREATE OR REPLACE TABLE
`rabbithole-analytics.intermediate_tables.full_quest_completion` AS (
  WITH
    quest_complete AS (
    SELECT
      address as nft_address,
      MIN(nft_transfer_date) AS first_completion_date,
      SUM(quest_1) AS NFT_1,
      SUM(quest_2) AS NFT_2,
      SUM(quest_3) AS NFT_3,
      SUM(quest_4) AS NFT_4,
      SUM(quest_5) AS NFT_5,
      SUM(quest_6) AS NFT_6,
      SUM(quest_7) AS NFT_7,
      SUM(quest_8) AS NFT_8,
      SUM(quest_9) AS NFT_9,
      SUM(quest_10) AS NFT_10,
      SUM(quest_11) AS NFT_11,
      SUM(quest_12) AS NFT_12,
      SUM(quest_13) AS NFT_13,
      SUM(quest_1) + SUM(quest_2) + 
      SUM(quest_3) + SUM(quest_4) + 
      SUM(quest_5) + SUM(quest_6) + 
      SUM(quest_7) + SUM(quest_8) + 
      SUM(quest_9) + SUM(quest_10) + 
      SUM(quest_11) + SUM(quest_12) + 
      SUM(quest_13) AS total_nfts
    FROM  -- this is the table created in the previous query
      `rabbithole-analytics.intermediate_tables.quest_completion`
    -- group by address so we can see which quests each address completed,
    -- how many quests total each address completed, and when each address
    -- completed their first quest
    GROUP BY 
      address)
  SELECT
    provided.address,
    quests.*
  FROM
  -- this is the csv of wallet addresses provided to me by Trevor
    `rabbithole-analytics.rabbithole_addresses.addresses` AS provided
  LEFT OUTER JOIN
  -- outer join to leave all provided wallet addresses intact, 
  -- even if they haven't completed a full quest. Values will be null
  -- in these cases
    quest_complete AS quests
  ON
    provided.address = quests.nft_address
  ORDER BY
    total_nfts)