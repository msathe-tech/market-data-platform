## VIEW using Pivot Table 
CREATE OR REPLACE VIEW `lseg_testing.LSE_PIVOTED_BY_RIC` AS
SELECT * FROM (
    WITH AllTrades AS(
      SELECT Date_Time,RIC,Price,Volume
      FROM `dbd-sdlc-prod.LSE_NORMALISED.LSE_NORMALISED` 
      WHERE Price IS NOT NULL
      -- Specific Date/Time range:
      AND (Date_Time BETWEEN "2023-05-16 12:00:00.000000" AND "2023-05-17 12:59:59.999999")
      AND Type = "Trade"
      AND VOLUME > 0
      AND PRICE > 0
      # All trades reported as "On Book" & "Regular Trades"
      # This is according to the FIX specs, most European trading venues adhere to this
      AND RIGHT(REGEXP_EXTRACT(Qualifiers, r";(.*)\[MMT_CLASS\]"),14) LIKE "12%"
    )
    SELECT RIC, ROUND(SAFE_DIVIDE(SUM(Volume*Price),SUM(Volume)),3) AS VWAP,SUM(Volume) AS TotalVolume,COUNT(RIC) AS NumTrades, extract(DATE FROM Date_Time) AS vwap_date
    FROM AllTrades
    WHERE RIC IN ('ULVR.L','VOD.L')
    GROUP BY RIC, vwap_date
    ORDER BY RIC
)
PIVOT (
  AVG (VWAP) AS VWAP,
  SUM (TotalVolume) AS TotalVolume,
  SUM(NumTrades) AS NumTrades
  FOR vwap_date in ('2023-05-17', '2023-05-16'));

-- CREATE THE CONNECTION TO THE LLM
CREATE OR REPLACE MODEL lseg_testing.llm_model
  REMOTE WITH CONNECTION `us.call-palm2`
  OPTIONS (remote_service_type = 'CLOUD_AI_LARGE_LANGUAGE_MODEL_V1');

-- CREATE SUMMARY COMPARING VWAP, VOLUME AND NUMBER TRADES OVER TIME PER RIC
  SELECT * FROM
  ML.GENERATE_TEXT(
    MODEL `duet-1.lseg_testing.llm_model`,
    (
  SELECT
        CONCAT('From the following trading information, make an executive summary comparing vwap, volume and number of trades over 2 days. Always mention the company legal name based on the RIC:', 
        RIC,' vwap day one:', VWAP_2023_05_16, ' vwap day 2', VWAP_2023_05_17, ' total trades day one', NumTrades_2023_05_16, ' trades day 2:',NumTrades_2023_05_17, 'total volume day one: ',TotalVolume_2023_05_16,' total volume day 2:',TotalVolume_2023_05_17 )
        AS prompt from `lseg_testing.LSE_PIVOTED_BY_RIC`
         
        ),
    STRUCT(
      0.2 AS temperature,
      800 AS max_output_tokens,
      TRUE AS flatten_json_output));