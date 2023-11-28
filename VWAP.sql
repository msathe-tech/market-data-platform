-- Get VWAP for Uniliver, Vodafone, Standard Chartard, Natwest for the 17th of Nov 2023
WITH AllTrades AS(
  SELECT Date_Time,RIC,Price,Volume
  FROM `dbd-sdlc-prod.LSE_NORMALISED.LSE_NORMALISED` 
  WHERE Price IS NOT NULL
  -- Specific Date/Time range:
  AND (Date_Time BETWEEN "2023-11-17 12:00:00.000000" AND "2023-11-17 12:59:59.999999")
  AND Type = "Trade"
  AND VOLUME > 0
  AND PRICE > 0
  # All trades reported as "On Book" & "Regular Trades"
  # This is according to the FIX specs, most European trading venues adhere to this
  AND RIGHT(REGEXP_EXTRACT(Qualifiers, r";(.*)\[MMT_CLASS\]"),14) LIKE "12%"
)
SELECT RIC, ROUND(SAFE_DIVIDE(SUM(Volume*Price),SUM(Volume)),3) AS VWAP,ROUND(AVG(Price),3) AS TWAP, SUM(Volume) AS TotalVolume,COUNT(RIC) AS NumTrades
FROM AllTrades
WHERE RIC IN ('ULVR.L','VOD.L', 'STAN.L', 'NWG.L')
GROUP BY RIC
ORDER BY RIC;

-- Get VWAP for Uniliver, Vodafone, Standard Chartard, Natwest for the last 6 months
WITH AllTrades AS(
  SELECT Date_Time,RIC,Price,Volume
  FROM `dbd-sdlc-prod.LSE_NORMALISED.LSE_NORMALISED` 
  WHERE Price IS NOT NULL
  -- Specific Date/Time range:
  AND (Date_Time BETWEEN "2023-5-17 12:00:00.000000" AND "2023-11-17 12:59:59.999999")
  AND Type = "Trade"
  AND VOLUME > 0
  AND PRICE > 0
  # All trades reported as "On Book" & "Regular Trades"
  # This is according to the FIX specs, most European trading venues adhere to this
  AND RIGHT(REGEXP_EXTRACT(Qualifiers, r";(.*)\[MMT_CLASS\]"),14) LIKE "12%"
)
SELECT RIC, ROUND(SAFE_DIVIDE(SUM(Volume*Price),SUM(Volume)),3) AS VWAP,ROUND(AVG(Price),3) AS TWAP, SUM(Volume) AS TotalVolume,COUNT(RIC) AS NumTrades
FROM AllTrades
WHERE RIC IN ('ULVR.L','VOD.L', 'STAN.L', 'NWG.L')
GROUP BY RIC
ORDER BY RIC;

-- Get VWAP for Uniliver, Vodafone, Standard Chartard, Natwest for the last 23 years
WITH AllTrades AS(
  SELECT Date_Time,RIC,Price,Volume
  FROM `dbd-sdlc-prod.LSE_NORMALISED.LSE_NORMALISED` 
  WHERE Price IS NOT NULL
  -- Specific Date/Time range:
  AND (Date_Time BETWEEN "2000-5-17 12:00:00.000000" AND "2023-11-17 12:59:59.999999")
  AND Type = "Trade"
  AND VOLUME > 0
  AND PRICE > 0
  # All trades reported as "On Book" & "Regular Trades"
  # This is according to the FIX specs, most European trading venues adhere to this
  AND RIGHT(REGEXP_EXTRACT(Qualifiers, r";(.*)\[MMT_CLASS\]"),14) LIKE "12%"
)
SELECT RIC, ROUND(SAFE_DIVIDE(SUM(Volume*Price),SUM(Volume)),3) AS VWAP,ROUND(AVG(Price),3) AS TWAP, SUM(Volume) AS TotalVolume,COUNT(RIC) AS NumTrades
FROM AllTrades
WHERE RIC IN ('ULVR.L','VOD.L', 'STAN.L', 'NWG.L')
GROUP BY RIC
ORDER BY RIC;