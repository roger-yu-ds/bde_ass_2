--Note that `neighbourhood_cleansed` hAS been replaced by the mORe accurate
--`LGA_NAME16`

--(a)
--What are the main differences FROM a populatiON point of view (i.g. higher populatiON
--of under 30s) between the best perfORMINg “neighbourhood_cleansed”and the wORst
--(in terms of estimated revenue per active listings) over the lASt 12 mONths?
WITH total_table AS (
SELECT 
  t."LGA_CODE16" 
, t."LGA_NAME16" 
, t.est_revenue_per_active_listing 
, cgnl."Age_0_4_yr_P"
, cgnl."Age_5_14_yr_P"
, cgnl."Age_15_19_yr_P"
, cgnl."Age_20_24_yr_P"
, cgnl."Age_25_34_yr_P"
, cgnl."Age_35_44_yr_P"
, cgnl."Age_45_54_yr_P"
, cgnl."Age_55_64_yr_P"
, cgnl."Age_65_74_yr_P"
, cgnl."Age_75_84_yr_P"
, cgnl."Age_85ov_P"
, cgnl."Age_0_4_yr_P" + 
cgnl."Age_5_14_yr_P" + 
cgnl."Age_15_19_yr_P" + 
cgnl."Age_20_24_yr_P" + 
cgnl."Age_25_34_yr_P" + 
cgnl."Age_35_44_yr_P" + 
cgnl."Age_45_54_yr_P" + 
cgnl."Age_55_64_yr_P" + 
cgnl."Age_65_74_yr_P" + 
cgnl."Age_75_84_yr_P" + 
cgnl."Age_85ov_P" AS total_populatiON
FROM data_mart.table1 t 
LEFT JOIN star."2016Census_G01_NSW_LGA" cgnl 
ON t."LGA_CODE16" = cgnl."LGA_CODE_2016"
WHERE est_revenue_per_active_listing = (SELECT max(est_revenue_per_active_listing) FROM data_mart.table1)
   OR est_revenue_per_active_listing = (SELECT MIN(est_revenue_per_active_listing) FROM data_mart.table1)
)
SELECT 
  "LGA_CODE16" 
, "LGA_NAME16" 
, "Age_0_4_yr_P"
, "Age_5_14_yr_P"
, "Age_15_19_yr_P"
, "Age_20_24_yr_P"
, "Age_25_34_yr_P"
, "Age_35_44_yr_P"
, "Age_45_54_yr_P"
, "Age_55_64_yr_P"
, "Age_65_74_yr_P"
, "Age_75_84_yr_P"
, "Age_85ov_P"
, total_population
, "Age_0_4_yr_P" / total_populatiON::FLOAT AS Age_0_4_yr_P_perc 
, "Age_5_14_yr_P" / total_populatiON::FLOAT AS Age_5_14_yr_P_perc 
, "Age_15_19_yr_P" / total_populatiON::FLOAT AS Age_15_19_yr_P_perc 
, "Age_20_24_yr_P" / total_populatiON::FLOAT AS Age_20_24_yr_P_perc 
, "Age_25_34_yr_P" / total_populatiON::FLOAT AS Age_25_34_yr_P_perc 
, "Age_35_44_yr_P" / total_populatiON::FLOAT AS Age_35_44_yr_P_perc 
, "Age_45_54_yr_P" / total_populatiON::FLOAT AS Age_45_54_yr_P_perc 
, "Age_55_64_yr_P" / total_populatiON::FLOAT AS Age_55_64_yr_P_perc 
, "Age_65_74_yr_P" / total_populatiON::FLOAT AS Age_65_74_yr_P_perc 
, "Age_75_84_yr_P" / total_populatiON::FLOAT AS Age_75_84_yr_P_perc 
, "Age_85ov_P" / total_populatiON::FLOAT AS Age_85ov_P_perc  
FROM total_table;


--(b)
--What will be the best type of listing (property type, room type and accommodates fOR)
--fOR the top 5 “neighbourhood_cleansed” (in terms of estimated revenue per active
--listing) to have the highest number of stays?
SELECT 
  property_type 
, room_type 
, accommodates 
, MAX(est_revenue_per_active_listing) AS max_revenue
, SUM(n_stays) AS total_n_stays
FROM data_mart.TABLE2 
GROUP BY 1, 2, 3
ORder by 4 desc 
limit 5
;

--(c)
--Are hosts WITH multiple listings mORe inclined to have their listings in the same
--“neighbourhood” AS WHERE they live?
WITH ssc_cleaned AS(
    SELECT 
      geometry
    , split_part(SA."SSC_NAME", '(', 1) AS ssc_name_clean 
    FROM star."SSC_2011_AUST" sa 
    WHERE not (not (SA."SSC_NAME" LIKE '%NSW%') AND (SA."SSC_NAME" LIKE '%(%'))
),
host_property_gt_1 AS (
	SELECT 
	  host_id 
	, count(distinct(id)) AS n_property
	FROM STAR.dim_property dp 
	GROUP BY 1
	HAVING COUNT(DISTINCT(id)) > 1
), 
property_WITH_clean_suburb AS (
	SELECT 
	  dp.id
	, dp.host_id
	, sc.ssc_name_clean AS property_suburb
	FROM star.dim_property dp
	LEFT JOIN ssc_cleaned sc
	ON ST_CONTAINS(st_setsrid(sc.geometry, 4326), ST_SetSRID(st_point(dp.lONgitude, dp.latitude), 4326))
),
-- Clean the `host_neighbourhood`
host_ssc AS (
	SELECT 
	  host_id
    , split_part(dh.host_neighbourhood, '/', 1) AS host_suburb 
	FROM star.dim_host dh 
	WHERE dh.host_neighbourhood NOT IN (SELECT host_neighbourhood FROM star.host_neighbourhood_mapping hnm2)
	uniON
	SELECT 
	  dh.host_id
	, hnm.host_neighbourhood AS host_suburb
	FROM star.dim_host dh 
	LEFT JOIN star.host_neighbourhood_mapping hnm 
	ON dh.host_neighbourhood = hnm.host_neighbourhood
	WHERE dh.host_neighbourhood IN (SELECT host_neighbourhood FROM star.host_neighbourhood_mapping hnm2)
),
comparison AS (
	SELECT 
	  pwcs.id
	, pwcs.host_id
	, pwcs.property_suburb
	, hs.host_suburb
	, CASE WHEN hs.host_suburb = pwcs.property_suburb THEN 1 ELSE 0 END AS same_neighbourhood
	FROM property_WITH_clean_suburb pwcs
	LEFT JOIN host_ssc hs
	ON pwcs.host_id = hs.host_id
	WHERE pwcs.host_id in (SELECT host_id FROM host_property_gt_1)
),
proportion_same AS (
SELECT 
  host_id
, count(host_id) AS n_property
, SUM(same_neighbourhood) AS properties_in_same_neighbourhood
, AVG(same_neighbourhood) AS proportion
FROM comparison
WHERE host_suburb IS NOT NULL
GROUP BY host_id
)
SELECT 
AVG(proportiON) AS total_proportiON
FROM proportiON_same
;


--(d)
--FOR hosts WITH a unique listing, does their estimated revenue over the lASt 12 mONths
--cover the annualised median mORtgage repayment of their listing’s “neighbourhood_cleansed”/"LGA"?

WITH host_property_e_1 AS (
	SELECT 
	  host_id 
	, COUNT(DISTINCT(id)) AS n_property
	FROM STAR.dim_property dp 
	GROUP BY 1
	HAVING COUNT(DISTINCT(id)) = 1
),
est_annual_revenue AS (
    SELECT  
      fa.host_id
    , fa.id
    , MIN(dp."LGA_CODE_2016") AS lga_code
    , SUM((30 - fa.availability_30) * dp.price) AS est_annual_revenue
    FROM star.fact_airbnb fa 
    LEFT JOIN star.dim_property dp ON fa.id = dp.id
    GROUP BY 1, 2
),
revenue_mORtgage AS (
	SELECT 
	  ear.host_id 
	, ear.id
	, ear.lga_code
	, ear.est_annual_revenue
	, cgnl."Median_mortgage_repay_monthly" * 12 AS median_annual_mORtgage
	FROM est_annual_revenue ear
	LEFT JOIN star."2016Census_G02_NSW_LGA" cgnl 
	  ON ear.lga_code = cgnl."LGA_CODE_2016" 
	WHERE ear.host_id IN (SELECT host_id FROM host_property_e_1)
)
SELECT 
  AVG(CASE WHEN (est_annual_revenue - median_annual_mORtgage) > 0 THEN 1 ELSE 0 END) AS propORtiON_covered
FROM revenue_mORtgage;