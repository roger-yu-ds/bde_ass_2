--Note that `neighbourhood_cleansed` has been replaced by the more accurate
--`LGA_NAME16`

--(a)
--What are the main differences from a population point of view (i.g. higher population
--of under 30s) between the best performing “neighbourhood_cleansed”and the worst
--(in terms of estimated revenue per active listings) over the last 12 months?

--FROM `data_mart.table1`Get the `LGA_NAME16` or `LGA_CODE16` with the highest `est_revenue_per_active_listing`
--Get the `LGA_NAME16` or `LGA_CODE16` with the lowest `est_revenue_per_active_listing`
--For each LGA, get the distribution of population by age group from `star."2016Census_G01_NSW_LGA"`
--Do some comparisons

with total_table as (
select 
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
cgnl."Age_85ov_P" as total_population
from data_mart.table1 t 
left join star."2016Census_G01_NSW_LGA" cgnl 
on t."LGA_CODE16" = cgnl."LGA_CODE_2016"
where est_revenue_per_active_listing = (select max(est_revenue_per_active_listing) from data_mart.table1)
   or est_revenue_per_active_listing = (select min(est_revenue_per_active_listing) from data_mart.table1)
)
select 
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
, "Age_0_4_yr_P" / total_population::float AS Age_0_4_yr_P_perc 
, "Age_5_14_yr_P" / total_population::float AS Age_5_14_yr_P_perc 
, "Age_15_19_yr_P" / total_population::float AS Age_15_19_yr_P_perc 
, "Age_20_24_yr_P" / total_population::float AS Age_20_24_yr_P_perc 
, "Age_25_34_yr_P" / total_population::float AS Age_25_34_yr_P_perc 
, "Age_35_44_yr_P" / total_population::float AS Age_35_44_yr_P_perc 
, "Age_45_54_yr_P" / total_population::float AS Age_45_54_yr_P_perc 
, "Age_55_64_yr_P" / total_population::float AS Age_55_64_yr_P_perc 
, "Age_65_74_yr_P" / total_population::float AS Age_65_74_yr_P_perc 
, "Age_75_84_yr_P" / total_population::float AS Age_75_84_yr_P_perc 
, "Age_85ov_P" / total_population::float AS Age_85ov_P_perc  
from total_table;


--(b)
--What will be the best type of listing (property type, room type and accommodates for)
--for the top 5 “neighbourhood_cleansed” (in terms of estimated revenue per active
--listing) to have the highest number of stays?

--Get the top 5 `LGA_NAME16` or `LGA_CODE16` by `est_revenue_per_active_listing`
--Within these 5 LGAs, GROUP BY property type, room type and accommodates for, and sum the number of stays
--Pick the group with the highest number of stays

select 
  property_type 
, room_type 
, accommodates 
, MAX(est_revenue_per_active_listing) as max_revenue
, SUM(n_stays) as total_n_stays
from data_mart.TABLE2 
group by 1, 2, 3
order by 4 desc 
limit 5
;

--(c)
--Are hosts with multiple listings more inclined to have their listings in the same
--“neighbourhood” as where they live?

--?In `star.dim_property`, filter on `host_id` with more than one `id`
--?Clean the `star.dim_host.host_neighbourhood`, get the SSC_NAME
--Join the `star.dim_property` with `star.dim_host.host_neighbourhood_cleansed`
--For each host, calculate the percentage of the properties that are in the same SSC.
--Average the proportions, or group them, e.g. 
--  * for hosts with 2 properties: 
--    * x% have 2 in the home SSC
--    * y% have 1 in the home SSC
--    * z% have 0 in the home SSC
--  * for hosts with 3 properties: 10% have the second property outside of their home SSC:
--    * w% have 3 in the home SSC
--    * x% have 2 in the home SSC
--    * y% have 1 in the home SSC
--    * z% have 0 in the home SSC

-- SSC cleaned
with ssc_cleaned AS(
    select 
      geometry
    , split_part(SA."SSC_NAME", '(', 1) as ssc_name_clean 
    from star."SSC_2011_AUST" sa 
    where not (not (SA."SSC_NAME" LIKE '%NSW%') and (SA."SSC_NAME" LIKE '%(%'))
),
host_property_gt_1 as (
	select 
	  host_id 
	, count(distinct(id)) as n_property
	from STAR.dim_property dp 
	group by 1
	having count(distinct(id)) > 1
), 
property_with_clean_suburb as (
	select 
	  dp.id
	, dp.host_id
	, sc.ssc_name_clean as property_suburb
	from star.dim_property dp
	left join ssc_cleaned sc
	on ST_CONTAINS(st_setsrid(sc.geometry, 4326), ST_SetSRID(st_point(dp.longitude, dp.latitude), 4326))
),
-- Clean the `host_neighbourhood`
host_ssc as (
	select 
	  host_id
    , split_part(dh.host_neighbourhood, '/', 1) as host_suburb 
	from star.dim_host dh 
	where dh.host_neighbourhood not in (select host_neighbourhood from star.host_neighbourhood_mapping hnm2)
	union
	select 
	  dh.host_id
	, hnm.host_neighbourhood as host_suburb
	from star.dim_host dh 
	left join star.host_neighbourhood_mapping hnm 
	on dh.host_neighbourhood = hnm.host_neighbourhood
	where dh.host_neighbourhood in (select host_neighbourhood from star.host_neighbourhood_mapping hnm2)
),
comparison as (
	select 
	  pwcs.id
	, pwcs.host_id
	, pwcs.property_suburb
	, hs.host_suburb
	, case when hs.host_suburb = pwcs.property_suburb then 1 else 0 end as same_neighbourhood
	from property_with_clean_suburb pwcs
	left join host_ssc hs
	on pwcs.host_id = hs.host_id
	where pwcs.host_id in (select host_id from host_property_gt_1)
),
proportion_same as (
select 
  host_id
, count(host_id) as n_property
, sum(same_neighbourhood) as properties_in_same_neighbourhood
, avg(same_neighbourhood) as proportion
from comparison
where host_suburb is not null
group by host_id
limit 20
)
select 
avg(proportion) as total_proportion
from proportion_same
;
--(d)
--For hosts with a unique listing, does their estimated revenue over the last 12 months
--cover the annualised median mortgage repayment of their listing’s “neighbourhood_cleansed”/"LGA"?

--In `star.dim_property`, filter on `host_id` with only one `id`
--Join with `star."2016Census_G02_NSW_LGA".Median_mortgage_repay_monthly`
--Per LGA and host, average the revenue over the 12 months
--Per LGA, count the number of hosts that 

WITH host_property_e_1 AS (
	SELECT 
	  host_id 
	, COUNT(DISTINCT(id)) AS n_property
	FROM STAR.dim_property dp 
	GROUP BY 1
	HAVING COUNT(DISTINCT(id)) = 1
),
est_annual_revenue as (
    select  
      fa.host_id
    , fa.id
    , min(dp."LGA_CODE_2016") as lga_code
    , sum((30 - fa.availability_30) * dp.price) as est_annual_revenue
    from star.fact_airbnb fa 
    left join star.dim_property dp on fa.id = dp.id
    group by 1, 2
),
revenue_mortgage as (
	select 
	  ear.host_id 
	, ear.id
	, ear.lga_code
	, ear.est_annual_revenue
	, cgnl."Median_mortgage_repay_monthly" * 12 as median_annual_mortgage
	from est_annual_revenue ear
	left join star."2016Census_G02_NSW_LGA" cgnl 
	  on ear.lga_code = cgnl."LGA_CODE_2016" 
	where ear.host_id in (select host_id from host_property_e_1)
	limit 10
)
select 
  avg(case when (est_annual_revenue - median_annual_mortgage) > 0 then 1 else 0 end) as proportion_covered
from revenue_mortgage