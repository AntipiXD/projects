-- Section: Market Overview / Annuals
-- Total charts: 5

-- The following query is for the first four charts: 
-- (1) sales volume in {{area filter value}}, 
-- (2) sales value in {{area filter value}}, 
-- Property price change in {{area filter value}} (two  graphs: (3) price per sqft, (4) price)

-- Query:
-- Create a materialized view called 'market_overview' to store pre-aggregated market sales data
CREATE MATERIALIZED VIEW market_overview AS

SELECT
    -- Extract year from the transaction date
    EXTRACT(YEAR FROM instance_date) AS instance_year,
    
    -- Extract month from the transaction date
    EXTRACT(MONTH FROM instance_date) AS instance_month,
    
    -- Simplify property usage: if Residential keep as is, else mark as Commercial
    CASE
        WHEN property_usage = 'Residential' THEN 'Residential'
        ELSE 'Commercial'
    END AS property_usage_modified,
    
    -- Transaction group ID (e.g., Sales, Mortgage)
    trans_group_id,
    
    -- Area ID of the property
    area_id,
    
    -- Market type ID (e.g., Primary, Secondary)
    reg_type_id,
    
    -- Total sales value in this group
    SUM(actual_worth) AS sales_value,
    
    -- Total number of transactions in this group
    COUNT(*) AS sales_volume,
    
    -- Median sale price within this group
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) AS median_price,
    
    -- Median price per square foot (converting property area to sqft)
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (actual_worth / (procedure_area * 10.7639))) AS median_price_per_sqft,
    
    -- Average sales value of the past 3 years (only considers transactions within last 3 years)
    ROUND(
        AVG(
            CASE
                WHEN instance_date >= CURRENT_DATE - INTERVAL '3 years' THEN actual_worth
                ELSE NULL
            END
        ), 
    2) AS avg_sales_prev_three_years

FROM
    transactions

-- Exclude certain transaction group (e.g., group 3)
WHERE
    trans_group_id != 3
    
    -- Only consider data from 2008 onwards
    AND EXTRACT(YEAR FROM instance_date) >= 2008

-- Group data by year, month, property type, transaction group, area, and market type
GROUP BY
    EXTRACT(YEAR FROM instance_date),
    EXTRACT(MONTH FROM instance_date),
    CASE
        WHEN property_usage = 'Residential' THEN 'Residential'
        ELSE 'Commercial'
    END,
    trans_group_id,
    area_id,
    reg_type_id;

-- --Filters
-- 1) Type: All (by default), Residential, Commercial
-- (case
-- 	when property_usage = 'Residential' then 'Residential'
-- 	else 'Commercial'
-- end) as "Type"

-- 2) Procedure: Mortgage, Sales (Sales by default)
-- trans_group_name as "Procedure"

-- 3) Area: (all areas by default), area name
-- area_name as "Area"

-- 4) Market: All (All by default), Primary, Secondary
    -- (case
	-- 	when reg_type_name = 'Existing Properties' then 'Secondary'
	-- 	when reg_type_name = 'Off-Plan Properties' then 'Primary'
	-- end) as "Market"

-- 5) Till end of month: user chooses month, All (all months)
-- start month = January (fixed), end month = (user chooses month or chooses all months)


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Section: Best selling areas in Dubai
-- Total selects: 6

-- Graphs:
-- 1. Top 5 performing areas (Sales volume) - Bar graph
-- 2. Top performing areas, market share (Sales Volume) - Pie chart
-- 3. and 4. Median property prices in the top performing areas - Line chart
-- 5. Top 5 performing areas (Rental value) 
-- 6. Top 5 performing areas (Cap Gain)

-- CHART 2
-- Top performing areas, market share (Sales Volume) - Pie chart

-- Query
with area_stats_by_filter as (
 select
  area_id,
  SUM(volume) as total_volume,
  row_number() over (
   order by
       SUM(volume) desc
      ) as rn
 from area_volume_value_group 
 -- where 
	--Filters:
  	-- year -- user chooses year
	-- and property_type (All (By default), Residential, Commercial)
	-- bed (Any Bed (default), 1, 2, 3, 4, 5, 6, 7, 7+, Studio)
	-- market (All (All by default), Primary, Secondary)
 group by area_id
),
area_stats_with_share as (
 select 
  area_id,
  total_volume,
  rn,
  ROUND(
      total_volume::numeric * 100.0 / SUM(total_volume) OVER (),
      10
   ) AS share_pct
 from area_stats_by_filter
),
top_seven_area as (
select 
 area_id,
 total_volume,
 share_pct
from 
 area_stats_with_share
where 
 rn < 8),
remaining as (
select
 -1 area_id,
    sum(a.total_volume)
      - coalesce((select sum(total_volume) from top_seven_area), 0)
      as total_volume,
    sum(a.share_pct)
      - coalesce((select sum(share_pct)   from top_seven_area), 0)
      as share_pct
from area_stats_with_share a
)
select
 area_id, -- here '-1' is 'Others' in the pie chart
 total_volume,
 share_pct
from 
 top_seven_area
union
select 
 area_id,
 total_volume,
 share_pct
from 
 remaining;






-- Filters
-- 1) Year - user chooses year (Current year - by default)
-- year

-- 2) Type - user chooses type - All (By default), Residential, Commercial
-- property_type

-- 3) Bed - user chooses bed - Any Bed (default), 1, 2, 3, 4, 5, 6, 7, 7+, Studio
	-- (case
	-- 	when rooms_name = 'Studio' then 'Studio'
	-- 	when rooms_name = '1 B/R' then '1'
	-- 	when rooms_name = '2 B/R' then '2'
	-- 	when rooms_name = '3 B/R' then '3'
	-- 	when rooms_name = '4 B/R' then '4'
	-- 	when rooms_name = '5 B/R' then '5'
	-- 	when rooms_name = '6 B/R' then '6'
	-- 	when rooms_name = '7 B/R' then '7'
	-- 	else '7+'
	-- end) as "Bed"

-- 4) Market: All (All by default), Primary, Secondary
    -- (case
		-- WHEN reg_type_id = 1 THEN 'Secondary' (Existing Properties)
		-- WHEN reg_type_id = 0 THEN 'Primary' (Off-Plan Properties)
	-- end) as "Market"



----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- ROI section.
-- The following query aims to filter the data to fix the ROI issue.
-- Fitlers done:
-- Time interval
-- Property type
-- price limit, area limit
-- (For Sales) Price limit, market is not Gifts), actual area limit, projects are not null 

-- ROI section: calculate rental yield and property flip ROI
-- Filters applied:
-- 1) Time interval (last 12 years)
-- 2) Property type
-- 3) Price limit, area limit
-- 4) Ensure project_number exists

-- Step 1: Filter rent contracts
WITH rents AS (
    SELECT
        -- Year-month for grouping
        date_trunc('year', contract_start_date) AS ym,
        id,
        area_id,
        project_number,
        actual_area,
        property_usage,
        ejari_property_type_id,
        annual_amount::numeric AS annual_rent
    FROM rent_contracts
    WHERE
        -- Only consider contracts in last 12 years
        contract_start_date >= date_trunc('year', CURRENT_DATE) - interval '12 years'
        AND contract_start_date < date_trunc('year', CURRENT_DATE) + interval '1 year'
        -- Only certain property types
        AND ejari_bus_property_type IN ('Villa', 'Unit')
        AND ejari_property_type IN ('Flat', 'Villa')
        AND ejari_property_sub_type IN (
            'Studio', '1bed room+Hall', '2 bed rooms+hall', '2 bed rooms+hall+Maids Room',
            '3 bed rooms+hall', '4 bed rooms+hall', '5 bed rooms+hall', '6 bed rooms+hall',
            '7 bed rooms+hall', '8 bed rooms+hall', '9 bed rooms+hall', '10 bed rooms+hall',
            '11 bed rooms+hall', '15 bed room+hall'
        )
        -- Minimum area and rent filters
        AND actual_area IS NOT NULL
        AND actual_area > 39
        AND contract_end_date - contract_start_date > 365
        AND contract_amount > 5000
        AND project_number IS NOT NULL
),

-- Step 2: Filter sales transactions
sales AS (
    SELECT
        date_trunc('year', instance_date) AS ym,
        id,
        area_id,
        project_number,
        property_usage,
        property_type_id,
        procedure_area AS sale_area,
        actual_worth AS sale_price
    FROM transactions
    WHERE
        -- Only consider last 12 years
        instance_date >= date_trunc('year', CURRENT_DATE) - interval '12 years'
        -- Only certain property types
        AND property_type_name IN ('Villa', 'Unit')
        AND property_sub_type_name IN ('Unit', 'Flat', 'Villa')
        AND rooms_name IN ('Studio','1 B/R','2 B/R','3 B/R','4 B/R','5 B/R','6 B/R','7 B/R','8 B/R','9 B/R')
        -- Ensure valid project number, area and price
        AND project_number IS NOT NULL
        AND procedure_area > 40
        AND actual_worth > 8000
        AND reg_type_id != 3
),

-- Step 3: Aggregate rental data per project and property type
rent_agg AS (
    SELECT
        ym,
        area_id,
        project_number,
        property_usage,
        ejari_property_type_id AS property_type,
        SUM(annual_rent) AS sum_annual_rent,
        SUM(actual_area) AS sum_rent_area,
        (SUM(annual_rent) / SUM(actual_area)) AS rent_per_sqm  -- Rent per sqm
    FROM rents
    GROUP BY 1,2,3,4,5
),

-- Step 4: Aggregate sales data per project and property type
sale_agg AS (
    SELECT
        ym,
        area_id,
        project_number,
        property_usage,
        property_type_id AS property_type,
        SUM(sale_price) AS sum_sale_price,
        SUM(sale_area) AS sum_sale_area,
        (SUM(sale_price) / SUM(sale_area)) AS price_per_sqm  -- Price per sqm
    FROM sales
    GROUP BY 1,2,3,4,5
),

-- Step 5: Join rental and sales aggregates to get both metrics per project
rental_sales_joined AS (
    SELECT
        r.ym,
        r.area_id,
        r.project_number,
        r.property_usage,
        r.rent_per_sqm,
        s.price_per_sqm
    FROM rent_agg r
    JOIN sale_agg s
        USING (ym, area_id, project_number, property_usage)
),

-- Step 6: Compute yearly averages for rental and sales per sqm
yearly_agg AS (
    SELECT
        EXTRACT(YEAR FROM ym) AS ym,
        AVG(rent_per_sqm) AS avg_rent_per_sqm,
        AVG(price_per_sqm) AS avg_price_per_sqm
    FROM rental_sales_joined
    GROUP BY 1
),

-- Step 7: Calculate ROI metrics
rental_flip_roi AS (
    SELECT
        ym,
        -- Rental yield = rent per sqm / price per sqm
        (avg_rent_per_sqm / avg_price_per_sqm) AS rental_yield,
        -- Flip ROI = % increase in price year-over-year
        (avg_price_per_sqm - LAG(avg_price_per_sqm) OVER (ORDER BY ym)) 
          / LAG(avg_price_per_sqm) OVER (ORDER BY ym) AS flip_roi
    FROM yearly_agg
),

-- Step 8: Final ROI calculation, round percentages
final_roi_query AS (
    SELECT
        ym,
        ROUND(rental_yield * 100, 0) AS rental_yield, -- % rental yield
        ROUND(flip_roi * 100, 0) AS flip_roi,         -- % price change
        -- Total ROI = rental yield + flip ROI (if flip ROI exists)
        CASE
            WHEN flip_roi IS NULL THEN ROUND(rental_yield * 100, 0)
            ELSE ROUND((rental_yield + flip_roi) * 100, 0)
        END AS total_roi
    FROM rental_flip_roi
)

-- Step 9: Return the final yearly ROI metrics
SELECT *
FROM final_roi_query;
