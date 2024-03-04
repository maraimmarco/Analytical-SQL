--Question one:
    --query one:

select distinct customer_id,sum(quantity*price) over(partition by customer_id) as total_revenue 
from tableretail;
-------------------------------------------
    --Query Two:

select substr(invoicedate,1,7)as year_month,
sum(quantity*price)over (partition by substr(invoicedate,1,7)) as total_revenue from tableretail;
-----------------------------------------------------
    --Query Three:

select stockcode,sum(quantity) over(partition by stockcode) as total_quantity_sold from
tableretail order by total_quantity_sold desc;
----------------------------
    --Query Four:

select distinct
 customer_id ,
 count(distinct invoice) over (partition by customer_id)as total_pur
 from tableretail
 order by total_pur desc;
--------------------------------------
    --Query Five:

select distinct invoice,sum(quantity*price) over (partition by invoice) as orde_value
 from tableretail
 order by orde_value desc;
----------------------------------------
    --Query Six:

with CountryProductCounts as (
 select
 country,
 stockcode,
 count(*) as product_count,
 row_number() over(partition by country order by count(*) desc) as rn
 from
 tableretail
 group by
 country, stockcode
)
select
 country,
 stockcode,
 product_count
from
 CountryProductCounts
where
 rn = 1;
------------------------------
    --Query seven:

select
 country,
 stockcode,
 invoicedate,
 quantity,
 lag(quantity) over (partition by stockcode order by invoicedate) as previous_quantity
from
 tableretail
order by
 country, stockcode, invoicedate;
 ---------------------------------------
--Question One:
--b:

WITH customer_purchase_data AS (
SELECT
customer_id,
Quantity,
Price,
Invoice,
ROUND(MAX(TO_DATE(InvoiceDate, 'MM/DD/YYYY HH24:MI')) OVER () -
MAX(TO_DATE(InvoiceDate,'MM/DD/YYYY HH24:MI')) OVER (PARTITION BY customer_id)) AS
recency 
FROM tableRetail
),
customer_frequency_monetary AS (
SELECT
customer_id,
COUNT(DISTINCT invoice) AS frequency,
SUM(price * quantity) AS monetary,
recency 
FROM customer_purchase_data 
GROUP BY customer_id, recency 
),
customer_score AS (
SELECT
customer_id,
recency,
frequency,
NTILE(5) OVER (ORDER BY frequency ) AS F_score,
monetary,
ROUND(PERCENT_RANK() OVER (ORDER BY monetary ), 2) AS monetary_rank 
FROM customer_frequency_monetary 
),
customer_rfm_score AS (
SELECT
customer_id,
recency,
frequency,
monetary,
monetary_rank,
NTILE(5) OVER (ORDER BY recency DESC) AS R_score,
NTILE(5) OVER (ORDER BY (F_score + monetary_rank) / 2) AS FM_score 
FROM customer_score 
)
SELECT
customer_id,
recency AS R,
frequency AS F,
monetary AS M,
R_score,
FM_score,
monetary_rank ,
monetary / SUM(monetary) OVER () AS M_percentage,
CASE
 WHEN R_score = 5 AND FM_score IN (5, 4) THEN 'Champions'
 WHEN R_score = 4 AND FM_score = 5 THEN 'Champions'
 WHEN R_score = 5 AND FM_score = 2 THEN 'Potential Loyalists'
 WHEN R_score = 4 AND FM_score IN (2 , 3) THEN 'Potential Loyalists'
 WHEN R_score = 3 AND FM_score = 3 THEN 'Potential Loyalists'
 WHEN R_score = 5 AND FM_score = 3 THEN 'Loyal Customers'
 WHEN R_score = 4 AND FM_score = 4 THEN 'Loyal Customers'
 WHEN R_score = 3 AND FM_score IN (5, 4) THEN 'Loyal Customers'
 WHEN R_score = 5 AND FM_score = 1 THEN 'Recent Customers'
 WHEN R_score = 4 AND FM_score = 1 THEN 'Promising'
 WHEN R_score = 3 AND FM_score = 1 THEN 'Promising'
 WHEN R_score = 2 AND FM_score IN (3, 2) THEN 'Needs Attention'
 WHEN R_score = 3 AND FM_score = 2 THEN 'Needs Attention'
 WHEN R_score = 2 AND FM_score IN (5, 4) THEN 'At Risk'
 WHEN R_score = 1 AND FM_score = 3 THEN 'At Risk'
 WHEN R_score = 1 AND FM_score IN (5, 4) THEN 'Can not Lose Them'
 WHEN R_score = 1 AND FM_score = 2 THEN 'Hibernating'
 WHEN R_score = 1 AND FM_score = 1 THEN 'Lost'
 ELSE 'Inactive'
END AS Customer_segmentation
FROM customer_rfm_score ;

---------------------------
--Question Two:
--a:

with purchase_dates as (
select
cust_id,
calendar_dt,
row_number() over (partition by cust_id order by calendar_dt) as rn
from
my_table
),
date_diffs as (
select
cust_id,
calendar_dt,
calendar_dt - lag(calendar_dt) over (partition by cust_id order by calendar_dt) as diff
from
purchase_dates
)
select
cust_id,
max(diff) as max_consecutive_days
from
date_diffs
group by
cust_id;
-------------------------------
--B:
with purchase_dates as (
 select
 cust_id,
 min(calendar_dt) as first_purchase_date
 from
 my_table
 group by
 cust_id
),
cumulative_spending as (
 select
 cust_id,
 calendar_dt,
 sum(amt_le) over (partition by cust_id order by calendar_dt) as total_spent
 from
 my_table
),
threshold_dates as (
 select
 cust_id,
 min(case when total_spent >= 250 then calendar_dt end) as threshold_date
 from
 cumulative_spending
 group by
 cust_id
)
select
 avg(
 case
 when threshold_date is not null then threshold_date - first_purchase_date + 1
 else null
 end
 ) as average_days_to_threshold
from (
 select
 fp.cust_id,
 min(fp.first_purchase_date) as first_purchase_date,
 tr.threshold_date
 from
 purchase_dates fp
 left join
 threshold_dates tr on fp.cust_id = tr.cust_id
 group by
 fp.cust_id, tr.threshold_date
) subquery;    