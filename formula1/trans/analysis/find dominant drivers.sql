-- Databricks notebook source
create or replace temp view v_dominant_drivers
as
select driver_name,
count(1) as total_races ,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by AVG (calculated_points ) desc) driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having count(1)>=50
order by total_points desc

-- COMMAND ----------

select
year,
driver_name,
count(1) as total_races ,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by AVG (calculated_points ) desc) driver_rank
from f1_presentation.calculated_race_results
where driver_name in ( select driver_name from v_dominant_drivers where driver_rank <=10)
group by driver_name , year
order by year , total_points desc

-- COMMAND ----------

select
year,
driver_name,
count(1) as total_races ,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by AVG (calculated_points ) desc) driver_rank
from f1_presentation.calculated_race_results
where driver_name in ( select driver_name from v_dominant_drivers where driver_rank <=10)
group by driver_name , year
order by year , total_points desc

-- COMMAND ----------

select
year,
driver_name,
count(1) as total_races ,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by AVG (calculated_points ) desc) driver_rank
from f1_presentation.calculated_race_results
where driver_name in ( select driver_name from v_dominant_drivers where driver_rank <=10)
group by driver_name , year
order by year , total_points desc