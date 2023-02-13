-- Databricks notebook source
use f1_raw;

-- COMMAND ----------

select*from results

-- COMMAND ----------

select*from results

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
as 
select races.year,
constructors.name as team_name,
drivers.name as driver_name,
results.position,
results.points,
11 - results.position as calculated_points
from results
JOIN f1_raw.drivers on (results.driverId = drivers.driverId)
JOIN f1_raw.constructors on (results.constructorId = constructors.constructorId)
JOIN f1_raw.races on (results.raceId = races.raceId)
where results.position <=10