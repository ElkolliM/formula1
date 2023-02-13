-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.circuits(

 circuitId int,
 circuitRef string,
 name string ,
 location string, 
 country string ,
 lat double ,
 lng double ,
 alt int,
 url string

)
using csv
options (path"/mnt/formula1md/raw/circuits.csv" , header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.races(

raceId  int,
year int ,
round  int,
circuitId int,
name string,
date date,
time string,
url string

)
using csv
options (path"/mnt/formula1md/raw/races.csv" , header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING,
nationality STRING,
url STRING
)
using json
options (path"/mnt/formula1md/raw/constructors.json" , header true)


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.drivers(
 driverId int ,
 driverRef string, 
 number int,
 code string ,
 name struct <forename : string ,surname string>,
 dob date ,
 nationality string,
 url string 
)
using json
options (path"/mnt/formula1md/raw/drivers.json" , header true)


-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.results(
raceId  Int,
 driverId Int, 
 constructorId  Int, 
  number Int ,
  grid  Int,
  position Int,
  positionText  String,
  positionOrder Int,
  points  Float,
  laps Int,
  time  String,
  milliseconds Int,
    fastestLap  Int,
   rank Int,
  fastestLapTime  String,
   fastestLapSpeed Float,
   statusId  Int
   )
   using json
options (path"/mnt/formula1md/raw/results.json" , header true)


-- COMMAND ----------

select* from f1_raw.results

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.pit_stops(
race_id int,
driver_id int,
stop string,
lap int,
time string,
duration string,
milliseconds int
)
using json
options (path "/mnt/formula1md/raw/pit_stops.json" , multiLine true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.lap_times(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options (path "/mnt/formula1md/raw/lap_times" )

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS  f1_raw.qualifying(
qualify_Id int,
race_id int,
driver_id int,
constructor_id int,
number int,
position int,
q1 string,
q2 string,
q3 string
)
using csv
options (path "/mnt/formula1md/raw/qualifying" , multiLine true )


-- COMMAND ----------

desc extended f1_raw.qualifying