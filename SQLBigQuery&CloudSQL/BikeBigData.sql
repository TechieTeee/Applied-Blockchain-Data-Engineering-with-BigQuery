#Bike Data EDA
SELECT end_station_name FROM `bigquery-public-data.london_bicycles.cycle_hire`;

SELECT * FROM `bigquery-public-data.london_bicycles.cycle_hire` WHERE duration>=1200;

SELECT start_station_name FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name;

SELECT start_station_name, COUNT(*) FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name;

SELECT start_station_name, COUNT(*) AS num_starts FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name;

SELECT start_station_name, COUNT(*) AS num FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name ORDER BY start_station_name;

SELECT start_station_name, COUNT(*) AS num FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name ORDER BY num;

SELECT start_station_name, COUNT(*) AS num FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name ORDER BY num DESC;

SELECT start_station_name, COUNT(*) AS num FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY start_station_name ORDER BY num DESC;

SELECT end_station_name, COUNT(*) AS num FROM `bigquery-public-data.london_bicycles.cycle_hire` GROUP BY end_station_name ORDER BY num DESC;

#Terminal Commands Not Included


#Creating and Setting Up Database
CREATE DATABASE bike;
USE bike;

CREATE TABLE london1 (start_station_name VARCHAR(255), num INT);
USE bike;

CREATE TABLE london2 (end_station_name VARCHAR(255), num INT);

SELECT * FROM london1;
SELECT * FROM london2;

SELECT * FROM london1;

SELECT * FROM london2;

DELETE FROM london1 WHERE num=0;
DELETE FROM london2 WHERE num=0;

INSERT INTO london1 (start_station_name, num) VALUES ("test destination", 1);

SELECT start_station_name AS top_stations, num FROM london1 WHERE num>100000
UNION
SELECT end_station_name, num FROM london2 WHERE num>100000
ORDER BY top_stations DESC;


