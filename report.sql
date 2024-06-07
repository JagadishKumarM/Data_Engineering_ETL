CREATE DATABASE IF NOT EXISTS report;
USE report;

/* This statement disables the SQL safe updates mode, allowing bulk updates or deletes.*/
SET sql_safe_updates=0;

DESC TABLE cities;
DESC TABLE forecast;

SELECT * FROM cities;
SELECT * FROM forecast;

SELECT * 
FROM cities AS C 
INNER JOIN forecast AS F 
ON C.City_Name=F.City_Name;


/* Writing a query to select City names with elevation more than 300m and the temperature < 40 C on a particluar day */

SELECT C.City_Name, C.Elevation_in_M, F.Temperature,F.`Time`
FROM cities C, forecast F
WHERE C.Elevation_in_M>300 AND F.Temperature <40 AND Day_of_Week='Thursday';


