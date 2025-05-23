DROP DATABASE dimenzijski_model;
CREATE DATABASE dimenzijski_model;

USE dimenzijski_model;

SELECT COUNT(*) FROM dim_airline;
SELECT COUNT(*) FROM dim_aircraft;
SELECT COUNT(*) FROM fact_flight;

SELECT * FROM dim_airline LIMIT 10;
SELECT * FROM dim_aircraft LIMIT 10;
SELECT * FROM dim_arr_delay LIMIT 10;
SELECT * FROM dim_dep_delay LIMIT 10;
SELECT * FROM dim_route LIMIT 10;
SELECT * FROM dim_date LIMIT 10;
SELECT * FROM dim_time LIMIT 10;

SELECT * FROM fact_flight LIMIT 10;

-- Provjera NULL time_id u fact_flight tablici
SELECT 
    COUNT(*) as total_flights,
    COUNT(time_id) as flights_with_time_id,
    COUNT(*) - COUNT(time_id) as flights_missing_time_id,
    ROUND((COUNT(*) - COUNT(time_id)) * 100.0 / COUNT(*), 2) as percentage_missing
FROM dimenzijski_model.fact_flight;

SELECT DISTINCT 
    sched_dep_time, hour, minute, time_id
FROM fact_flight 
WHERE time_id IS NULL 
LIMIT 10;


SELECT COUNT(*) AS broj_letova_bez_kasnjenja
FROM star_shema.fact_flight ff
JOIN star_shema.dim_dep_delay ddep ON ff.dep_delay_id = ddep.dep_delay_tk
JOIN star_shema.dim_arr_delay darr ON ff.arr_delay_id = darr.arr_delay_tk
WHERE ddep.delay_time = 0 AND darr.delay_time = 0;

select count(*) from fact_flight;

