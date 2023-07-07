create or replace table curated_zone.stations
( stations_key number(38,0) NOT NULL autoincrement,
station_id integer,
station_name varchar,
created_date timestamp,
updated_date timestamp);

create or replace table curated_zone.trips_fact
( trips_fact_key number(38,0) NOT NULL autoincrement,
tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);