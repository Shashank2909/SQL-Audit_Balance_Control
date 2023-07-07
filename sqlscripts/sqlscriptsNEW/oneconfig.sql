use testdb;
create schema landing_zone;
create schema curated_zone;
create schema consumption_zone;

use schema landing_zone;
create or replace table landing_zone.trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

create or replace stage landing_zone.abc_stage
url = 's3://snowflake-workshop-lab/citibike-trips-csv/';



create or replace file format landing_zone.abc_csv_ff  type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

use schema curated_zone;
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


use schema consumption_zone;
create or replace table consumption_zone.stationwise_daily_rides
(START_STATION_ID integer,
 START_STATION_NAME varchar,
 END_STATION_ID integer,
 END_STATION_NAME varchar,
 RIDE_DATE DATE,
 RIDE_COUNT integer
);