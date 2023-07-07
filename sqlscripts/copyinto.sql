use database testdb;
use schema landing_zone;

create or replace table trips
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

create or replace stage citibike_trips
url = 's3://snowflake-workshop-lab/citibike-trips-csv/';

show stages;

list @citibike_trips;

show file formats in database testdb;

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

copy into trips from @citibike_trips
file_format = CSV PATTERN = '.*1_0_0*.csv.*'
size_limit = 1000;





