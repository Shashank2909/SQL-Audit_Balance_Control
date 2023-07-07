create or replace schema landing_zone;
create or replace schema curated_zone;
create or replace schema consumption_zone;

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

create or replace file format CSV type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting    data for zero to snowflake';
  

create or replace stream landing_trips_stm on table trips
append_only = true;

-- copy into trips from @citibike_trips file_format = CSV;

--------------------------------------------------------------------
use schema curated_zone;

create or replace table trips_curated
(tripduration integer,
start_station_id integer,
start_station_name string,
end_station_id integer,
end_station_name string,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

create or replace stream curated_trips_stm on table trips_curated;


create or replace task curated_tsk
warehouse = compute_wh
schedule = '1 minute'
when
    system$stream_has_data('testdb.landing_zone.landing_trips_stm')
as
merge into testdb.curated_zone.trips_curated trips_curated using
testdb.landing_zone.landing_trips_stm trips on
trips_curated.bikeid = trips.bikeid
when matched
then update set
trips_curated.tripduration = trips.tripduration,
trips_curated.start_station_id = trips.start_station_id,
trips_curated.start_station_name = trips.start_station_name,
trips_curated.end_station_id = trips.end_station_id,
trips_curated.end_station_name = trips.end_station_name,
trips_curated.bikeid = trips.bikeid,
trips_curated.membership_type = trips.membership_type,
trips_curated.usertype = trips.usertype,
trips_curated.birth_year = trips.birth_year,
trips_curated.gender = trips.gender
when not matched then
insert (
trips_curated.tripduration,
trips_curated.start_station_id,
trips_curated.start_station_name,
trips_curated.end_station_id,
trips_curated.end_station_name,
trips_curated.bikeid,
trips_curated.membership_type,
trips_curated.usertype,
trips_curated.birth_year,
trips_curated.gender)
values(
trips.tripduration,
trips.start_station_id,
trips.start_station_name,
trips.end_station_id,
trips.end_station_name,
trips.bikeid,
trips.membership_type,
trips.usertype,
trips.birth_year,
trips.gender);

-----------------------------------------------------------------------

use schema consumption_zone;


create or replace table trips_male
(tripduration integer,
start_station_name string,
end_station_name string,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

create or replace table trips_female
(tripduration integer,
start_station_name string,
end_station_name string,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);


create or replace task consumption_female_tsk
warehouse = compute_wh
schedule = '1 minute'
when
    system$stream_has_data('testdb.curated_zone.curated_trips_stm')
as
merge into testdb.consumption_zone.trips_female trips_female using
testdb.curated_zone.curated_trips_stm trips on
trips_female.bikeid = trips.bikeid
when matched
then update set
trips_female.tripduration = trips.tripduration,
trips_female.start_station_name = trips.start_station_name,
trips_female.end_station_name = trips.end_station_name,
trips_female.bikeid = trips.bikeid,
trips_female.membership_type = trips.membership_type,
trips_female.usertype = trips.usertype,
trips_female.birth_year = trips.birth_year,
trips_female.gender = trips.gender
when not matched then
insert (
trips_female.tripduration,
trips_female.start_station_name,
trips_female.end_station_name,
trips_female.bikeid,
trips_female.membership_type,
trips_female.usertype,
trips_female.birth_year,
trips_female.gender)
values(
trips.tripduration,
trips.start_station_name,
trips.end_station_name,
trips.bikeid,
trips.membership_type,
trips.usertype,
trips.birth_year,
trips.gender);









  

  






