use database testdb;
use schema landing_zone;

copy into trips from @citibike_trips
file_format = CSV PATTERN = '.*1_0_0*.csv.*'
size_limit = 1000;