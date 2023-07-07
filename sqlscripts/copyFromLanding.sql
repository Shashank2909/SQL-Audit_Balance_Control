use database testdb;
use schema curated_zone;


merge into testdb.curated_zone.trips_curated trips_curated using
testdb.landing_zone.trips trips on
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