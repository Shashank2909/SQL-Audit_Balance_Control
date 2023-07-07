use database testdb;
use schema consumption_zone;



merge into testdb.consumption_zone.trips_female trips_female using
testdb.curated_zone.trips_curated trips on
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