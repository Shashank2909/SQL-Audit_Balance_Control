use ABC;
truncate table landing_zone.trips;
truncate table curated_zone.stations;
truncate table curated_zone.trips_fact;
truncate table consumption_zone.stationwise_daily_rides;

drop table landing_zone.trips;
drop table curated_zone.stations;
drop table curated_zone.trips_fact;
drop table consumption_zone.stationwise_daily_rides;