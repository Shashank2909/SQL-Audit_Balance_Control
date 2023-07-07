create or replace table consumption_zone.stationwise_daily_rides
(START_STATION_ID integer,
 START_STATION_NAME varchar,
 END_STATION_ID integer,
 END_STATION_NAME varchar,
 RIDE_DATE DATE,
 RIDE_COUNT integer
);