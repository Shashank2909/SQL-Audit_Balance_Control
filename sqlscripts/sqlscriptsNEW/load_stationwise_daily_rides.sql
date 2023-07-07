insert into consumption_zone.stationwise_daily_rides
select f.START_STATION_ID, ds.station_name, f.END_STATION_ID, de.station_name, date(f.STARTTIME), count(1)  
from "TESTDB"."CURATED_ZONE"."TRIPS_FACT" f
join "TESTDB"."CURATED_ZONE"."STATIONS" ds
on f.START_STATION_ID=ds.STATION_ID
join "TESTDB"."CURATED_ZONE"."STATIONS" de
on f.end_STATION_ID=de.STATION_ID
group by f.START_STATION_ID, ds.station_name, f.END_STATION_ID, de.station_name, date(f.STARTTIME);

use schema control_check;
update control_check.etl_completion_seq set seq_num = completion_seq.nextval;