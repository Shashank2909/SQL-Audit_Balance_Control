merge into curated_zone.stations st using 
(select nvl(station_id,-1) as station_id, station_name
from
(select distinct END_STATION_ID as station_id, END_STATION_NAME as station_name from landing_zone.trips
union
select distinct START_STATION_ID as station_id, START_STATION_NAME as station_name from landing_zone.trips)) src
on st.station_id=src.station_id and st.station_name=src.station_name
when matched then 
update set
st.station_name=src.station_name,
st.updated_date=current_timestamp()
when not matched then
insert (st.station_id,st.station_name,st.created_date)
values (src.station_id,src.station_name,current_timestamp());