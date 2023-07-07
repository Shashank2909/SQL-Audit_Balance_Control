use database testdb;
create or replace stage landing_zone.abc_stage
url = 's3://snowflake-workshop-lab/citibike-trips-csv/';
--s3://snowflake-workshop-lab/citibike-trips-csv/
-- truncate table landing_zone.trips_org ;
copy into landing_zone.trips_org from @landing_zone.abc_stage
file_format = landing_zone.abc_csv_ff;

desc table trips_org;

select count(*) from trips_org;
select count(*) from trips;
select * from trips_org limit 100;

list @abc_stage;

create or replace file format landing_zone.abc_csv_ff  type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

show stages;
list @unload_stage;

-- drop stage landing_zone.abc_stage;
-- drop file format landing_zone.abc_csv_ff;



create storage integration s3_int
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::740295434790:role/snowflakeAccess'
storage_allowed_locations = ('s3://snowtripsdata/unload/');

desc storage integration s3_int;

create stage unload_stage
storage_integration= s3_int
url = 's3://snowtripsdata/unload/'
file_format= ABC_CSV_FF;

show stages;

copy into @unload_stage/ul from (select * from trips_org order by bikeid limit 10000)
file_format=(format_name=ABC_CSV_FF,compression='None')
overwrite=True
MAX_FILE_SIZE = 5000000; --5MB MINIMUM

--------------------------------------------------------------------------------

create database cntrlship_db;

use database testdb;
create schema control_check;

CREATE OR REPLACE SEQUENCE start_seq START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE completion_seq START = 1 INCREMENT = 1;
create or replace table control_check.start_seq (seq_num int);
create or replace table control_check.etl_completion_seq (seq_num int);

insert into control_check.start_seq values(start_seq.nextval);
insert into control_check.etl_completion_seq values(completion_seq.nextval);


select (select seq_num from control_check.start_seq)-(select seq_num from control_check.etl_completion_seq) as diff;
-- drop table etl_completion_seq;


select seq_num from control_check.start_seq; --3
select seq_num from control_check.etl_completion_seq; --2 