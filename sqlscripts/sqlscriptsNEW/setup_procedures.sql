use database testdb;
use schema control_check;

create or replace procedure incr_seq(seq_name varchar)
returns varchar 
language sql
as
declare
    tbl_nm varchar := seq_name; 
begin
    EXECUTE IMMEDIATE 'update control_check.' || tbl_nm || ' set seq_num = completion_seq.nextval';
    return 'update complete';
end;