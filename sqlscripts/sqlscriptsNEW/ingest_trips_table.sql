copy into landing_zone.trips from @landing_zone.unload_stage
file_format = landing_zone.abc_csv_ff;

use schema control_check;
update control_check.start_seq set seq_num = start_seq.nextval;