------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Data population script for table scheduler.special_days.
-- WARNING: *** This script overwrites the entire table! ***
--          *** Save important content before running.   ***
-- To run this script from the command line:
--   $ sqlplus "scheduler/[password]@[instance]" @special_days.sql
-- This file was generated from database instance ABC.
--   Database Time    : 28FEB2018 17:04:53
--   IP address       : 192.5.20.64
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : ahl64
--   O/S user         : vcr
------------------------------------------------------------------------------
set feedback off;
prompt Populating 40 records into table scheduler.special_days.

-- Truncate the table:
truncate table scheduler.special_days;

------------------------------------------------------------------------------
-- Populating the table:
------------------------------------------------------------------------------

insert into scheduler.special_days
  (DAY,DESCRIPTION)
  values
  (to_date('20200101000000','YYYYMMDDHH24MISS'),'Bank holiday');

-- etc...

commit;
set feedback on;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

