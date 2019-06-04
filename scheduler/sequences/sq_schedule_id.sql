------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Creation script for sequence SCHEDULER.SQ_SCHEDULE_ID
--
-- This file was generated from database instance ABC.
--   Database Time    : 28FEB2018 17:10:06
--   IP address       : 192.5.20.64
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : ahl64
--   O/S user         : vcr
-- To run this script from the command line:
-- sqlplus UTL/[password]@[instance] @sq_schedule_id.sql
------------------------------------------------------------------------------
set feedback off;
prompt Creating sequence SCHEDULER.SQ_SCHEDULE_ID

-- Drop type if it already exists
-- Note that the contents of the table will also be deleted.
declare 
  v_count integer:=0;
begin
  select count(*)
    into v_count
    from sys.all_objects
   where object_type = 'SEQUENCE'
     and owner = upper('SCHEDULER')
     and object_name = upper('SQ_SCHEDULE_ID');
  if(v_count>0)then
    execute immediate 'drop sequence SCHEDULER.SQ_SCHEDULE_ID';
  end if;
end;
/
------------------------------------------------------------------------------
-- Create sequence
------------------------------------------------------------------------------

create sequence SCHEDULER.SQ_SCHEDULE_ID
  minvalue 1 
  maxvalue 999999999999999999999999999
  increment by 1
  nocycle
  cache 20
  noorder
;

------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

