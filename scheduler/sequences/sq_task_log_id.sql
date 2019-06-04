------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Creation script for sequence SCHEDULER.SQ_TASK_LOG_ID
--
-- This file was generated from database instance ABC.
--   Database Time    : 28FEB2018 17:10:07
--   IP address       : 192.5.20.64
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : ahl64
--   O/S user         : vcr
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @sq_task_log_id.sql
------------------------------------------------------------------------------
set feedback off;
prompt Creating sequence scheduler.sq_task_log_id

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
     and object_name = upper('SQ_TASK_LOG_ID');
  if(v_count>0)then
    execute immediate 'drop sequence scheduler.sq_task_log_id';
  end if;
end;
/
------------------------------------------------------------------------------
-- Create sequence
------------------------------------------------------------------------------

create sequence scheduler.sq_task_log_id
minvalue 1
maxvalue 999999999999999999999999999
start with 1
increment by 1
cache 20;


------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

