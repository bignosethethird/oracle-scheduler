------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Creation script for object SYS.SCHEDULER_SYS
--
-- This file was generated from database instance APP01.
--   Database Time    : 11OCT2005 18:38:08
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SYS/[password]@[instance] @scheduler_sys.sql
------------------------------------------------------------------------------
set serveroutput on size 1000000 feedback off
prompt Creating role SCHEDULER_SYS

-- Drop role if already exists
declare
  v_count integer:=0;
begin
  select count(*)
    into v_count
    from sys.dba_roles
   where role = upper('SCHEDULER_SYS');
  if(v_count>0)then
    dbms_output.put_line('Role SCHEDULER_SYS already exists');
  else
    execute immediate 'create role SCHEDULER_SYS not identified';
  end if;
exception
  when others then
    dbms_output.put_line('WARNING: Could not create role SCHEDULER_SYS. '||sqlerrm);
end;
/
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

