------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.SPECIAL_DAYS
--
-- This file was generated from database instance APP01.
--   Database Time    : 23AUG2018 11:39:34
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @special_days.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.SPECIAL_DAYS

-- Drop table if it already exists
-- Note that the contents of the table will also be deleted
--  and that referential constraints will also be dropped.
-- You will be warned when this happens.
declare 
  v_count integer:=0;
begin
  select count(*)
    into v_count
    from sys.all_objects
   where object_type = 'TABLE'
     and owner = upper('SCHEDULER')
     and object_name = upper('SPECIAL_DAYS');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.SPECIAL_DAYS already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.SPECIAL_DAYS';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.SPECIAL_DAYS cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.SPECIAL_DAYS
(
  DAY                             DATE       not null
, DESCRIPTION                     VARCHAR2  (100)
)
tablespace SCHED_DATA_SMALL
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.SPECIAL_DAYS is
  'List of Special Days that the scheduler should either include or exclude';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.SPECIAL_DAYS.DAY is
  'Date of special day';
comment on column SCHEDULER.SPECIAL_DAYS.DESCRIPTION is
  'Description of this day';
 
------------------------------------------------------------------------------
-- Create/Recreate primary key constraints
------------------------------------------------------------------------------
alter table SCHEDULER.SPECIAL_DAYS
  add constraint PK_SPECIAL_DAYS
  primary key (DAY)
  using index
  tablespace SCHED_IDX_SMALL
;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

