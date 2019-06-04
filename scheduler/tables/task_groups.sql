------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.TASK_GROUPS
--
-- This file was generated from database instance APP01.
--   Database Time    : 09SEP2018 12:58:22
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @task_groups.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.TASK_GROUPS

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
     and object_name = upper('TASK_GROUPS');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.TASK_GROUPS already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.TASK_GROUPS';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.TASK_GROUPS cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.TASK_GROUPS
(
  GROUP_NAME                      VARCHAR2  (20)
, GROUP_PRIORITY                  NUMBER     default 0
, CHANGE_REASON                   VARCHAR2  (1000)
)
tablespace SCHED_DATA_SMALL
  pctfree 10
  initrans 1
  maxtrans 255
  storage(
    initial 128K
    next 128K
    minextents 1
    maxextents unlimited
    pctincrease 0
  )
  logging
  parallel
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.TASK_GROUPS is
  'Task Group definitions';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.TASK_GROUPS.GROUP_NAME is
  'Name of a group of tasks';
comment on column SCHEDULER.TASK_GROUPS.GROUP_PRIORITY is
  'Prioritization value: The higher the weighting the higher the prioritization';
comment on column SCHEDULER.TASK_GROUPS.CHANGE_REASON is
  'Reason for change ';
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

