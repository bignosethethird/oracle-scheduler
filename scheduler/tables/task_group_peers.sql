------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.TASK_GROUP_PEERS
--
-- This file was generated from database instance APP01.
--   Database Time    : 09SEP2018 12:04:27
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @task_group_peers.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.TASK_GROUP_PEERS

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
     and object_name = upper('TASK_GROUP_PEERS');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.TASK_GROUP_PEERS already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.TASK_GROUP_PEERS';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.TASK_GROUP_PEERS cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.TASK_GROUP_PEERS
(
  GROUP_PEER1                     VARCHAR2  (20)
, GROUP_PEER2                     VARCHAR2  (20)
, CHANGE_REASON                   VARCHAR2  (1000)
)
tablespace SCHED_DATA_SMALL
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.TASK_GROUP_PEERS is
  'Peered relationships between task groups';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.TASK_GROUP_PEERS.GROUP_PEER1 is
  'Group name of peer 1';
comment on column SCHEDULER.TASK_GROUP_PEERS.GROUP_PEER2 is
  'Group name of peer 1';
comment on column SCHEDULER.TASK_GROUP_PEERS.CHANGE_REASON is
  'Reason for change ';
 
------------------------------------------------------------------------------
-- Create/Recreate indexes 
------------------------------------------------------------------------------
create index SCHEDULER.IX_GROUP_PEER1 on SCHEDULER.TASK_GROUP_PEERS(GROUP_PEER1)
  tablespace SCHED_IDX_SMALL
;
create index SCHEDULER.IX_GROUP_PEER2 on SCHEDULER.TASK_GROUP_PEERS(GROUP_PEER2)
  tablespace SCHED_IDX_SMALL
;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

