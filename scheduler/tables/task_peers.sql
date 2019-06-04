------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.TASK_PEERS
--
-- This file was generated from database instance APP01.
--   Database Time    : 09SEP2018 12:04:28
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @task_peers.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.TASK_PEERS

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
     and object_name = upper('TASK_PEERS');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.TASK_PEERS already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.TASK_PEERS';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.TASK_PEERS cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.TASK_PEERS
(
  TASK_PEER1                      NUMBER    
, TASK_PEER2                      NUMBER    
, CHANGE_REASON                   VARCHAR2  (1000)
)
tablespace SCHED_DATA_SMALL
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.TASK_PEERS is
  'Peered relationships between tasks';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.TASK_PEERS.TASK_PEER1 is
  'Task Id of peer task';
comment on column SCHEDULER.TASK_PEERS.TASK_PEER2 is
  'Task Id of peer task';
comment on column SCHEDULER.TASK_PEERS.CHANGE_REASON is
  'Reason for change';
 
------------------------------------------------------------------------------
-- Create/Recreate indexes 
------------------------------------------------------------------------------
create index SCHEDULER.IX_TASK_PEER1 on SCHEDULER.TASK_PEERS(TASK_PEER1)
  tablespace SCHED_IDX_SMALL
;
create index SCHEDULER.IX_TASK_PEER2 on SCHEDULER.TASK_PEERS(TASK_PEER2)
  tablespace SCHED_IDX_SMALL
;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

