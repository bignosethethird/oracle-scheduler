------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.TASK_LOG
--
-- This file was generated from database instance APP01.
--   Database Time    : 23AUG2018 11:39:35
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @task_log.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.TASK_LOG

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
     and object_name = upper('TASK_LOG');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.TASK_LOG already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.TASK_LOG';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.TASK_LOG cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.TASK_LOG
(
  ID                              NUMBER    (22) not null
, TASK_ID                         NUMBER    (22)
, STATE                           VARCHAR2  (20)
, EXECUTION_ID                    NUMBER    (22)
, REPEAT_COUNT                    NUMBER    (22)
, SCHEDULED_TIME                  DATE      
, STARTED_AT                      DATE      
, ENDED_AT                        DATE      
, QUEUE_ID                        NUMBER    (22)
, PROCESS_ID                      NUMBER    (22)
, RETURN_CODE                     NUMBER    (22)
, MODAL                           VARCHAR2  (20)
, WHAT                            VARCHAR2  (4000)
)
tablespace SCHED_DATA_SMALL
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.TASK_LOG is
  'An entry is made every time that a task is run.';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.TASK_LOG.ID is
  'Task event Id';
comment on column SCHEDULER.TASK_LOG.TASK_ID is
  'Task id in schedule';
comment on column SCHEDULER.TASK_LOG.STATE is
  'New task state';
comment on column SCHEDULER.TASK_LOG.EXECUTION_ID is
  'Task execution Id';
comment on column SCHEDULER.TASK_LOG.REPEAT_COUNT is
  'The Nth time that this task has repeated';
comment on column SCHEDULER.TASK_LOG.SCHEDULED_TIME is
  'Time that this this task run was actually scheduled for. This is referred to by some as a cycle identifyer.';
comment on column SCHEDULER.TASK_LOG.STARTED_AT is
  'Time of launching the task';
comment on column SCHEDULER.TASK_LOG.ENDED_AT is
  'Time of perceived completion of the task';
comment on column SCHEDULER.TASK_LOG.QUEUE_ID is
  'DBMS_JOB Queue Id';
comment on column SCHEDULER.TASK_LOG.PROCESS_ID is
  'O/S Process Id. On used for O/S SHELL tasks';
comment on column SCHEDULER.TASK_LOG.RETURN_CODE is
  'Return code after execution';
comment on column SCHEDULER.TASK_LOG.MODAL is
  'Modal task lifecycle';
comment on column SCHEDULER.TASK_LOG.WHAT is
  'Actual code that was submitted to DBMS_JOB';
 
------------------------------------------------------------------------------
-- Create/Recreate indexes 
------------------------------------------------------------------------------
create index SCHEDULER.IX_TASK_LOG_EXEC_ID on SCHEDULER.TASK_LOG(EXECUTION_ID)
  tablespace SCHED_IDX_SMALL
;
create index SCHEDULER.IX_TASK_LOG_ID on SCHEDULER.TASK_LOG(ID)
  tablespace SCHED_IDX_SMALL
;
create index SCHEDULER.IX_TASK_LOG_TASK_ID on SCHEDULER.TASK_LOG(TASK_ID)
  tablespace SCHED_IDX_SMALL
;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

