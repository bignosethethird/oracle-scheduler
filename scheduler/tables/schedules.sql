------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Table creation script for table SCHEDULER.SCHEDULES
--
-- This file was generated from database instance APP01.
--   Database Time    : 08SEP2018 17:17:46
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @schedules.sql
------------------------------------------------------------------------------
set feedback off;
set serveroutput on size 1000000;
prompt Creating table SCHEDULER.SCHEDULES

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
     and object_name = upper('SCHEDULES');
  if(v_count>0)then
    dbms_output.put_line('Table SCHEDULER.SCHEDULES already exists. Dropping it');
    execute immediate 'drop table SCHEDULER.SCHEDULES';
  end if;
exception
  when others then
    if(v_count>0)then
      dbms_output.put_line('and dropping referential constraints to it');
      execute immediate 'drop table SCHEDULER.SCHEDULES cascade constraints';
    end if;
end;
/
------------------------------------------------------------------------------
-- Create table
------------------------------------------------------------------------------
create table SCHEDULER.SCHEDULES
(
  TASK_ID                         NUMBER    (22) not null
, SUBMITTED_BY                    VARCHAR2  (30)
, TASK_TYPE                       VARCHAR2  (12) default 'DURABLE'

, GROUP_NAME                      VARCHAR2  (20) default 'DEFAULT'

, OPERATION_ID                    NUMBER    (22)
, COMMAND                         VARCHAR2  (500)
, COMMAND_TYPE                    VARCHAR2  (20) default 'SQL'

, DESCRIPTION                     VARCHAR2  (100)
, DEPENDENCIES                    VARCHAR2  (100)
, MAX_WAITTIME                    NUMBER    (22)
, MAX_RUNTIME                     NUMBER    (22)
, QUEUE_ID                        NUMBER    (22)
, PROCESS_ID                      NUMBER    (22)
, RETURN_CODE                     NUMBER    (22)
, STATE                           VARCHAR2  (10) default 'INITIAL'

, STATE_TMSTMP                    DATE      
, PREV_STATE                      VARCHAR2  (10)
, STARTED_AT                      DATE      
, FINISHED_AT                     DATE      
, YEAR                            VARCHAR2  (4)
, MONTH                           VARCHAR2  (2)
, DAY                             VARCHAR2  (3)
, HOUR                            VARCHAR2  (3)
, MINUTE                          VARCHAR2  (3)
, WEEKDAYS                        VARCHAR2  (7)
, SPECIAL_DAYS                    VARCHAR2  (7)
, NEXT_DUE_DATE                   DATE      
, REPEATS                         NUMBER    (22)
, REPEAT_INTERVAL                 NUMBER    (22)
, REPEAT_COUNT                    NUMBER    (22) default 0

, EFFECTIVE_DATE_OFFSET           NUMBER    (22)
, MODAL                           VARCHAR2  (1) default 'N'

, IGNORE_ERROR                    VARCHAR2  (1) default 'N'

, DEPENDENCY_SQL                  VARCHAR2  (2000)
, REPEAT_PERIODIC                 VARCHAR2  (1) default 'Y'

, CHANGE_REASON                   VARCHAR2  (1000)
, TASK_PRIORITY                   NUMBER     default 0

)
tablespace SCHED_DATA_SMALL
;
 
------------------------------------------------------------------------------
-- Table comment:
------------------------------------------------------------------------------
comment on table SCHEDULER.SCHEDULES is
  'Schedule all batch jobs from here';
 
------------------------------------------------------------------------------
-- Column comments:
------------------------------------------------------------------------------
comment on column SCHEDULER.SCHEDULES.TASK_ID is
  'Primary key: Batch Job item Id.';
comment on column SCHEDULER.SCHEDULES.SUBMITTED_BY is
  'Logged-in Oracle user who submitted this task or who last attempted to modify the task';
comment on column SCHEDULER.SCHEDULES.TASK_TYPE is
  'Type of task: DURABLE (default), VOLATILE, TIMECRITICAL and PERSISTENT';
comment on column SCHEDULER.SCHEDULES.GROUP_NAME is
  'Name of a group of operations (tasks). The grouping of operations (tasks) is optional';
comment on column SCHEDULER.SCHEDULES.OPERATION_ID is
  'Notional and optional Operation Id for a particular task. Other tasks cannot be made dependent on this task if it does not have an Operation Id. The Operation Id does not need to be unique across a schedule nor across the task group.';
comment on column SCHEDULER.SCHEDULES.COMMAND is
  'The SQL code or stored procedure to be executed. Fixed parameters can be passed to the procedure. Dynamic parameters that the scheduler calculates are indicated in angle brackets and substituted with the actual value before the procedure is launched.';
comment on column SCHEDULER.SCHEDULES.COMMAND_TYPE is
  'Determines how the command code needs to be wrapped, i.e. SQL, FUNCTION, PROCEDURE, or EXTERNAL_PROC';
comment on column SCHEDULER.SCHEDULES.DESCRIPTION is
  'Description of what this task does';
comment on column SCHEDULER.SCHEDULES.DEPENDENCIES is
  'Logical dependency statement of operations in a program-like notation. At least one logical path of the dependencies needs to be completed for this task to be considered for execution. There are special cases where a task may not have any dependencies.';
comment on column SCHEDULER.SCHEDULES.MAX_WAITTIME is
  'Time to wait (in minutes) for predecessor jobs to complete before aborting. Null = wait forever.';
comment on column SCHEDULER.SCHEDULES.MAX_RUNTIME is
  'Max allowable task runtime minutes. If NULL, then there no max run time. This value may not be defined when the task is a REPEATING task';
comment on column SCHEDULER.SCHEDULES.QUEUE_ID is
  'Most recent Job queue Id that DBMS_JOB assigned to this task';
comment on column SCHEDULER.SCHEDULES.PROCESS_ID is
  'Most recent Process ID that the operating system assigned to this task. Only used in O/S calls';
comment on column SCHEDULER.SCHEDULES.RETURN_CODE is
  'Return code of the command that was called';
comment on column SCHEDULER.SCHEDULES.STATE is
  'The current state of this job';
comment on column SCHEDULER.SCHEDULES.STATE_TMSTMP is
  'Timestamp of when state was changed.';
comment on column SCHEDULER.SCHEDULES.PREV_STATE is
  'The previous state of this job. 0:READY,1:DUE,2:EXECUTING,3:SUSPENDED,4:RESUMED,8:COMPLETED,9:DISABLED,10:TIMEDOUT';
comment on column SCHEDULER.SCHEDULES.STARTED_AT is
  'Timestamp of when this job was started.';
comment on column SCHEDULER.SCHEDULES.FINISHED_AT is
  'Timestamp of when this job completed. Null if still busy';
comment on column SCHEDULER.SCHEDULES.YEAR is
  'Year in which this job is scheduled';
comment on column SCHEDULER.SCHEDULES.MONTH is
  'Month in which this job is scheduled';
comment on column SCHEDULER.SCHEDULES.DAY is
  'Day of month in which this job is scheduled. When it is a negative number, the days will be count backwards from the last day of the month.';
comment on column SCHEDULER.SCHEDULES.HOUR is
  'Hour of day in which this job is scheduled';
comment on column SCHEDULER.SCHEDULES.MINUTE is
  'Minute of Hour in which this job is scheduled';
comment on column SCHEDULER.SCHEDULES.WEEKDAYS is
  'Days in week when this job is scheduled, 7=SUNDAY,1=MONDAY, etc. e.g. 1..45.7';
comment on column SCHEDULER.SCHEDULES.SPECIAL_DAYS is
  'Either INCLUDE or EXCLUDE days specified in table TB_SPECIAL_DAYS';
comment on column SCHEDULER.SCHEDULES.NEXT_DUE_DATE is
  'Next date that this task is due for execution.';
comment on column SCHEDULER.SCHEDULES.REPEATS is
  'Number of times to repeat this task';
comment on column SCHEDULER.SCHEDULES.REPEAT_INTERVAL is
  'Interval in minutes over which to repeat this task';
comment on column SCHEDULER.SCHEDULES.REPEAT_COUNT is
  'Number of times a repeating task has executed so far';
comment on column SCHEDULER.SCHEDULES.EFFECTIVE_DATE_OFFSET is
  'Effective date to pass to the task as a function of the number of days prior to NEXT_DUE_DATE. When this value is defined, it is passed as a parameter to the procedure specified in the command field. The parameter is indicated as <EffectiveDate>.';
comment on column SCHEDULER.SCHEDULES.MODAL is
  'A modal task can only execute if no other task tree is executing. While a modal task is executing, no other task - modal or non-modal - may execute.';
comment on column SCHEDULER.SCHEDULES.IGNORE_ERROR is
  'Ignore non-zero return code from tasks';
comment on column SCHEDULER.SCHEDULES.DEPENDENCY_SQL is
  'Generated SQL code that matches the dependency expression';
comment on column SCHEDULER.SCHEDULES.REPEAT_PERIODIC is
  'The next task retry is based on REPEAT_INTERVAL and on the NEXT_DUE_DATE rather than on the FINISHED_AT date of the previous attempt.';
comment on column SCHEDULER.SCHEDULES.CHANGE_REASON is
  'Reason for last last to this task';
comment on column SCHEDULER.SCHEDULES.TASK_PRIORITY is
  'Prioritization value: The higher the weighting the higher the prioritization.';
 
------------------------------------------------------------------------------
-- Create/Recreate primary key constraints
------------------------------------------------------------------------------
alter table SCHEDULER.SCHEDULES
  add constraint PK_SCHEDULE_TASK_ID
  primary key (TASK_ID)
  using index
  tablespace SCHED_IDX_SMALL
;
 
------------------------------------------------------------------------------
-- Create/Recreate indexes 
------------------------------------------------------------------------------
create index SCHEDULER.IX_DEPENDENCIES on SCHEDULER.SCHEDULES(DEPENDENCIES)
  tablespace SCHED_IDX_SMALL
;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

