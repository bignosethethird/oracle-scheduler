------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Creation script for view SCHEDULER.VW_FSM_TASKS
--
-- This file was generated from database instance APP01.
--   Database Time    : 09SEP2018 13:53:08
--   IP address       : 10.44.0.228
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : misqux42
--   O/S user         : abc
-- To run this script from the command line:
-- sqlplus SCHEDULER/[password]@[instance] @vw_fsm_tasks.sql
------------------------------------------------------------------------------
set feedback off;
prompt Creating view SCHEDULER.VW_FSM_TASKS

------------------------------------------------------------------------------
-- Create view
--
-- Note: If this view does not build then it may be because the objects
--       referred to are not fully qualified. The best place to fix is in the
--       source database and to regenerate this script.
------------------------------------------------------------------------------

create or replace view SCHEDULER.VW_FSM_TASKS
as
select   s.*,
         g.group_priority
    from scheduler.schedules s
    left join scheduler.task_groups g on s.group_name = g.group_name
;
/

------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

