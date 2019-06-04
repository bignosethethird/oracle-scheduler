------------------------------------------------------------------------------
------------------------------------------------------------------------------
--  Database shutdown trigger
--
--  USAGE:
--  $ sqlplus "sys/[password] as sysdba" @scheduler_shutdown.sql
------------------------------------------------------------------------------
prompt Database shutdown trigger to kill the Scheduler FSM
create or replace trigger sys.scheduler_shutdown before shutdown on database
begin
  scheduler.sched.kill_fsm;
end;
/

------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------
