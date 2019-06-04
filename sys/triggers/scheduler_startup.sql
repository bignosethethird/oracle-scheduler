------------------------------------------------------------------------------
------------------------------------------------------------------------------
--  Database startup trigger
--
--  USAGE:
--  $ sqlplus "sys/[password] as sysdba" @scheduler_startup.sql
------------------------------------------------------------------------------
prompt Database startup trigger to initialise the FSM
create or replace trigger sys.scheduler_startup after startup on database
begin
  scheduler.sched.init_fsm;
end;
/

------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------
