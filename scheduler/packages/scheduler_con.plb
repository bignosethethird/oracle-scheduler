create or replace package body scheduler.scheduler_con
as
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- CONSOLE to the Task Scheduler
-- Functions return on Success and non-0 on an error
--
-- The implementatio approach is that these console functions perform as much
-- as possible so that a wrapper shell script would be absolutely minimal.
--
-- Example shell script that calls a parameterised PLSQL function [task.reset]:
-- #!/usr/bin/ksh
-- [[ -z $1 ]] ampersant ampersant echo "Usage: $0 TaskId"
-- sqlplus -s / <<!
-- set feedback off
-- set serveroutput on size 100000
-- var i number
-- exec :i:=scheduler.scheduler_con.task_reset(${1:-null});
-- quit :i
-- !
-- exit $?
--
-- Note: Return codes generated from Oracle are modded with 256. 
--       The result is that values greater than 255 are incorrectly returned.
-------------------------------------------------------------------------------
-- DO NOT "BEAUTIFY" THIS CODE!
-------------------------------------------------------------------------------

--===========================================================================--
-- DATA DUMP AND MONITORING FUNCTIONS
--===========================================================================--

-- Ouput usually very long error details to dbms_output in nice lines
-- Remember to set: SQL> set serveroutput on format wrapped
procedure dump_error_details(p_error_code in utl.error_codes.error_code%type)
is
  c_proc    constant varchar2(100)  := pc_schema||'.'||pc_package||'.dump_error_details';
  v_retcode utl.global.t_error_code;
  v_msg     utl.error_codes.message%type;
  v_exp     utl.error_codes.explanation%type;
  l_err     dbms_sql.varchar2s;
  v_lines   pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc,null);
  v_retcode:=utl.pkg_errorhandler.code2desc(p_error_code,v_msg, v_exp);
  if(v_retcode<>utl.pkg_exceptions.gc_undefined)then
    dbms_output.put_line('Error Message:');
    dbms_output.put_line('-------------');
    v_retcode:=utl.pkg_string.break_string(v_msg,l_err,v_lines,'',80); 
    for i in l_err.first..l_err.last loop
      dbms_output.put_line(l_err(i));
    end loop;  
    dbms_output.put_line('Explanation:');
    dbms_output.put_line('-----------');
    v_lines:=0;
    v_retcode:=utl.pkg_string.break_string(v_exp,l_err,v_lines,'',80);
    for i in l_err.first..l_err.last loop
      dbms_output.put_line(l_err(i));
    end loop;
  else
    raise utl.pkg_exceptions.e_undefined;
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;  
end dump_error_details;

-- Output all the schedules to the SQLPLUS console
-- Remember to set: SQL> set serveroutput on format wrapped
function scheduler_dump
return pls_integer
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.scheduler_dump';
  l_lines   dbms_sql.varchar2s;
  v_count   pls_integer;
  v_retcode utl.global.t_error_code;
  v_msg     varchar2(200);
  v_exp     varchar2(1000);
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_retcode:=utl.pkg_string.break_string(scheduler_rep.schedule_dump,l_lines,v_count,null,80);
  if(v_retcode<>utl.pkg_exceptions.gc_success)then
    utl.pkg_errorhandler.code2desc(v_retcode,v_msg, v_exp);
    dbms_output.put_line(v_msg);
    dbms_output.put_line(v_exp);
  else
    for i in l_lines.first..l_lines.last loop
      dbms_output.put_line(l_lines(i));
    end loop;
  end if;
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then        
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    v_retcode:=sqlcode;
    if(mod(v_retcode,256)=0)then
      return 1;
    else
      return mod(v_retcode,256);
    end if;    
end scheduler_dump;

-- Output all the schedules
function scheduler_show
return pls_integer
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.scheduler_show';
  l_lines   dbms_sql.varchar2s;
  v_count   pls_integer;
  v_retcode utl.global.t_error_code;
  v_msg     varchar2(200);
  v_exp     varchar2(1000);
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_retcode:=utl.pkg_string.break_string(scheduler_rep.schedule_show,l_lines,v_count,null,80);
  if(v_retcode<>utl.pkg_exceptions.gc_success)then
    utl.pkg_errorhandler.code2desc(v_retcode,v_msg, v_exp);
    dbms_output.put_line(v_msg);
    dbms_output.put_line(v_exp);
  else
    for i in l_lines.first..l_lines.last loop
      dbms_output.put_line(l_lines(i));
    end loop;
  end if;
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_show;

-- General Schedule query 
-- Select tasks that match the Description, state, group or command fields
-- with the search string
function scheduler_query(p_search in  schedules.description%type :=null) return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_query';
  l_lines   dbms_sql.varchar2s;
  v_count   pls_integer;
  v_retcode utl.global.t_error_code;
  v_msg     varchar2(200);
  v_exp     varchar2(1000);

begin
  dbms_application_info.set_module(c_proc,null);
  v_retcode:=utl.pkg_string.break_string(scheduler_rep.schedule_query(p_search),l_lines,v_count,null,80);
  
  if(v_retcode<>utl.pkg_exceptions.gc_success)then
    utl.pkg_errorhandler.code2desc(v_retcode,v_msg, v_exp);
    dbms_output.put_line(v_msg);
    dbms_output.put_line(v_exp);
  else
    for i in l_lines.first..l_lines.last loop
      dbms_output.put_line(l_lines(i));
    end loop;
  end if;
 
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_query;

-- Get a process log of last 100 events related to the scheduler
-- Remember to set: SQL> set serveroutput on format wrapped
function scheduler_errors return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_errors';
  v_retcode utl.global.t_error_code;
  l_lines   dbms_sql.varchar2s;
  v_lines   pls_integer:=0;  
  cursor c_log is 
    select *
      from (select t.*
              from utl.log_message t
             where t.program_name like 'scheduler.%'
               and t.message_type = 'ERROR'
               and t.error_code <> 0
             order by t.log_date desc 
            ) 
      where rownum<101
     order by log_date asc;
begin
  dbms_application_info.set_module(c_proc,null);
  for c in c_log loop
    dbms_output.put_line('Timestamp: '||to_char(c.log_date,gc_datetime_format));
    dbms_output.put_line('Error code:'||c.error_code);
    dbms_output.put_line('Procedure: '||c.program_name);
    v_lines:=0;
    v_retcode:=utl.pkg_string.break_string(c.message_text,l_lines,v_lines,'',80);
    for i in l_lines.first..l_lines.last loop
      dbms_output.put_line(l_lines(i));
    end loop;  
  end loop;
  dbms_application_info.set_module(null,null);
  return 0;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_errors;

-- Shows last 100 task launches
function scheduler_launches return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_launches';
  cursor c_log is 
    select *
      from (select t.*
              from task_log t
             where t.state = 'EXECUTING'
             order by t.execution_id desc  
           ) 
     where rownum<101
     order by execution_id asc;
  v_line  varchar2(250);      
begin
  dbms_application_info.set_module(c_proc,null);
  for c in c_log loop
    v_line:='Launched:'||to_char(c.started_at,gc_datetime_format)||' TaskId:'||lpad(c.task_id,4)||' Repeat:'||nvl(to_char(c.repeat_count),' ')||' ExitCode:'||c.return_code;    
    dbms_output.put_line(v_line);  
  end loop;
  dbms_application_info.set_module(null,null);
  return 0;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_launches;

--===========================================================================--
-- SCHEDULER CONTROL
--===========================================================================--

-------------------------------------------------------------------------------
-- Scheduler Startup, shutdown, abort, resume etc...
-- Thows:   * Illegal botton click
function scheduler_startup return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_startup';
  v_status varchar2(30):=sched.status;
begin
  dbms_application_info.set_module(c_proc,null);
  if(v_status=sched.gc_mode_QUIESCED or v_status=sched.gc_mode_ABORTED)then
    dbms_output.put_line('Starting the scheduler up...');
    sched.startup;
    v_status:=sched.status;
    dbms_output.put_line('Current Scheduler Status: '||v_status);
    if(v_status=sched.gc_mode_UNQUIESCED)then
      dbms_application_info.set_module(null,null);
      return 0;
    end if;
  else
    dbms_output.put_line('Cannot start the scheduler up. Current Scheduler Status: '||v_status);
  end if;
  dbms_application_info.set_module(null,null);
  return 1;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_startup;

function scheduler_shutdown return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_shutdown';
  v_status varchar2(30):=sched.status;
  v_count  pls_integer;
begin
  dbms_application_info.set_module(c_proc,null);
  if(v_status=sched.gc_mode_UNQUIESCED)then
    dbms_output.put_line('Shutting the scheduler down...');
    sched.shutdown;
    v_status:=sched.status;
    dbms_output.put_line('Current Scheduler Status: '||v_status);
    if(v_status=sched.gc_mode_QUIESCED)then
      select count(*)
        into v_count
        from schedules
       where state=sched.gc_state_EXECUTING;
      if(v_count>0)then
        dbms_output.put_line(v_count||' tasks are still completing. Check the scheduler status later again.');
      else
        dbms_application_info.set_module(null,null);
        return 0;
      end if;
    end if;
  else
    dbms_output.put_line('Cannot shut the scheduler down. Current Scheduler Status: '||v_status);
  end if;
  dbms_application_info.set_module(null,null);
  return 1;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_shutdown;

-------------------------------------------------------------------------------
-- Scheduler command and control
--
-- Returns the following and should be displayed with the corresponding colour.
-- Only the Buttons shown should be enabled for a state.
-- 'RUNNING'            Green         Abort, Suspend, Shutdown
-- 'SHUTTING DOWN';     Orange        Abort
-- 'SHUT DOWN';         Red           Startup
-- 'SUSPENDED';         Yellow        Resume, Abort
-- 'ABORTING';          Orange        (None)
-- 'ABORTED';           Red           Startup
-- 'UNKNOWN';           Blue          Startup
--
-- Called:    * This function should regularly be called while the scheduler
--              control screen is being viewed
--            * When the REFRESH BUTTON is clicked (even though it relates to
--              the scheduler log)
function scheduler_status return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_status';
begin
  dbms_application_info.set_module(c_proc,null);
  dbms_output.put_line(sched.status);
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_status;

function scheduler_resume return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_resume';
  v_status varchar2(30):=sched.status;
begin
  dbms_application_info.set_module(c_proc,null);
  if(v_status=sched.gc_mode_SUSPENDED)then
    dbms_output.put_line('Resuming Scheduler');
    sched.resume;
    dbms_application_info.set_module(null,null);
    return 0;
  else
    dbms_output.put_line('Scheduler has not been suspended');
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_resume;

function scheduler_suspend return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_suspend';
  v_status varchar2(30):=sched.status;
begin
  dbms_application_info.set_module(c_proc,null);
  if(v_status=sched.gc_mode_UNQUIESCED)then
    sched.suspend;
    dbms_application_info.set_module(null,null);
    return 0;
  else
    dbms_output.put_line('Suspend request ignored - the current scheduler status is '||v_status);
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_suspend;

function scheduler_abort return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_abort';
  v_status        varchar2(30):=sched.status;
  v_count         pls_integer;
begin
  dbms_application_info.set_module(c_proc,null);
  if(v_status<>sched.gc_mode_ABORTING and v_status<>sched.gc_mode_ABORTED)then
    dbms_output.put_line('Aborting scheduler.');
    select count(*)
      into v_count
      from schedules
     where state=sched.gc_state_EXECUTING;
    if(v_count>0)then
      dbms_output.put_line('Aborting '||v_count||' task(s). This may take a while...');
    end if;
    sched.abort;
    select count(*)
      into v_count
      from schedules
     where state=sched.gc_state_EXECUTING;
    if(v_count>0)then
      dbms_output.put_line('There are still '||v_count||' task(s) that have not aborted.');
      dbms_output.put_line('They may need to be manually killed.');
      dbms_application_info.set_module(null,null);
      return 1;
    else
      dbms_output.put_line('Scheduler Abort complete.');
      dbms_application_info.set_module(null,null);
      return 0;
    end if;
  else
    dbms_output.put_line('Abort request ignored - the current scheduler status is '||v_status);
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_abort;

function scheduler_kill  return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_kill';
  v_count   pls_integer;
begin
  dbms_application_info.set_module(c_proc,null);
  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Killing the Scheduler FSM daemon');
  scheduler.sched.kill_fsm;
  select count(*)
    into v_count
    from sys.user_jobs
   where upper(what) like upper('%.fsm%');
  if(v_count=0)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Scheduler FSM daemon has been killed.');
    dbms_application_info.set_module(null,null);
    return 0;
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Could not kill Scheduler FSM daemon. You could try to manually remove ths FSM job from the DBMS_JOB queue.');
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;

end scheduler_kill;

function scheduler_init return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.scheduler_init';
  v_count   pls_integer;
  v_scan_interval pls_integer:=utl.pkg_config.get_variable_int(sched.gc_config_scan_int_key);
begin
  dbms_application_info.set_module(c_proc,null);
  dbms_output.put_line('Initialising the Scheduler FSM daemon');
  select count(*)
    into v_count
    from sys.user_jobs
   where upper(what) like upper('%.fsm%');
  if(v_count>0)then
    dbms_output.put_line('Scheduler FSM daemon is already polling once every '||v_scan_interval||' seconds.');
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
  scheduler.sched.init_fsm;
  select count(*)
    into v_count
    from sys.user_jobs
   where upper(what) like upper('%.fsm%');
  if(v_count>0)then
    dbms_output.put_line('Scheduler FSM daemon has been initialised to poll once every '||v_scan_interval||' seconds.');
    dbms_application_info.set_module(null,null);
    return 0;
  else
    dbms_output.put_line('Could not initialise the Scheduler FSM daemon.');
    dbms_application_info.set_module(null,null);
    return 1;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end scheduler_init;

--===========================================================================--
-- TASK CONTROL
--===========================================================================--



-------------------------------------------------------------------------------
-- Delete the task.
--
-- Parameters:  Primary Key Task Id
--              Comma-separated string of Task Id's if multiple tasks selected.
-- Returns:     A non-success return code means that it was impossible
--              to remove the task from the schedule
function task_delete (p_task_id in schedules.task_id%type) 
return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_delete';
  v_retcode utl.global.t_error_code:=1;
  v_task_spec     sched.t_schedule_rec;
begin
  dbms_application_info.set_module(c_proc,null);
  v_task_spec.task_id:=p_task_id;
  v_retcode:=sched.get_task_details(v_task_spec);
  if(v_retcode<>utl.pkg_exceptions.gc_success)then
    dbms_output.put_line('Task Id '||p_task_id||' does not exist on the schedule.');
  else
    begin
      scheduler_mod.task_delete(p_task_id);
      v_retcode:=0;
      dbms_output.put_line('Task Id '||p_task_id||' ('||upper(v_task_spec.group_name)||':'||v_task_spec.operation_id||') removed from schedule.');
    exception
      when utl.pkg_exceptions.e_scheduler_task_busy then
        dbms_output.put_line('Task Id '||p_task_id||' ('||upper(v_task_spec.group_name)||':'||v_task_spec.operation_id||') is currently being executed by the sched.');
      when utl.pkg_exceptions.e_scheduler_task_exist then
        dbms_output.put_line('Task Id '||p_task_id||' ('||upper(v_task_spec.group_name)||':'||v_task_spec.operation_id||') does not exist on sched.');
      when utl.pkg_exceptions.e_scheduler_task_edit_lock then
        dbms_output.put_line('Could not get a lock on Task Id '||p_task_id||' ('||upper(v_task_spec.group_name)||':'||v_task_spec.operation_id||') does not exist on sched.');
    end;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_delete;

-------------------------------------------------------------------------------
-- English explanation
-- set serveroutput on format wrapped
function task_explain(p_task_id in schedules.task_id%type)
return pls_integer
is
  c_proc   constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_explain';
  v_exp varchar2(4000);
  l_exp dbms_sql.varchar2s;
  v_lines pls_integer;
  v_retcode utl.global.t_error_code;
begin
  dbms_application_info.set_module(c_proc,null);
  v_exp:=scheduler_rep.task_explanation(p_task_id);
  if(v_exp is null)then
    dbms_output.put_line('The Task Id '||p_task_id||' does not exist.');
    dbms_application_info.set_module(null,null);
    return 1;
  else
    v_retcode:=utl.pkg_string.break_string(v_exp,l_exp,v_lines,null,80);
    for v_pos in l_exp.first..l_exp.last loop
      dbms_output.put_line(l_exp(v_pos));
    end loop;
    dbms_application_info.set_module(null,null);
    return 0;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_explain;

-------------------------------------------------------------------------------
-- Suspend task, causing the task and any dependent tasks not to execute.
function task_suspend(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name  constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_suspend';
  v_task_spec sched.t_schedule_rec;
  v_retcode           pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Suspends a task by setting it to the '||sched.gc_state_SUSPENDED||' state.');
    dbms_output.put_line('You need to specify a valid Task Id. Exiting...');
    v_retcode:=1;
  else
    v_task_spec.task_id:=p_task_id;
    if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
      dbms_output.put_line('The specified Task Id '||p_task_id||' does not exist.');
      v_retcode:=1;
    else
      v_task_spec.task_id:=p_task_id;
      if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
        if(v_task_spec.state=sched.gc_state_EXECUTING)then
          dbms_output.put_line('This task is still executing - try again later.');
          v_retcode:=1;
        elsif(v_task_spec.state=sched.gc_state_SUSPENDED)then
          dbms_output.put_line('This task is already suspended');
          v_retcode:=1;
        elsif(v_task_spec.state=sched.gc_state_RESUMED)then
          dbms_output.put_line('This task is still resuming from its previous suspension');
        elsif(v_task_spec.state in (sched.gc_state_ERROR,
                                    sched.gc_state_DISABLED,
                                    sched.gc_state_UNDEFINED,
                                    sched.gc_state_BROKEN))then
          dbms_output.put_line('Resolve the '||v_task_spec.state||'-error before attempting to suspend the task');
          v_retcode:=1;
        elsif(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
          dbms_output.put_line('This task is currently being amended by a user. Try again later.');
          v_retcode:=1;
        else
          sched.set_task_state(v_task_spec,sched.gc_state_SUSPENDED);
          dbms_output.put_line('Task Id '||p_task_id||' has now been suspended.');
          v_retcode:=0;
        end if;
      else
        dbms_output.put_line('The specified Task Id does not exist.');
      end if;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_suspend;

-------------------------------------------------------------------------------
-- Resume task if it was in SUSPENDED state
function task_resume(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name  constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_resume';
  v_task_spec  sched.t_schedule_rec;
  v_retcode           pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Resumes a suspended task by setting it to the '||sched.gc_state_RESUMED||' state.');
    dbms_output.put_line('You need to specify a valid Task Id. Exiting...');
    v_retcode:=1;
  else
    v_task_spec.task_id:=p_task_id;
    if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
      dbms_output.put_line('The specified Task Id '||p_task_id||' does not exist.');
      v_retcode:=1;
    else
      v_task_spec.task_id:=p_task_id;
      if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
        if(v_task_spec.state=sched.gc_state_SUSPENDED)then
          sched.set_task_state(v_task_spec,sched.gc_state_RESUMED,sysdate);
          dbms_output.put_line('Task Id '||p_task_id||' has now been resumed.');
          v_retcode:=0;
        else
          if(v_task_spec.state=sched.gc_state_RESUMED)then
            dbms_output.put_line('This task has recently been resumed.');
            v_retcode:=1;
          else
            dbms_output.put_line('This task can only be resumed if it is in the suspended state.');
            v_retcode:=1;
          end if;
        end if;
      end if;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_resume;

-------------------------------------------------------------------------------
-- Disable task, regardless of the state of the task
function task_disable(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_disable';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Disables a task by setting it to the '||sched.gc_state_DISABLED||' state.');
    dbms_output.put_line('You need to specify a valid Task Id. Exiting...');
    v_retcode:=1;
  else
    v_task_spec.task_id:=p_task_id;
    if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
      dbms_output.put_line('The specified Task Id does not exist.');
      v_retcode:=1;
    else
      if(v_task_spec.state=sched.gc_state_DISABLED)then
        dbms_output.put_line('This task has already been disabled.');
        v_retcode:=1;
      elsif(v_task_spec.state=sched.gc_state_EXECUTING)then
        dbms_output.put_line('This task is currently executing and cannot be disabled. Try again later.');
        v_retcode:=1;
      elsif(v_task_spec.state=sched.gc_state_UNDEFINED)then
        dbms_output.put_line('This task is in an undefined state. It needs to be manually corrected.');
        v_retcode:=1;
      elsif(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
        dbms_output.put_line('This task is currently being amended by a user. Try again later.');
        v_retcode:=1;
      else
        sched.set_task_state(v_task_spec,sched.gc_state_DISABLED,sysdate);
        dbms_output.put_line('Task Id '||p_task_id||' has now been disabled.');
        v_retcode:=0;
      end if;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_disable;

-------------------------------------------------------------------------------
-- Reset a hung task to the READY state
function task_reset(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_reset';  
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Resets the state of a task to '||sched.gc_state_INITIAL||' if it is in one of the following states:');
    dbms_output.put_line(sched.gc_state_ERROR);
    dbms_output.put_line(sched.gc_state_TIMEDOUT);
    dbms_output.put_line(sched.gc_state_DISABLED);
    dbms_output.put_line(sched.gc_state_UNDEFINED);
    dbms_output.put_line(sched.gc_state_BROKEN);
    dbms_output.put_line(sched.gc_state_ABORTED);
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
    v_retcode:=1;
  else
    v_retcode:=scheduler_mod.task_reset(p_task_id);
    if(v_retcode=utl.pkg_exceptions.gc_success)then
      dbms_output.put_line('Task Id '||p_task_id||' has been reset to the '||sched.gc_state_INITIAL||' state.');
    elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_inv_state)then
      dbms_output.put_line('Task Id '||p_task_id||' does not need to be reset.');
    elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_exist)then
      dbms_output.put_line('Task Id '||p_task_id||' does not exist.');
    else
      dbms_output.put_line('The attempt to reset Task Id '||p_task_id||' caused the following error:');
      utl.pkg_logger.error(v_retcode);
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_reset;


-- Forwards a task to the next due date
-- set serveroutput on format wrapped
function task_forward(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_forward';
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  v_date_due          date;
  l_tasks             sched.t_schedules;   
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Forwards the current reference date of the task based on the recurring pattern.');
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
    v_retcode:=1;
  else
    dbms_output.put_line('Forwarding Task Id '||p_task_id||' and dependents to the next recurrance:');
    l_tasks:=scheduler_mod.task_forward(p_task_id,v_date_due);
    for i in l_tasks.first..l_tasks.last loop    
      if(i=1)then
        dbms_output.put('Parent ');
      else
        dbms_output.put('Child ');
      end if;
      if(l_tasks(i).next_due_date is not null)then    
        dbms_output.put_line('Task Id '||l_tasks(i).task_id||'''s new ''next due date'' has been set to '||to_char(l_tasks(i).next_due_date,gc_datetime_format)||'.');      
      else
        dbms_output.put_line('Task Id '||l_tasks(i).task_id||' does not have a ''next due date''.');      
      end if;
    end loop;
  end if;
  if(mod(v_retcode,256)=0)then
    v_retcode:=1;
  else
    v_retcode:=mod(v_retcode,256);
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_forward;

-------------------------------------------------------------------------------
-- Runs the selected task now
--
-- Parameters:  Primary Key Task Id
--              Comma-separated string of Task Id's if multiple tasks selected.
-- Returns:     A non-success return code means that it was impossible
--              to lauch the task, or if more than one task was selected,
--              one or more task launches failed.
function task_run(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_run';
  v_retcode           pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Forces the task to run now regardless of dependencies and execution pattern.');
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
    v_retcode:=1;
  else
    dbms_output.put_line('Setting Task Id '||p_task_id||' to run now...');
    v_retcode:=scheduler_mod.task_run_now(p_task_id);
    if(v_retcode=utl.pkg_exceptions.gc_success)then
      dbms_output.put_line('Task Id '||p_task_id||' is set to run.');
    elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_inv_state)then
      dbms_output.put_line('Task Id '||p_task_id||' is in a state that does not allow this action.');
      v_retcode:=1;
    else
      dbms_output.put_line('The attempt to run Task Id '||p_task_id||' now caused the following error:');
      utl.pkg_logger.error(v_retcode);
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_run;

-------------------------------------------------------------------------------
-- Aborts a currenly-running task
function task_abort(p_task_id in schedules.task_id%type) return pls_integer
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_abort';
  v_retcode       pls_integer:=1;
begin
  dbms_application_info.set_module(c_proc,null);
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Aborts a currently-executing task.');
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
  else
    begin
      dbms_output.put_line('Aborting Task Id '||p_task_id||'...');
      v_retcode:=scheduler_mod.task_abort(p_task_id);
      dbms_output.put_line('Task Id '||p_task_id||' successfully aborted.');
    exception
      when utl.pkg_exceptions.e_scheduler_task_exist then
        dbms_output.put_line('Task Id '||p_task_id||' does not exist on the schedule.');
      when utl.pkg_exceptions.e_scheduler_task_timeout then
        dbms_output.put_line('Task Id '||p_task_id||' cannot be confirmed to have terminated.');
      when utl.pkg_exceptions.e_scheduler_task_busy then
        dbms_output.put_line('Task Id '||p_task_id||' is still in the process of aborting. Please check later again.');
      when utl.pkg_exceptions.e_scheduler_task_abort then
        dbms_output.put_line('Task Id '||p_task_id||' cannot be aborted since it is not executing.');
      when utl.pkg_exceptions.e_scheduler_task_lost then
        dbms_output.put_line('Task Id '||p_task_id||' does not appear to be running any more.');
      when others then
        dbms_output.put_line('The attempt to abort Task Id '||p_task_id||' caused the following error:');
        dump_error_details(sqlcode);
    end;
  end if;
  if(mod(sqlcode,256)=0)then
    v_retcode:=1;
  else
    v_retcode:=mod(sqlcode,256);
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_abort;


-- Alters and commits all changes to an existing task
-- All in one operation
function task_alter(
  p_task_id           in schedules.task_id%type,
  p_submitted_by      in varchar2:=null,
  p_task_type         in varchar2:=null,
  p_task_priority     in varchar2:=null,
  p_task_peers        in varchar2:=null,
  p_group_name        in varchar2:=null,
  p_group_priority    in varchar2:=null,
  p_operation_id      in varchar2:=null,
  p_command           in varchar2:=null,
  p_command_type      in varchar2:=null,
  p_description       in varchar2:=null,
  p_dependencies      in varchar2:=null,
  p_max_waittime      in varchar2:=null,
  p_max_runtime       in varchar2:=null,
  p_year              in varchar2:=null,
  p_month             in varchar2:=null,
  p_day               in varchar2:=null,
  p_hour              in varchar2:=null,
  p_minute            in varchar2:=null,
  p_weekdays          in varchar2:=null,
  p_special_days      in varchar2:=null,
  p_next_due_date     in varchar2:=null,
  p_repeats           in varchar2:=null,
  p_repeat_interval   in varchar2:=null,
  p_repeat_periodic   in varchar2:=null,
  p_effective_date_offset in varchar2:=null,
  p_modal             in varchar2:=null,  
  p_ignore_error      in varchar2:=null,
  p_change_reason     in varchar2:=null
) return pls_integer
is
  pragma autonomous_transaction;
  c_proc  constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_alter';
  v_task_spec     sched.t_schedule_rec;
  v_sql           varchar2(2000);
  c_null          constant varchar2(10):='null';
begin
  dbms_application_info.set_module(c_proc,null);

  -- Get full task spec
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;
  -- Task state  
  if(v_task_spec.state in(sched.gc_state_EXECUTING,
                          sched.gc_state_ABORTING,
                          sched.gc_state_EDIT_LOCK) )
  then
    sched.set_task_state(v_task_spec,v_task_spec.state);  -- some smoke and mirrors
    raise utl.pkg_exceptions.e_scheduler_user_operation;
  end if;
  sched.set_task_state(v_task_spec,sched.gc_state_EDIT_LOCK);
  
  -- Insert for now and then validate
  v_sql:='update schedules set '||chr(10);                    
  if(p_submitted_by is not null)then 
    if(p_submitted_by=c_null)then
      v_sql:=v_sql||'submitted_by=null,';
    else
      v_sql:=v_sql||'submitted_by='''||p_submitted_by||''',';
    end if;
  end if;
  if(p_task_type is not null)then
    if(p_task_type=c_null)then
      v_sql:=v_sql||'task_type=null,';
    else
      v_sql:=v_sql||'task_type='''||p_task_type||''',';
    end if;
  end if;
  if(p_task_priority is not null)then
    if(p_task_priority=c_null)then
      v_sql:=v_sql||'task_priority=null,';
    else
      v_sql:=v_sql||'p_task_priority='''||p_task_priority||''',';
    end if;
  end if;
  if(p_group_name is not null)then
    if(p_group_name=c_null)then
      v_sql:=v_sql||'group_name=null,';
    else
      v_sql:=v_sql||'group_name='''||p_group_name||''',';
    end if;
  end if;
  if(p_operation_id is not null)then
    if(p_operation_id=c_null)then
      v_sql:=v_sql||'operation_id=null,';
    else
      v_sql:=v_sql||'operation_id='''||p_operation_id||''',';
    end if;
  end if;
  if(p_command is not null)then
    if(p_command = c_null)then    
      v_sql:=v_sql||'command=null,';
    else
      v_sql:=v_sql||'command='''||p_command||''',';
    end if;
  end if;
  if(p_command_type is not null)then
    if(p_command_type=c_null)then
      v_sql:=v_sql||'command_type=null,';
    else
      v_sql:=v_sql||'command_type='''||p_command_type||''',';
    end if;
  end if;
  if(p_description is not null)then
    if(p_description = c_null)then
      v_sql:=v_sql||'description=null,';
    else
      v_sql:=v_sql||'description='''||p_description||''',';
    end if;
  end if;
  if(p_dependencies is not null)then
    if(p_dependencies = c_null)then
      v_sql:=v_sql||'dependencies=null,';
    else
      v_sql:=v_sql||'dependencies='''||p_dependencies||''',';
    end if;
  end if;
  if(p_max_waittime is not null)then
    if(p_max_waittime = c_null)then
      v_sql:=v_sql||'max_waittime=null,';
    else
      v_sql:=v_sql||'max_waittime='''||p_max_waittime||''',';
    end if;
  end if;
  if(p_max_runtime is not null)then
    if(p_max_runtime = c_null)then
      v_sql:=v_sql||'max_runtime=null,';
    else    
      v_sql:=v_sql||'max_runtime='''||p_max_runtime||''',';
    end if;
  end if;
  if(p_year is not null)then
    if(p_year = c_null)then
      v_sql:=v_sql||'year=null,';    
    else
      v_sql:=v_sql||'year='''||p_year||''',';
    end if;
  end if;
  if(p_month is not null)then
    if(p_month = c_null)then
      v_sql:=v_sql||'month=null,';
    else
      v_sql:=v_sql||'month='''||p_month||''',';
    end if;
  end if;
  if(p_day is not null)then
    if(p_day = c_null)then
      v_sql:=v_sql||'day=null,';
    else
      v_sql:=v_sql||'day='''||p_day||''',';
    end if;
  end if;  
  if(p_hour is not null)then 
    if(p_hour = c_null)then
      v_sql:=v_sql||'hour=null,';
    else
      v_sql:=v_sql||'hour='''||p_hour||''',';
    end if;
  end if;
  if(p_minute is not null)then 
    if(p_minute = c_null)then
      v_sql:=v_sql||'minute=null,';
    else
      v_sql:=v_sql||'minute='''||p_minute||''',';
    end if;
  end if;
  if(p_weekdays is not null)then 
    if(p_weekdays = c_null)then
      v_sql:=v_sql||'weekdays=null,';
    else
      v_sql:=v_sql||'weekdays='''||p_weekdays||''',';
    end if;    
  end if;
  if(p_special_days is not null)then 
    if(p_special_days = c_null)then
      v_sql:=v_sql||'special_days=null,';
    else
      v_sql:=v_sql||'special_days='''||p_special_days||''',';
    end if;    
  end if;
  if(p_next_due_date is not null)then 
    if(p_next_due_date = c_null)then
      v_sql:=v_sql||'next_due_date=null,';
    else
      v_sql:=v_sql||'next_due_date='''||p_next_due_date||''',';
    end if;        
  end if;
  if(p_repeats is not null)then 
    if(p_repeats = c_null)then
      v_sql:=v_sql||'repeats=null,';
    else
      v_sql:=v_sql||'repeats='''||p_repeats||''',';
    end if;            
  end if;
  if(p_repeat_interval is not null)then 
    if(p_repeat_interval = c_null)then
      v_sql:=v_sql||'repeat_interval=null,';
    else
      v_sql:=v_sql||'repeat_interval='''||p_repeat_interval||''',';
    end if;                
  end if;
  if(p_repeat_periodic is not null)then 
    if(p_repeat_periodic = c_null)then
      v_sql:=v_sql||'repeat_periodic=null,';
    else
      v_sql:=v_sql||'repeat_periodic='''||p_repeat_periodic||''',';
    end if;                
  end if;
  if(p_effective_date_offset is not null)then 
    if(p_effective_date_offset = c_null)then
      v_sql:=v_sql||'effective_date_offset=null,';
    else
      v_sql:=v_sql||'effective_date_offset='''||p_effective_date_offset||''',';
    end if;                
  end if;
  if(p_modal is not null)then 
    if(p_modal = c_null)then
      v_sql:=v_sql||'modal=null,';
    else
      v_sql:=v_sql||'modal='''||p_modal||''',';
    end if;                    
  end if; 
  if(p_ignore_error is not null)then 
    if(p_modal = c_null)then
      v_sql:=v_sql||'ignore_error=null,';
    else
      v_sql:=v_sql||'ignore_error='''||p_ignore_error||''',';
    end if;                        
  end if;
  v_sql:=v_sql||'change_reason='''||p_change_reason||''',';
  v_sql:=v_sql||'dependency_sql = null where task_id = '||p_task_id; 
  execute immediate v_sql;  
  
  -- Get the resulting task after the proposed edits  
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;

  -- Validate the task  
  if(scheduler_val.validate_task(v_task_spec)<>utl.pkg_exceptions.gc_success) then
    raise utl.pkg_exceptions.e_scheduler_task_spec;
  end if;  
  
  if(scheduler_dep.validate_dependencies(v_task_spec)<>utl.pkg_exceptions.gc_success) then
    raise utl.pkg_exceptions.e_scheduler_circular_ref;
  end if;
  
  -- Update corrected values
  update schedules set 
         submitted_by = v_task_spec.submitted_by,
         task_type = v_task_spec.task_type,
         task_priority = v_task_spec.task_priority,
         group_name = v_task_spec.group_name,
         operation_id = v_task_spec.operation_id,
         command = v_task_spec.command,
         command_type = v_task_spec.command_type,
         description = v_task_spec.description,
         dependencies = v_task_spec.dependencies,
         max_waittime = v_task_spec.max_waittime,
         max_runtime = v_task_spec.max_runtime,
         queue_id = v_task_spec.queue_id,
         process_id = v_task_spec.process_id,
         return_code = v_task_spec.return_code,
         state = v_task_spec.prev_state, -- remove edit lock
         state_tmstmp = v_task_spec.state_tmstmp,
         started_at = v_task_spec.started_at,
         finished_at = v_task_spec.finished_at,
         year = v_task_spec.year,
         month = v_task_spec.month,
         day = v_task_spec.day,
         hour = v_task_spec.hour,
         minute = v_task_spec.minute,
         weekdays = v_task_spec.weekdays,
         special_days = v_task_spec.special_days,
         next_due_date = v_task_spec.next_due_date,
         repeats = v_task_spec.repeats,
         repeat_interval        = v_task_spec.repeat_interval,
         repeat_count           = v_task_spec.repeat_count,
         repeat_periodic        = v_task_spec.repeat_periodic,
         effective_date_offset  = v_task_spec.effective_date_offset,
         modal                  = v_task_spec.modal,
         ignore_error           = v_task_spec.ignore_error,
         dependency_sql         = v_task_spec.dependency_sql,
         change_reason          = v_task_spec.change_reason
   where task_id = v_task_spec.task_id;

  -- Update task peer values
  scheduler_mod.task_peers(p_task_id, p_task_peers);
  scheduler_mod.group_priority(v_task_spec.group_name,p_group_priority);   
  commit;  
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    sched.set_task_state(v_task_spec,v_task_spec.prev_state);
    return 1;
end task_alter;

-- Adds a task to the schedule
-- Returns the Id of the new task
function task_add(
  p_submitted_by      in schedules.submitted_by%type:=null,
  p_task_type         in schedules.task_type%type:=null,
  p_task_priority     in schedules.task_priority%type:=null,
  p_task_peers        in varchar2:=null,
  p_group_name        in schedules.group_name%type:=null,
  p_group_priority    in task_groups.group_priority%type:=null,
  p_operation_id      in schedules.operation_id%type:=null,
  p_command           in schedules.command%type:=null,
  p_command_type      in schedules.command_type%type:=null,
  p_description       in schedules.description%type:=null,
  p_dependencies      in schedules.dependencies%type:=null,
  p_max_waittime      in schedules.max_waittime%type:=null,
  p_max_runtime       in schedules.max_runtime%type:=null,
  p_year              in schedules.year%type:=null,
  p_month             in schedules.month%type:=null,
  p_day               in schedules.day%type:=null,
  p_hour              in schedules.hour%type:=null,
  p_minute            in schedules.minute%type:=null,
  p_weekdays          in schedules.weekdays%type:=null,
  p_special_days      in schedules.special_days%type:=null,
  p_next_due_date     in schedules.next_due_date%type:=null,
  p_repeats           in schedules.repeats%type:=null,
  p_repeat_interval   in schedules.repeat_interval%type:=null,
  p_repeat_periodic   in schedules.repeat_periodic%type:=null,  
  p_effective_date_offset in schedules.effective_date_offset%type:=null,
  p_modal             in schedules.modal%type:=null,
  p_ignore_error      in schedules.ignore_error%type:=null,
  p_change_reason     in schedules.change_reason%type:=null
) return pls_integer
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_add';
  v_task_id       schedules.task_id%type:=1;
begin
  dbms_application_info.set_module(c_proc,null);  
  v_task_id:=scheduler_mod.task_add(
    p_submitted_by             =>p_submitted_by,
    p_task_type                =>p_task_type,
    p_task_priority            =>p_task_priority,
    p_task_peers               =>p_task_peers,
    p_group_name               =>p_group_name,
    p_group_priority           =>p_group_priority,
    p_operation_id             =>p_operation_id,
    p_command                  =>p_command,
    p_command_type             =>p_command_type,
    p_description              =>p_description,
    p_dependencies             =>p_dependencies,
    p_max_waittime             =>p_max_waittime,
    p_max_runtime              =>p_max_runtime,
    p_year                     =>p_year,
    p_month                    =>p_month,
    p_day                      =>p_day,
    p_hour                     =>p_hour,
    p_minute                   =>p_minute,
    p_weekdays                 =>p_weekdays,
    p_special_days             =>p_special_days,
    p_next_due_date            =>p_next_due_date,
    p_repeats                  =>p_repeats,
    p_repeat_interval          =>p_repeat_interval,
    p_repeat_periodic          =>p_repeat_periodic,
    p_effective_date_offset    =>p_effective_date_offset,
    p_modal                    =>p_modal,
    p_ignore_error             =>p_ignore_error,
    p_change_reason            =>p_change_reason );

  dbms_output.put_line('Task Id '||v_task_id||' added to the scheduler.');
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_add;

-- Output the task content
function task_show(p_task_id in schedules.task_id%type)
return pls_integer
is
  c_proc   constant varchar2(100) := pc_schema||'.'||pc_package||'.task_show';
  v_dump  varchar(4000);
  l_lines   dbms_sql.varchar2s;
  v_count   pls_integer;
  v_retcode utl.global.t_error_code;
begin
  dbms_application_info.set_module(c_proc,null);  
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Displays content of a task.');
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
    dbms_application_info.set_module(null,null);  
    return 1;
  else
    v_dump:=scheduler_rep.task_show(p_task_id);
    v_retcode:=utl.pkg_string.break_string(v_dump,l_lines,v_count,null,80);
    for i in l_lines.first..l_lines.last loop
      dbms_output.put_line(l_lines(i));
    end loop;
  end if;
  dbms_application_info.set_module(null,null);  
  return 0;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return 1;
end task_show;

-- Output the task content
function task_dump(p_task_id in schedules.task_id%type)
return pls_integer
is
  c_proc   constant varchar2(100) := pc_schema||'.'||pc_package||'.task_dump';
  v_dump  varchar(4000);
  l_lines   dbms_sql.varchar2s;
  v_count   pls_integer;
  v_retcode utl.global.t_error_code;
  v_msg     varchar2(200);
  v_exp     varchar2(1000);
begin
  dbms_application_info.set_module(c_proc,null);  
  if(p_task_id is null)then
    -- Provide helpful function overview
    dbms_output.put_line('Dumps content of a task.');
    dbms_output.put_line('You need to specify a valid TaskId. Exiting...');
    dbms_application_info.set_module(null,null);  
    return 1;
  else
    v_dump:=scheduler_rep.task_dump(p_task_id);
    v_retcode:=utl.pkg_string.break_string(v_dump,l_lines,v_count,null,80);
    if(v_retcode<>utl.pkg_exceptions.gc_success)then
      utl.pkg_errorhandler.code2desc(v_retcode,v_msg, v_exp);
      dbms_output.put_line(v_msg);
      dbms_output.put_line(v_exp);
    else
      for i in l_lines.first..l_lines.last loop
        dbms_output.put_line(l_lines(i));
      end loop;
    end if;
  end if;
  dbms_application_info.set_module(null,null);  
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_dump;

-- Insert or Update task peer values for the given comma-delimited 
-- list of task Id's
function task_peers(p_task_id in schedules.task_id%type, 
                    p_task_peers in varchar2) return pls_integer
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_peers';
begin     
  dbms_application_info.set_module(c_proc,null);
  scheduler_mod.task_peers(p_task_id,p_task_peers,true);
  dbms_output.put_line('Peered TaskId '''||p_task_id||''' with Task Id(s) '''||p_task_peers||'''.');
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_peers;

-------------------------------------------------------------------------------
-- GROUP MODIFICATION
-------------------------------------------------------------------------------

-- Inserts or updates task group's priority
function group_priority(p_group_name task_groups.group_name%type,
                        p_priority   task_groups.group_priority%type)
return pls_integer                        
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.group_priority';
  v_count         pls_integer;
begin
  dbms_application_info.set_module(c_proc,null);
  select count(*) 
    into v_count
    from task_groups
   where group_name = upper(p_group_name);   
  scheduler_mod.group_priority(p_group_name,p_priority,true);
  if(v_count=0)then
    dbms_output.put_line('Inserted the new task group '''||p_group_name||''' and given it a relative priority of '||p_priority);
  else
    dbms_output.put_line('Updated the relative priority of group '''||p_group_name||''' to '||p_priority);
  end if;  
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end group_priority;                    

-- Peers task groups for the given comma-delimited list of group names
function group_peers(p_group_name task_group_peers.group_peer1%type,
                    p_group_peers varchar2) 
return pls_integer
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.group_peer';
begin
  dbms_application_info.set_module(c_proc,null);
  scheduler_mod.group_peers(p_group_name,p_group_peers,true);
  dbms_output.put_line('Peered group '''||p_group_name||''' with group(s) '''||p_group_peers||'''.');
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end group_peers;                    

begin
  gc_datetime_format      :=nvl(utl.pkg_config.get_variable_string(gc_config_key_datetimeformat),'YYYY/MM/DD HH24:MI');
end scheduler_con;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
