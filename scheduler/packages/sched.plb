create or replace package body scheduler.sched as
------------------------------------------------------------------------
------------------------------------------------------------------------
-- Task Scheduling Mechanism
--  DO NOT "BEAUTIFY" THIS CODE !!!!!
------------------------------------------------------------------------
--
-- Package Architecture:
-- ~~~~~~~~~~~~~~~~~~~~~
--  +---------------+       +---------------+        +---------------+
--  | SCHEDULER_FSM +---+---+ SCHEDULER_MOD +---+----+ SCHEDULER_GUI |
--  +---------------+   |   +---------------+   |    +---------------+
--                      |                       |
--                      |   +---------------+   |    +---------------+
--                      +---+ SCHEDULER_REP |   +----+ SCHEDULER_CON |
--                      |   +---------------+        +---------------+
--                      |
--                      |   +-----n----------+
--                      +---+ SCHEDULER_DEP |
--                      |   +---------------+
--                      |
--                      |   +---------------+
--                      +---+ SCHEDULER_DUE |
--                      |   +---------------+
--                      |
--                      |   +---------------+
--                      +---+ SCHEDULER_VAL |
--                          +---------------+
--
--===========================================================================--
-- PRIVATE FUNCTIONS
--===========================================================================--

-- The calling procedure does the commit 
procedure insert_task_event(p_task_log_spec in task_log%rowtype) is
begin
  insert into task_log(
         id,
         task_id,
         state,
         execution_id,
         repeat_count,
         scheduled_time,
         started_at,
         ended_at,
         queue_id,
         process_id,
         return_code,
         modal,
         what)
  values (
         sq_task_log_id.nextval,
         p_task_log_spec.task_id,
         p_task_log_spec.state,
         p_task_log_spec.execution_id,
         p_task_log_spec.repeat_count,
         p_task_log_spec.scheduled_time,
         p_task_log_spec.started_at,
         p_task_log_spec.ended_at,
         p_task_log_spec.queue_id,
         p_task_log_spec.process_id,
         p_task_log_spec.return_code,
         p_task_log_spec.modal,
         p_task_log_spec.what);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;  
end insert_task_event;

-- Let outside world in real time know what is going on
-- The information is not critical
procedure tx_heartbeat(p_msg in varchar2:=null) is
  v_retcode           pls_integer;
  v_vcr_home          varchar2(50);
begin
  v_vcr_home:=utl.pkg_config.get_variable_string('$APP_HOME');
  v_retcode:=utl.hostcmd('echo "'||nvl(p_msg,'*')||'." >> '||v_vcr_home||'/log/heartbeat');
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end tx_heartbeat;  

-- Set the scheduler state and persist it to the configuration table
procedure set_status(p_status in varchar2)
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.set_status';
  v_last_scheduler_status varchar2(30) := utl.pkg_config.get_variable_string(gc_config_status_key);
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_last_scheduler_status<>p_status)then
    if(p_status in (gc_mode_UNQUIESCED,
                    gc_mode_QUIESCING,
                    gc_mode_QUIESCED,
                    gc_mode_SUSPENDED,
                    gc_mode_ABORTING,
                    gc_mode_ABORTED) )
    then
      -- Valid states
      utl.pkg_config.set_variable_string(gc_config_status_key,p_status);
      utl.pkg_logger.log(null, 'Scheduler status changed from '''||v_last_scheduler_status||''' to '''||p_status||'''');
      tx_heartbeat('SCHEDULER->'||p_status);
    else
      -- Invalid state
      raise utl.pkg_exceptions.e_scheduler_inv_mode;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end set_status;

        
--===========================================================================--
-- PUBLIC FUNCTIONS
--===========================================================================--

-- Clear any previous instances of the schedule FSM
-- This function needs to be set up as a trigger:
-- create or replace trigger db_startup before shutdown on database
-- begin
--   utl.scheduler.kill_fsm;
-- end;
-- /
procedure kill_fsm is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.kill_fsm';
  cursor c_jobs is
    select job
      from sys.user_jobs
     where upper(what) like upper('%'||pc_package||'.fsm%');
  v_killcount   pls_integer:=0;
begin
  dbms_application_info.set_module(c_proc_name,null);
  utl.pkg_config.set_variable_date(gc_config_last_shutdown_key,sysdate);
  for c in c_jobs loop
    dbms_job.remove(c.job);
    v_killcount:=v_killcount+1;
  end loop;
  commit;
  if(v_killcount>0)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Scheduler FSM killed.');
  end if;
  tx_heartbeat('KILL');
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end kill_fsm;

-- Initialises the FSM
-- This function needs to be set up as a trigger:
-- create or replace trigger db_startup after startup on database
-- begin
--   utl.scheduler.init_fsm;
-- end;
-- /
-- This needs to be done as sysadmin.
procedure init_fsm is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.init_fsm';
  v_job     pls_integer;
  v_status  varchar2(30);
  v_fsm_scan_interval   pls_integer := nvl(utl.pkg_config.get_variable_int(gc_config_scan_int_key),10);
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- First deletes any instances of it running
  kill_fsm;
  dbms_job.submit(v_job,'begin '||pc_schema||'.'||pc_package||'.'||'fsm; end;', sysdate, 'sysdate+('||v_fsm_scan_interval||'/(24*60*60))');
  commit;
  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Scheduler FSM initialised');
  tx_heartbeat('INIT');
  v_status:=status();
  -- Set the scheduler status as it most recently was
  if(v_status=gc_mode_UNQUIESCED)then startup;  elsif
    (v_status=gc_mode_QUIESCING) then shutdown; elsif
    (v_status=gc_mode_QUIESCED)  then shutdown; elsif
    (v_status=gc_mode_SUSPENDED) then suspend;  elsif
    (v_status=gc_mode_ABORTING)  then abort;    elsif
    (v_status=gc_mode_ABORTED)   then abort;    end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end init_fsm;

-------------------------------------------------------------------------------
-- TRUE if FSM is running
function is_fsm_running return boolean is
  v_count   pls_integer;
begin
   select count(*)
     into v_count
     from sys.user_jobs j
    where upper(j.what) like upper('%'||pc_package||'.fsm%')
      and j.broken='N';
  if(v_count>0)then
    return true;
  else
    return false;
  end if;
exception 
  when others then
    return false;
end is_fsm_running;

-------------------------------------------------------------------------------
-- Get the status of the scheduler
-- Also updates the status when called.
-- When the scheduler is in UNKOWN state, it tries to resolve it to a known state.
function status return varchar2
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.status';
  v_count pls_integer;
  v_scheduler_status  varchar2(30):=nvl(utl.pkg_config.get_variable_string(gc_config_status_key),gc_mode_UNKNOWN);
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Same process that FSM uses in case it is not running
  if(v_scheduler_status=gc_mode_QUIESCING)then
    select count(*)
      into v_count
      from schedules
     where state = gc_state_EXECUTING;
    if(v_count=0)then
      -- All tasks done
      set_status(gc_mode_QUIESCED);
    end if;
  end if;

  if(v_scheduler_status=gc_mode_UNKNOWN)then
    -- Still unknown - Try to resolve
    select count(*)
      into v_count
      from sys.user_jobs
     where upper(what) like upper(pc_package||'.fsm%');
    if(v_count>0)then
      -- It could be ABORTING or QUIESCING. Choose the latter
      set_status(gc_mode_QUIESCING);
    else
      -- It could be ABORTED or QUIESCED. Choose the latter
      set_status(gc_mode_QUIESCED);
    end if;
  end if;

  dbms_application_info.set_module(null,null);
  return v_scheduler_status;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end status;

-------------------------------------------------------------------------------
-- Starts the scheduler after it has been shut down
-- Adds the FSM to the dbms_job queue after removing any previous entry.
-- Scheduler will start if in UNQUIESCED or ABORTED mode. Other requests are ignored.
-- Parameters:
--    1. FSM scanning interval in seconds
--      Defaults to last used value or 10 seconds
procedure startup
is
  pragma autonomous_transaction;
  c_proc_name           constant varchar2(100) := pc_schema||'.'||pc_package||'.startup';
  v_sysdate             date:=sysdate;
  v_last_shutdown       date := utl.pkg_config.get_variable_date(gc_config_last_shutdown_key);
  v_last_startup        date := utl.pkg_config.get_variable_date(gc_config_last_startup_key);
  v_status              varchar2(30):= status();  

  procedure reset_durable_tasks is
    v_task_spec   t_schedule_rec;
    cursor c_durable is
      select *
        from vw_fsm_tasks
       where state=gc_state_EXECUTING
         and task_type=gc_type_DURABLE
         and repeats is null;
  begin
    -- Reset states so that they will be restarted as soon as the FSM sees the task
    -- except for those that are in the middle of repeating executions:
    -- They must be left in the EXECUTING state 
    open c_durable;
    loop
      fetch c_durable into v_task_spec;
      exit when c_durable%notfound;
      set_task_state(v_task_spec,gc_state_READY,v_sysdate);
    end loop;
    close c_durable;
  exception
    when others then
      close c_durable;
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log;
  end reset_durable_tasks;

  procedure delete_volatile_tasks is
    cursor c_volatile is
      select state
        from schedules
       where state=gc_state_EXECUTING
         and task_type=gc_type_VOLATILE
         for update;
  begin
    -- Delete volatile tasks that were interrupted while executing
    for c in c_volatile loop
      delete schedules
       where current of c_volatile;
    end loop;
    commit;
  exception
    when others then
      close c_volatile;
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log;
  end delete_volatile_tasks;

begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Determine if there was a database shutdown
  if(v_last_shutdown is not null and v_last_startup is not null)then
    if(v_last_shutdown>v_last_startup)then
      -- This is the first time that we are setting the scheduler in the UNQUIESCED state
      -- after a shutdown
      -- Restart DURABLE tasks that were executing when the server shut down
      reset_durable_tasks;
      -- Remove volatile tasks
      delete_volatile_tasks;
    end if;
  end if;

  if(v_status=gc_mode_QUIESCED or v_status=gc_mode_ABORTED)then  
    -- Get current status and only react if stopped
    set_status(gc_mode_UNQUIESCED);
    utl.pkg_config.set_variable_date(gc_config_last_startup_key,sysdate);
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Starting up request ignored - the scheduler is currently in the '||v_status||' state.');
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
   rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end startup;

-------------------------------------------------------------------------------
-- Stops the scheduler
-- Suspends the FSM in the dbms_job queue
-- Outputs to CONSOLE
procedure shutdown
is
  c_proc_name           constant varchar2(100) := pc_schema||'.'||pc_package||'.shutdown';
  v_count               pls_integer;
  v_status              varchar2(30):= status();  
  v_last_count          pls_integer:=-1;
  v_fsm_scan_interval   pls_integer := nvl(utl.pkg_config.get_variable_int(gc_config_scan_int_key),10);
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_status=gc_mode_UNQUIESCED)then
    utl.pkg_config.set_variable_date(gc_config_last_shutdown_key,sysdate);
    set_status(gc_mode_QUIESCING);    -- This will stop the FSM from polling
  
    -- Will only wait for 10 FSM scans for all tasks to complete
    -- Checks every 2 seconds on how many are done
    for i in 1..10 loop
      -- Check how many jobs are still running
      select count(*)
        into v_count
        from schedules
       where state = gc_state_EXECUTING;
      if(v_count<>v_last_count)then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,v_count||' Tasks waiting to complete...');
        v_last_count:=v_count;
      end if;
      exit when v_count=0;
      dbms_lock.sleep(v_fsm_scan_interval);
    end loop;
  
    if(v_count>0)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, v_count||' tasks are still completing. Check the scheduler status later again.');
    else
      -- All tasks done
      set_status(gc_mode_QUIESCED);
    end if;
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Shutting down request ignored - the scheduler is currently in the '||v_status||' state.');
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end shutdown;

-------------------------------------------------------------------------------
-- Suspends the FSM in the dbms_job queue
--
-- NOTES ON DBMS_JOB
-- Would ideally use 'dbms_job.broken' but to get it to restart would sometime
-- require running 'dbms_job.run' which will reinitialize all the packages in
-- this session - i.e. all states will be reset. It also throws a weird exception.
-- Once a job is set to a broken state, there is no way to query this state except
-- by querying sys.all_jobs or sys.dba_jobs - Verboten! sys.user_jobs is not
-- give the correct state of the BROKEN flag.
-- So, the rules are this:
-- 1. We NEVER set a job to broken.
-- 2. We suspend a job by completely removing is and resubmit it when we resume
-- 3. Oh yeah, in case you have forgotten, we NEVER use a so-called "code beautifier"
-- C'mon, lets use Oracle 10g so that we can use DBMS_SCHEDULER!
procedure suspend
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.suspend';
  v_status        varchar2(30):= status();  
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Scheduler can only be suspended when it is in the running state
  if(v_status=gc_mode_UNQUIESCED)then
    -- The current FSM scan will asynchronously complete.
    set_status(gc_mode_SUSPENDED);
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Suspend request ignored - the scheduler is currently in the '||v_status||' state.');
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end suspend;

-------------------------------------------------------------------------------
-- Resumes the FSM in the dbms_job queue
-- Ignores request to resume if not in suspended mode
procedure resume
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.resume';
  v_status        varchar2(30):= status();  
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_status=gc_mode_SUSPENDED)then
    set_status(gc_mode_UNQUIESCED);
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Resume request ignored - the scheduler is currently '||v_status);
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end resume;

-------------------------------------------------------------------------------
-- Aborts the scheduler:
-- An attempt can be made to abruptly halt the entire schedule. This will
-- summarily terminate all tasks where possible and stop the FSM. The effect
-- is the same as when the Oracle instance and the server is hard shut down.
-- The results are unpredictable. It is highly unadvisable to perform this action.
-- Suspends the FSM in the dbms_job queue
procedure abort
is
  c_proc_name     constant varchar2(100) := pc_schema||'.'||pc_package||'.abort';
  v_task_spec     t_schedule_rec;    
  v_status        varchar2(30):= status();  
  v_fsm_scan_interval   pls_integer;
  v_job                 pls_integer;
  v_retcode       utl.global.t_error_code := utl.pkg_exceptions.gc_success;
  cursor c_executing_tasks is
    select *
      from vw_fsm_tasks
     where state=gc_state_EXECUTING
     order by task_id;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_status<>gc_mode_ABORTING and v_status<>gc_mode_ABORTED)then
    set_status(gc_mode_ABORTING);    -- This will stop the FSM from polling
    -- Abort all tasks
    open c_executing_tasks;
    loop
      fetch c_executing_tasks into v_task_spec;
      exit when c_executing_tasks%notfound;
      v_retcode:=scheduler_mod.task_abort(v_task_spec);
    end loop;
    -- For an aborting sequence to succeed, the FSM must be running
    -- The FSM will determine when the ABORT SEQUENCE has completed and will commit Hari-Kari
    if(not is_fsm_running)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Restarting Scheduler FSM for the duration of the abort sequence.');
      v_fsm_scan_interval:= nvl(utl.pkg_config.get_variable_int(gc_config_scan_int_key),10);
      dbms_job.submit(v_job,'begin '||pc_schema||'.'||pc_package||'.'||'fsm; end;', sysdate, 'sysdate+('||v_fsm_scan_interval||'/(24*60*60))');
    end if;          
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info, 'Abort request ignored - the scheduler is already '||v_status);
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end abort;


------------------------------------------------------------------------
-- FSM Controller
-- This procedure should be called every 30 seconds or so by DBMS_JOB,
-- and determines the FSM state for all batch jobs.
--
-- It is typically set up in Oracle's DBMS_JOB scheduler for a 10 second scan:
-- v_job pls_integer;
-- dbms_job.submit(v_job, 'begin utl.scheduler.fsm; end;', sydate, 'sysdate+10/(24*60*60)');
--
-- Check that the values in file $ORACLE_BASE/admin/$ORACLE_SID/pfile/init*.ora
-- are set up to:
-- job_queue_processes = number of processes to simultaneously run. The rule
--                       of thumb is to set it to (#CPUs x 2) + 1.
-- job_queue_interval  = number of seconds for DBMS_JOB to scan its job queue
--
------------------------------------------------------------------------
procedure fsm
is
  c_proc_name           constant varchar2(100)  := pc_schema||'.'||pc_package||'.fsm';
  v_ref_date            date := sysdate;  -- SYSDATE is the scheduler's time reference.
                                          -- Sampled only here and used throughout all 
                                          -- FSM operations to prevent race conditions.
  v_start_time          number;
  v_end_time            number;
  v_scan_duration       number;
  v_retcode             utl.global.t_error_code;
  v_task_spec           t_schedule_rec; 
  l_tasks_in_tree       t_schedules;        -- lists of tasks in tree
  n_task_spec           t_schedule_rec;     -- task tree node
  i_task_spec           binary_integer;     -- task tree iterator
  v_next_due_date       date;           
  v_next_retry_date     date;
  v_artificial_time     boolean:=false;
  v_scheduler_status    varchar2(30):=status();
  v_executing_tasks     pls_integer:=0;
  v_aborting_tasks      pls_integer:=0;
  v_abort_timeout       pls_integer;  
  v_edit_timeout        pls_integer;  
  v_max_tasks           pls_integer;
  v_error_count         pls_integer:=0;

  cursor c_state is
    select *
      from vw_fsm_tasks t
     where nvl(t.state,gc_state_INITIAL) not in (
             gc_state_BROKEN,               
             gc_state_TIMEDOUT,
             gc_state_DISABLED,
             gc_state_UNDEFINED
           )           
     order by nvl(t.modal,'N') desc,  -- Modal tasks first
           t.group_priority desc,     -- Group priority
           t.task_priority desc,      -- Resolve peered tasks
           t.state_tmstmp asc;        -- Prevent skewed behaviour for high frequency tasks   

  -- Launches the current task
  -- After a task has been launched, we need to exit out of the loop 
  -- after we have launched a task so that the states in the cursor 
  -- can be refreshed. 
  procedure launch_task is
  begin
    v_retcode:=submit_task(v_task_spec, v_ref_date);
    if(v_retcode=utl.pkg_exceptions.gc_success)then
      set_task_state(v_task_spec,gc_state_EXECUTING,v_ref_date);
      -- Update task count when being lauched 
      -- but only if it is a proper (non-DUMMY) task
      if(v_task_spec.command is not null)then
        v_executing_tasks:=v_executing_tasks+1; 
      end if;
    elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_submit)then
      set_task_state(v_task_spec,gc_state_BROKEN,v_ref_date);
    else
      set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
    end if;  
  end launch_task;
  
begin
  dbms_application_info.set_module(c_proc_name,null); 
    
  begin
    -- Do nothing if the schedule has been quiesced
    if(v_scheduler_status in (gc_mode_QUIESCED,gc_mode_ABORTED,gc_mode_SUSPENDED))then
      return;
    end if;
      
    -- Collect performance metrics
    v_start_time:= dbms_utility.get_time();
  
    -- Allow injection of system time for testing
    v_artificial_time:=utl.pkg_config.get_variable_date(gc_config_artifical_date_key,v_ref_date);

    -- Need to keep tabs on this value, since we need to limit this number  
    v_executing_tasks:=get_num_executing_tasks;
    
    -- Check and react to state of each task
    open c_state;
    loop
      begin
        fetch c_state into v_task_spec;
        exit when c_state%notfound;
        -- FSM Control
        -- NULL ==============================================
        if(v_task_spec.state is null)then
          set_task_state(v_task_spec,gc_state_INITIAL,v_ref_date);
        -- INITIAL ===========================================
        elsif(v_task_spec.state=gc_state_INITIAL)then
          if(v_scheduler_status <>gc_mode_QUIESCING and v_scheduler_status<>gc_mode_ABORTING)then
            -- Calculate the dependency sql based on the dependency expressions
            -- After a task edit or reset, the task state ends up here.
            v_task_spec.dependency_sql:=scheduler_dep.make_dependency_sql(v_task_spec.dependencies);
            -- Calculate the next due date based on the current value of next_due_date
            v_retcode:=scheduler_due.calc_next_due_date(v_task_spec,nvl(v_task_spec.next_due_date,v_ref_date),v_next_due_date);
            if(v_retcode=utl.pkg_exceptions.gc_success)then
              if(v_next_due_date is not null)then
                set_task_state(v_task_spec,gc_state_WAITING,v_ref_date,v_next_due_date);
              else    
                -- v_next_due_date is NULL and we have dependencies, so no time constraint
                set_task_state(v_task_spec,gc_state_DUE,v_ref_date);
              end if;              
            elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_special_day)then
              -- Rare case: 
              utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,null,'SPECIAL_DAYS',v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_special_day);
              set_task_state(v_task_spec,gc_state_DISABLED,v_ref_date,v_next_due_date);            
            else
              -- Real Error
              utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Next Due Date could not be calculated.',null,v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_inv_date);
              utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,scheduler_rep.task_dump(v_task_spec),null,v_task_spec.task_id,v_retcode,c_proc_name);
              set_task_state(v_task_spec,gc_state_ERROR,v_ref_date,v_next_due_date);            
            end if;
          end if;
        -- WAITING ===========================================
        -- Next due date has been calculated for the task.
        -- Check if it is due
        -- If it, set to DUE state
        elsif(v_task_spec.state=gc_state_WAITING)then
          if(v_scheduler_status<>gc_mode_QUIESCING and v_scheduler_status<>gc_mode_ABORTING)then
            -- Job ready for when it becomes due again
            if(is_task_due(v_task_spec,v_ref_date))then
              set_task_state(v_task_spec,gc_state_DUE,v_ref_date);
            end if;
          end if;
        -- DUE =============================================
        -- Task is due now.
        -- Check if all dependencies are satisfied.
        -- If so, submit the task to the job queue and repare to execute
        -- unless a peer task is already running
        elsif(v_task_spec.state=gc_state_DUE)then
          if(v_scheduler_status<>gc_mode_QUIESCING and v_scheduler_status<>gc_mode_ABORTING)then
            if(scheduler_dep.is_mutual_task_running(v_task_spec))then
              set_task_state(v_task_spec,gc_state_EXCLUDED,v_ref_date);
            else
              if(not scheduler_dep.is_peer_group_running(v_task_spec))then
                if(not scheduler_dep.is_peer_task_running(v_task_spec))then                
                  if(v_task_spec.task_type=gc_type_TIMECRITICAL)then
                    -- Check if time has expired by at least one minute for this task to execute
                    if( (v_ref_date-nvl(v_task_spec.max_waittime,1)/utl.pkg_date.gc_mins_per_day) > v_task_spec.next_due_date )then
                      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                        gc_type_TIMECRITICAL||'-type was not given the opportunity to execute. '||
                        'It was supposed to start execution within '||v_task_spec.max_waittime||
                        ' minutes of '||to_char(v_task_spec.next_due_date,'YYYYMMDD HH24:MI')||
                        ' and is set to the DONE state as it it had actually run',null,v_task_spec.task_id);
                      set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
                    else              
                      if(scheduler_dep.is_dependencies_satisfied(v_task_spec))then                
                        set_task_state(v_task_spec,gc_state_READY,v_ref_date);
                        -- We need to exit out of the loop after we have launched a task
                        -- so that the states in the cursor can be refreshed.
                        -- This is important for dealing with peered groups and peered tasks.
                        exit;                        
                      end if;
                    end if;
                  else
                    -- No time expiry on this task
                    if(scheduler_dep.is_dependencies_satisfied(v_task_spec))then
                      set_task_state(v_task_spec,gc_state_READY,v_ref_date);
                      -- We need to exit out of the loop after we have launched a task
                      -- so that the states in the cursor can be refreshed.
                      -- This is important for dealing with peered groups and peered tasks.
                      exit;                                              
                    else
                      if( v_task_spec.max_waittime is not null 
                      and (v_ref_date-v_task_spec.state_tmstmp)*1440>v_task_spec.max_waittime )
                      then
                        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                          'Task timed out because predecessor tasks have not completed in time.',null,v_task_spec.task_id);
                        set_task_state(v_task_spec,gc_state_TIMEDOUT,v_ref_date);
                      end if;
                    end if;
                  end if;
                end if;
              end if;
            end if;
          end if;
        -- READY ===========================================
        -- Ready to be launched
        -- This state is also an entry point when restarting tasks that were interrupted
        -- under catastrophic circumstances, as well as for manually launching ad-hoc tasks
        elsif(v_task_spec.state=gc_state_READY)then
          if(v_scheduler_status<>gc_mode_QUIESCING and v_scheduler_status<>gc_mode_ABORTING)then
            -- Check if we have no exceeded the number of executing tasks on the scheduler
            -- Get the maximum number of tasks that the scheduler can handle .
            -- Default to the worst case of what a typical 2xCPU server could handle. 
            if(v_max_tasks is null)then
              v_max_tasks:=nvl(utl.pkg_config.get_variable_int(gc_config_max_tasks_key),5); 
            end if;          
            if(v_executing_tasks<v_max_tasks)then
              -- Not too many tasks executing
              if(is_task_modal(v_task_spec))then
                -- Modal task can only run when no other tasks are running
                if(not is_any_task_executing)then
                  -- Launch task
                  launch_task;
                  -- We need to exit out of the loop after we have launched a task
                  -- so that the states in the cursor can be refreshed.
                  exit;
                --else
                --  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Modal task is yielding to another modal task','Task Id '||v_task_spec.task_id);
                end if;
              else
                -- Other tasks can only run when no modal tasks are running
                if(not is_modal_task_running(v_task_spec))then
                  -- Launch task
                  launch_task;
                  -- We need to exit out of the loop after we have launched a task
                  -- so that the states in the cursor can be refreshed.
                  exit;                  
                --else 
                --  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Non-Modal task is yielding to another modal task','Task Id '||v_task_spec.task_id);
                end if;
              end if;              
            end if;
          end if;
        -- EXECUTING ===========================================
        elsif(v_task_spec.state=gc_state_EXECUTING)then
          -- Task currently executing.
          -- Check if the task is completed.
          -- If so, set the task state to DONE.        
          v_retcode:=get_completion_state(v_task_spec,v_ref_date);
          if(v_retcode=utl.pkg_exceptions.gc_success)then
            -- Task has SUCCESSFULLY completed
            -- Remove job from DBMS_JOB queue if it was a repeating job
            if(v_task_spec.repeats is not null and v_task_spec.repeat_interval is not null)then
              safe_job_remove(v_task_spec.queue_id);
            end if;
            set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
            v_executing_tasks:=v_executing_tasks-1;
          elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_dbms_job_fail)then
            -- The task code has become invalid. This will take a long time before DBMS_JOB sets the task 
            -- to BROKEN, which is why the scheduler interferes by removing the task from DBMS_JOB.        
            safe_job_remove(v_task_spec.queue_id);
            set_task_state(v_task_spec,gc_state_BROKEN,v_ref_date);
            v_executing_tasks:=v_executing_tasks-1;
          elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_broken)then
            -- DBMS_JOB had a problem managing the task
            set_task_state(v_task_spec,gc_state_BROKEN,v_ref_date);
            v_executing_tasks:=v_executing_tasks-1;
          elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_fail)then          
            -- Task completed with an ERROR
            if(v_task_spec.task_type=gc_type_PERSISTENT)then
              if(nvl(v_task_spec.repeat_count,0)<=v_task_spec.repeats)then                
                set_task_state(v_task_spec,gc_state_RETRY,v_ref_date);
              else
                if(v_task_spec.ignore_error='Y')then
                  -- We can ignore the error
                  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                    'Ignoring task execution error code '||v_task_spec.return_code,null,
                    v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_task_long);            
                  set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
                else              
                  set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
                end if;
              end if;
            else
              if(v_task_spec.ignore_error='Y')then
                -- We can ignore the error
                utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                  'Ignoring task execution error code '||v_task_spec.return_code,null,
                  v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_task_long);            
                set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
              else            
                set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
              end if;
            end if;                        
            -- Yank the job from DBMS_JOB in case it still exists
            safe_job_remove(v_task_spec.queue_id);
            v_executing_tasks:=v_executing_tasks-1;
          elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_lost)then
            -- Lost track of the task.
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,null,
              null,v_task_spec.task_id,
              utl.pkg_exceptions.gc_scheduler_task_lost);                      
            set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
          else
            -- Task is STILL RUNNNING
            -- Check if task has been running too long
            v_retcode:=is_task_run_too_long(v_task_spec, v_ref_date);
            if(v_retcode=utl.pkg_exceptions.gc_scheduler_task_long)then
              utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                'Task took longer than '||v_task_spec.max_runtime||
                ' minutes to run. It is now set to the '||gc_state_TIMEDOUT||' state.',null,
                v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_task_long);
              set_task_state(v_task_spec,gc_state_TIMEDOUT,v_ref_date);              
            end if;
          end if;
        -- ABORTING  ===========================================
        elsif(v_task_spec.state=gc_state_ABORTING)then
          -- Executing task has been set to terminate by an external process.
          -- Check when this is done and if timeout execeeded,
          -- remove task from job queue and assume that the task is ready to run again.
          -- Get configured Abort interval:
          if(v_abort_timeout is null)then
            v_abort_timeout:=nvl(utl.pkg_config.get_variable_int(gc_config_abort_int_key),10); -- minutes
          end if;   
          -- Keep count of aborting tasks
          v_aborting_tasks:=v_aborting_tasks+1;
          -- Check for time out
          if((v_ref_date-v_task_spec.state_tmstmp)*1440 > v_abort_timeout)then
            -- Timed out checking if the task has been completely removed from the scheduler.
            -- It probably has completed and we can get on with our lives.
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
              'The scheduler gave up waiting for the aborting task to be removed from the system and assumed that the task is not running any more.',
              null,v_task_spec.task_id);
            set_task_state(v_task_spec,gc_state_ABORTED,v_ref_date);
          else
            -- Check if task is still runnig
            if(v_task_spec.command_type=gc_command_type_EXTERNAL_PROC)then
              -- SHELL task
              if(is_os_process_done(v_task_spec,v_ref_date)=utl.pkg_exceptions.gc_success)then
                set_task_state(v_task_spec,gc_state_ABORTED,v_ref_date);
              end if;
            else
              -- SQL-based task
              if(is_sql_process_done(v_task_spec)=utl.pkg_exceptions.gc_success)then
                set_task_state(v_task_spec,gc_state_ABORTED,v_ref_date);
              end if;
            end if;
          end if;
        -- ABORTED  ===========================================
        --elsif(v_task_spec.state=gc_state_ABORTED)then
          -- Aborted. Do nothing until the task is manually reset by a user.
        --  null;          
        -- SUSPENDED ===========================================
        elsif(v_task_spec.state=gc_state_SUSPENDED)then
          -- To get a job out of the SUSPENDED state, it needs to be put into the RESUMED state.
          if(v_scheduler_status<>gc_mode_QUIESCING)then
            if(v_task_spec.prev_state=gc_state_EXECUTING)then
              v_retcode:=get_completion_state(v_task_spec,v_ref_date);
              if(v_retcode=utl.pkg_exceptions.gc_success)then
                set_task_state(v_task_spec,gc_state_DONE,v_ref_date);       -- remove DBMS_JOB queue item
                set_task_state(v_task_spec,gc_state_SUSPENDED,v_ref_date);  -- resumes state and save DONE in PREV_STATE
              elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_task_broken)then
                set_task_state(v_task_spec,gc_state_BROKEN,v_ref_date);     -- removed DBMS_JOB queue item
                set_task_state(v_task_spec,gc_state_SUSPENDED,v_ref_date);  -- resumes state and save DONE in PREV_STATE
              end if;
            end if;
          end if;
        -- RESUMED ===========================================
        elsif(v_task_spec.state=gc_state_RESUMED)then
          if(v_scheduler_status<>gc_mode_QUIESCING)then
            -- Job has been manually resumed after an error condition or after having manually been set to SUSPENDED.
            -- Set to READY if the task has not been launched (it will later be set to DUE if it is indeed due)
            -- Set to EXECUTING if the job is still running,
            -- Set to DONE if it has completed.
            if(v_task_spec.process_id is null)then
              -- Job has not been started to be executed during this task group
              -- before it was suspended, and is now resumed:
              set_task_state(v_task_spec,gc_state_WAITING,v_ref_date);
            else
              v_retcode:=get_completion_state(v_task_spec,v_ref_date);
              if(v_retcode=utl.pkg_exceptions.gc_success)then
                set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
              else
                set_task_state(v_task_spec,gc_state_EXECUTING,v_ref_date);
              end if;
            end if;
          end if;
        -- EXCLUDED ============================================
        elsif(v_task_spec.state=gc_state_EXCLUDED)then
          if(v_scheduler_status<>gc_mode_QUIESCING)then
            if(not scheduler_dep.is_mutual_task_running(v_task_spec))then
              -- Task that was excluded due to a peer task running can be restored to its original state
              set_task_state(v_task_spec,v_task_spec.prev_state,v_ref_date);
            end if;
            -- Job excluded for this coming run only. Will be un-excluded when past its due date.
            -- TODO: What the hell...?
            --if(is_task_due(v_task_spec,v_ref_date))then
            --  set_task_state(v_task_spec,gc_state_EXECUTING,v_ref_date);
            --  set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
            --  set_task_state(v_task_spec,gc_state_WAITING,v_ref_date);
            --end if;
          end if;
        -- BROKEN ============================================
        --elsif(v_task_spec.state=gc_state_BROKEN)then
          -- Job is broken
          -- This will manually be dealt with
          -- Ignore it.
          -- Manual intervention is required to fix this up.
        --  null;
        -- RETRY =============================================
        elsif(v_task_spec.state=gc_state_RETRY)then
          -- A PERSISTENT task is being retried by the scheduler. The task 
          -- remains in this state for the duration of the REPEAT_INTERVAL, 
          -- specified in minutes.
          if(v_task_spec.task_type=gc_type_PERSISTENT)then
            -- PERSISTENT task needs to repeat until successful or until REPEAT 
            -- executions have been performed. 
            -- If REPEAT_PERIODIC='Y' then restart a repeat based on the 
            -- NEXT_DUE_DATE and the REPEAT_INTERVAL.
            -- If REPEAT_PERIODIC='N' then restart a repeat based on the 
            -- NEXT_DUE_DATE and the FINISHED_AT date.
            -- Note: The REPEAT_COUNT is updated every time, at the beginning,
            --       that the task is executed and is reset when a task goes 
            --       into the initial WAITING state
            if(v_task_spec.repeat_periodic='Y')then
              -- Determine next attempt time based in a regular pattern based on NEXT_DUE_DATE
              v_next_retry_date:=v_task_spec.next_due_date + (nvl(v_task_spec.repeat_interval,1) -- 
                                                             *nvl(v_task_spec.repeat_count,1))
                                                             /utl.pkg_date.gc_mins_per_day;
            else                                                             
              -- Determine next attempt time based in the previous attempt completed
              v_next_retry_date:=v_task_spec.finished_at+(nvl(v_task_spec.repeat_interval,1)
                                                         *nvl(v_task_spec.repeat_count,1))
                                                         /utl.pkg_date.gc_mins_per_day;
            end if;                                                             
            if(v_ref_date>=v_next_retry_date)then
              -- Repeat interval has expired
              set_task_state(v_task_spec,gc_state_DUE,v_ref_date);              
              /* this is taken care of in the EXECUTING state
              if(nvl(v_task_spec.repeat_count,0)<v_task_spec.repeats  -- Have another attempt at running this task
              or v_task_spec.repeats is null) -- Repeat ad infinitum
              then                
                set_task_state(v_task_spec,gc_state_DUE,v_ref_date);
              else 
                -- Should not ever come to this. This is to prevent deadlock.
                utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
                  'PERSISTENT task deadlock detected in scheduler FSM',null,
                  v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_failure);                                
                if(v_task_spec.ignore_error='Y')then
                  -- We can ignore the error
                  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
                    'Ignore task error and pretend that the task has completed',null,
                    v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_failure);                                  
                  set_task_state(v_task_spec,gc_state_DONE,v_ref_date);
                else                    
                  set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
                end if;
              end if;
              */
            else
              -- Repeat interval is still Waiting. Do nothing.
              null;
            end if;
          end if;
        -- ERROR =============================================
        --elsif(v_task_spec.state=gc_state_ERROR)then
          -- Do nothing until the task has been manually reset.
        --  null;
        -- TIMEDOUT ===========================================
        elsif(v_task_spec.state=gc_state_TIMEDOUT)then
          -- Job timed out because predecessor jobs did not complete in time
          -- or because job is running too long.
          -- This will manually be dealt with
          -- Ignore it
          null;
        -- DONE ===========================================
        elsif(v_task_spec.state=gc_state_DONE)then
          if(v_scheduler_status<>gc_mode_QUIESCING and v_scheduler_status<>gc_mode_ABORTING)then
            -- Task completed
            -- Different way of dealing with DONE tasks when dealing with dependency trees:
            -- 1. Only when all the tasks in a tree are done, can they be moved away from the DONE state
            -- 2. When, in an FSM scan, the first task is found that is allowed to move away from the
            --   DONE state, then all the other tasks need to be simultaneously moved away from the DONE state.
            --   This has the consequence that the data in the cursor c_state is changed, so the FSM scan 
            --   needs to be exited. This is common practise in real-time systems.
            if(scheduler_dep.is_dependency_tree_done(v_task_spec,l_tasks_in_tree))then
              -- All dependent tasks in tree have completed - reset all tasks at once:
              i_task_spec:=l_tasks_in_tree.first;
              while(i_task_spec is not null)loop
                -- Get tree node
                n_task_spec:=l_tasks_in_tree(i_task_spec);
                if(n_task_spec.task_type=gc_type_VOLATILE)then
                  -- Remove task and peer definitions if the task was VOLATILE
                  -- Delete from scheduler
                  delete schedules
                   where task_id=n_task_spec.task_id;
                   -- Delete from task peers
                  delete task_peers
                   where task_peer1=n_task_spec.task_id
                      or task_peer2=n_task_spec.task_id;
                  -- Delete from group peers if this is the last remaining task of this group
                  delete task_group_peers
                   where (    group_peer1 = v_task_spec.group_name
                           or group_peer2 = v_task_spec.group_name
                         )
                     and ( select count(*)  
                             from schedules
                            where group_name = v_task_spec.group_name
                         ) = 0; -- This was the last VOLATILE task of this group name
                  commit;
                else
                  -- If this was a one-off task
                  if( v_task_spec.year   is not null and
                      v_task_spec.month  is not null and
                      v_task_spec.day    is not null and
                      v_task_spec.hour   is not null and
                      v_task_spec.minute is not null)
                  then
                    -- A one-off task: set to disabled so that it will not be considered again.
                    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
                                      'Task '||n_task_spec.task_id||' is a one-off task: Set to disabled so that it will not be considered again.',null,n_task_spec.task_id);
                    set_task_state(n_task_spec,gc_state_DISABLED,v_ref_date);
                  else
                    -- Recurring task: Determine when it needs to run again
                    v_retcode:=scheduler_due.calc_next_due_date(n_task_spec,v_ref_date,v_next_due_date);
                    if(v_retcode=utl.pkg_exceptions.gc_success)then
                      if(v_next_due_date is not null)then
                        set_task_state(n_task_spec,gc_state_WAITING,v_ref_date,v_next_due_date);
                      else    
                        -- Next_due_date is NULL and we have dependencies --> skip WAITING and go to DUE
                        set_task_state(n_task_spec,gc_state_DUE,v_ref_date);
                      end if;
                    elsif(v_retcode=utl.pkg_exceptions.gc_scheduler_special_day)then
                      -- Rare case: 
                      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,null,'SPECIAL_DAYS',v_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_special_day);
                      set_task_state(v_task_spec,gc_state_DISABLED,v_ref_date,v_next_due_date);                                  
                    else
                      -- Real Error
                      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
                                        'Task '||n_task_spec.task_id||' Next Due Date could not be calculated.',
                                         null,n_task_spec.task_id,utl.pkg_exceptions.gc_scheduler_inv_date);
                      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,scheduler_rep.task_dump(v_task_spec),'Task '||v_task_spec.task_id,v_task_spec.task_id,v_retcode,c_proc_name);
                      set_task_state(n_task_spec,gc_state_ERROR,v_ref_date,v_next_due_date);            
                    end if;
                  end if;
                end if;
                i_task_spec:=l_tasks_in_tree.next(i_task_spec);
              end loop;
              -- Exit from the FSM loop if we processed more than one task here
              --      since we could conflict with cursor c_state's view on all the other tasks' states
              if(l_tasks_in_tree.count>1)then
                exit;
              end if;
            end if;
          end if;
        -- DISABLED ===========================================
        --elsif(v_task_spec.state=gc_state_DISABLED)then
          -- Job has been disabled
          -- Ignore it.
        --  null;
        -- EDIT_LOCK ===========================================
        elsif(v_task_spec.state=gc_state_EDIT_LOCK)then
          -- Job is being edited
          -- If the editing time expires the maximum time, then the editing state
          -- is abandoned and task is set to previous state
          if(v_edit_timeout is null)then
            v_edit_timeout:=nvl(utl.pkg_config.get_variable_int(gc_config_edit_timeout_key),10); -- minutes
          end if;
          if(v_ref_date-v_task_spec.state_tmstmp>=v_edit_timeout/utl.pkg_date.gc_mins_per_day)then
            set_task_state(v_task_spec,v_task_spec.prev_state,v_ref_date);
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
              'The editing of the task is abandoned due to the editing operation lasting longer than '||v_edit_timeout||' minutes.',
              null,v_task_spec.task_id);                      
          end if;
        -- Unknown state ===========================================
        --else
          -- Unknown state
          -- Set to ERROR state
          -- This will manually be dealt with
        --  set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
        --  raise utl.pkg_exceptions.e_scheduler_inv_state;
        end if;
      -- Task-level exception handling
      exception
        when others then
          v_error_count:=v_error_count+1;
          utl.pkg_errorhandler.handle;
          utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,null,'State: '||v_task_spec.state,v_task_spec.task_id,sqlcode);          
          utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,scheduler_rep.task_dump(v_task_spec),null,v_task_spec.task_id,sqlcode,c_proc_name);
          -- Set the task state to ERROR
          set_task_state(v_task_spec,gc_state_ERROR,v_ref_date);
          if(v_error_count>5)then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
              'An Excessive number of task-level exceptions have occurred. Raising a Scheduler Failure exception.');
            raise utl.pkg_exceptions.e_scheduler_failure;
          end if;            
      end;
    end loop;
    
    close c_state;
  
    if(v_scheduler_status=gc_mode_QUIESCING and v_executing_tasks=0)then
      set_status(gc_mode_QUIESCED);
    else
      if(v_scheduler_status=gc_mode_ABORTING and v_aborting_tasks=0)then
        set_status(gc_mode_ABORTED);
      end if;
    end if;
    
    -- Update system test injection time
    v_end_time:= dbms_utility.get_time();
    v_scan_duration:=(v_end_time-v_start_time)*10; -- in milliseconds
    if(v_artificial_time)then      
      utl.pkg_config.set_variable_date(gc_config_artifical_date_key,v_ref_date+(v_scan_duration/1000)/utl.pkg_date.gc_secs_per_day);
    end if;

    -- Update last FSM scan time stamp
    utl.pkg_config.set_variable_date(gc_config_last_fsm_scan_key,v_ref_date);
    -- Update FSM scan duration
    utl.pkg_config.set_variable_int(gc_config_scan_duration_key,v_scan_duration);
    
  exception
    when utl.pkg_exceptions.e_scheduler_failure then
      raise;
    when others then
      utl.pkg_logger.log;
      v_error_count:=v_error_count+1;
  end;
  -- Got too many small failures - raise a BIG failure
  if(v_error_count>5)then
    raise utl.pkg_exceptions.e_scheduler_failure;
  end if;  

  -- Let outside world know that we are running 
  tx_heartbeat('* FSM Scan:'||to_char(v_scan_duration)||'ms');
  
  dbms_application_info.set_module(null,null);
  -- Scheduler-level exception handling
exception
  when others then
    utl.pkg_errorhandler.handle;  
    utl.pkg_errorhandler.log_sqlerror;  -- Sends an incident email
    dbms_lock.sleep(60);  -- Make sure that we do not flood the recipient's email box
end fsm;

-------------------------------------------------------------------------------
-- Get task details
--
-- Parameters:
--  Specify task_id in p_task_spec
--  All task parameters returned in p_task_spec
--
-- Returns:
--  0 on success
--  gc_scheduler_task_exist if task does not exist
function get_task_details(p_task_spec in out t_schedule_rec) return UTL.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_task_details';
  v_task_id           number:=p_task_spec.task_id;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select *
    into p_task_spec
    from vw_fsm_tasks s
   where s.task_id = v_task_id;
  dbms_application_info.set_module(null,null);
  return utl.pkg_exceptions.gc_success;
exception
  when no_data_found then
    return utl.pkg_exceptions.gc_scheduler_task_exist; 
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return utl.pkg_exceptions.gc_scheduler_task_exist;
end get_task_details;

-- Set the state of a batch job.
-- This function will be called by the FSM only when a change of state has occurred.
-- Updates the date stamp when the job has been started or resumed.
--
-- TODO: p_task_spec should reflect that changes made to it.
--       Check all code that uses "set_task_state"?
--
-- TODO: Add message queueing to avoid concurrency problems
--
procedure set_task_state(
  p_task_spec     in  out t_schedule_rec,
  p_desired_state in      schedules.state%type,    -- Desired state
  p_ref_date      in      date:=sysdate,
  p_next_due_date in      date:=null
)
is
  pragma autonomous_transaction;
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.set_task_state';
  v_desired_state     schedules.state%type:=p_desired_state;
  v_prev_state        schedules.state%type:=nvl(p_task_spec.state,gc_state_INITIAL);  -- collect previous state
  v_msg               varchar2(2000);
  v_task_log_row      task_log%rowtype;
  --v_current_user      varchar2(30);
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_desired_state not in (
    gc_state_INITIAL             ,
    gc_state_WAITING             ,
    gc_state_DUE                 ,
    gc_state_READY               ,
    gc_state_EXECUTING           ,
    gc_state_ABORTING            ,
    gc_state_ABORTED             ,
    gc_state_SUSPENDED           ,
    gc_state_RESUMED             ,
    gc_state_EXCLUDED            ,
    gc_state_BROKEN              ,
    gc_state_ERROR               ,
    gc_state_RETRY               ,
    gc_state_DONE                ,
    gc_state_TIMEDOUT            ,
    gc_state_DISABLED            ,
    gc_state_UNDEFINED           ,
    gc_state_EDIT_LOCK))
  then
    utl.pkg_logger.error(utl.pkg_exceptions.gc_scheduler_inv_state);
    v_desired_state := gc_state_ERROR;
  end if;

  -- Simplistic locking logic
  -- TODO: USE DBMS_LOCK to prevent deadlock between automatic and manual state overrides
  if(v_desired_state=gc_state_EDIT_LOCK and p_task_spec.state=gc_state_EDIT_LOCK)then
    utl.pkg_logger.warn(utl.pkg_exceptions.gc_scheduler_task_edit_lock);
    return;
  end if;

  if(v_desired_state=gc_state_EDIT_LOCK and p_task_spec.state=gc_state_EXECUTING)then
    utl.pkg_logger.warn(utl.pkg_exceptions.gc_scheduler_task_busy);
    return;
  end if;

  -- Now set task state
  -- ~~~~~~~~~~~~~~~~~~
   
  -- RESUMES ================================    
  if(v_desired_state=gc_state_RESUMED)then
    -- Special case to deal deal with concurrency problems between FSM and MANUAL state setting.
    -- Normally use a message queueing mechanism to prevent these sort of problems.
    -- Need to add similar mechanisms for other states that are manually set from external points.
    if(  p_task_spec.state=gc_state_ERROR
      or p_task_spec.state=gc_state_SUSPENDED
      or p_task_spec.state=gc_state_TIMEDOUT
      or p_task_spec.state=gc_state_BROKEN
      or p_task_spec.state=gc_state_DISABLED
      or p_task_spec.state=gc_state_UNDEFINED)
    then    
      update schedules
         set state        = v_desired_state,
             state_tmstmp = p_ref_date,
             prev_state   = v_prev_state
       where TASK_ID = p_task_spec.TASK_ID;
    else
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,null,null,null,utl.pkg_exceptions.gc_scheduler_task_resume);
    end if;
  -- WAITING ================================    
  elsif(v_desired_state=gc_state_WAITING)then
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           next_due_date= nvl(p_next_due_date,next_due_date),
           process_id   = null,
           repeat_count = 1,                 -- Reset for PERSISTENT-type and repeating tasks 
           dependency_sql = p_task_spec.dependency_sql
     where TASK_ID = p_task_spec.TASK_ID;
     p_task_spec.repeat_count:=1; -- should really re-read task from table
  -- DUE  ============================
  elsif(v_desired_state=gc_state_DUE)then  
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           return_code  = null,               -- Reset transcient task values
           process_id   = null,               -- 
           queue_id     = null,               --                       
           dependency_sql = p_task_spec.dependency_sql
     where TASK_ID = p_task_spec.TASK_ID; 
  -- READY ================================    
  elsif(v_desired_state=gc_state_READY)then     
      update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           return_code  = null,               -- Reset transcient task values
           process_id   = null,               -- 
           queue_id     = null                --            
     where TASK_ID = p_task_spec.TASK_ID;
  -- EXECUTING ================================    
  elsif(v_desired_state=gc_state_EXECUTING)then
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           return_code  = decode(p_task_spec.command,null,0,null),  -- need to deal with Dummy Tasks
           started_at   = p_ref_date,
           queue_id     = p_task_spec.queue_id,
           repeat_count = repeat_count+1      
     where TASK_ID = p_task_spec.TASK_ID;
  -- DONE ================================    
  elsif(v_desired_state=gc_state_DONE)then
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           finished_at  = p_ref_date,
           repeat_count = null
     where TASK_ID = p_task_spec.TASK_ID;
  -- BROKEN ================================    
  elsif(v_desired_state=gc_state_BROKEN)then
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           finished_at  = p_ref_date,
           return_code  = p_task_spec.return_code
     where TASK_ID = p_task_spec.TASK_ID;
  -- INITIAL ================================    
  elsif(v_desired_state=gc_state_INITIAL)then   
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,               
           return_code  = null,               -- Reset transcient task values
           process_id   = null,               -- 
           queue_id     = null,               --       
           repeat_count = null,               --   
           finished_at  = null,               --
           started_at   = null                --
     where TASK_ID = p_task_spec.TASK_ID;
    p_task_spec.repeat_count:=null;
  -- RETRY ================================   
  elsif(v_desired_state=gc_state_RETRY)then   
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state
     where TASK_ID = p_task_spec.TASK_ID;
    if(p_task_spec.task_type=gc_type_PERSISTENT)then      
     v_msg:='Task p_task_spec.task_id returned an error code of '||p_task_spec.return_code||
            '. The scheduler will re-attempt this task in '||p_task_spec.repeat_interval||
            ' minutes. You may reset this task for an immediate retry, which will also reset the '||
            'retry count of '||p_task_spec.repeat_count||' so far.';
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_warn,v_msg,null,p_task_spec.task_id,p_task_spec.return_code);
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,scheduler_rep.task_dump(p_task_spec),null,p_task_spec.task_id,p_task_spec.return_code);                
    end if;
  -- ERROR ================================    
  elsif(v_desired_state=gc_state_ERROR)then     
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state,
           finished_at  = p_ref_date
     where TASK_ID = p_task_spec.TASK_ID;
     
    -- Log as much as possible about this task 
    -- if we are not returning from an EDITING operation
    if(v_prev_state<>gc_state_EDIT_LOCK)then
      v_msg:= 'The scheduler has determined that Task Id '||p_task_spec.task_id||
              ', Name '||p_task_spec.group_name||':'||p_task_spec.operation_id||' caused an error. '||
              'The conditions surrounding the error should be investigated. '||
              'More informationcan be found in the application log immediately preceeding this event. '|| 
              ' The task''s reference date has not been altered and is still set to '||
              to_char(p_task_spec.next_due_date,'YYYYMMDD HH24:MI');
      if(p_task_spec.task_type=gc_type_PERSISTENT)then
        if(p_task_spec.repeat_count>=p_task_spec.repeats)then
          v_msg:=v_msg||'The scheduler has unsuccessfully re-attempted this task '||p_task_spec.repeat_count||
                        ' times. The task needs to be manually reset for the task to be re-attempted'||chr(10);
        end if;
      else 
        v_msg:=v_msg||'The scheduler will not re-attempt to run this task any more after '||p_task_spec.repeat_count||
                      '. counts. The task needs to be manually reset for the task to be re-attempted.'||chr(10);
      end if;    
      v_msg:=v_msg||'Use the following console command to reset this task:'||chr(10)||'  $ task.reset '||p_task_spec.task_id;
      utl.pkg_errorhandler.log_error(p_task_spec.return_code,v_msg,null,p_task_spec.task_id,true);
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,scheduler_rep.task_dump(p_task_spec),null,p_task_spec.task_id,p_task_spec.return_code,c_proc_name);
    end if;
  -- OTHER STATES ============================
  else
    update schedules
       set state        = v_desired_state,
           state_tmstmp = p_ref_date,
           prev_state   = v_prev_state
     where TASK_ID = p_task_spec.TASK_ID;
  end if;
  
  -- Clean up job queue
  if(v_desired_state=gc_state_BROKEN)then
    begin
      dbms_job.remove(p_task_spec.process_id);
    exception
      when others then
         null;
     end;
  end if;
  
  -- Heartbeat and tasklog
  if(trim(v_desired_state)<>trim(p_task_spec.state))then
    if(p_task_spec.modal='Y')then
      tx_heartbeat(p_task_spec.task_id||'->'||v_desired_state||'(MODAL)');        
      if(v_desired_state=gc_state_EXECUTING)then
        v_task_log_row.modal:='BEGIN';
      elsif(v_desired_state  in (gc_state_DONE,
                                 gc_state_RETRY,
                                 gc_state_ERROR,
                                 gc_state_ABORTING,
                                 gc_state_TIMEDOUT,
                                 gc_state_DISABLED,
                                 gc_state_UNDEFINED))
      then        
        v_task_log_row.modal:='END';      
      end if;
    else
      tx_heartbeat(p_task_spec.task_id||'->'||v_desired_state);      
    end if;
  
    -- Add entry to task_log table
    v_task_log_row.task_id:=p_task_spec.task_id;
    v_task_log_row.state:=v_desired_state;        
    v_task_log_row.scheduled_time:=p_task_spec.next_due_date;
    v_task_log_row.queue_id:=p_task_spec.queue_id;
    v_task_log_row.repeat_count:=p_task_spec.repeat_count;  
    v_task_log_row.started_at:=p_ref_date;
    v_task_log_row.scheduled_time :=p_task_spec.next_due_date;    
    insert_task_event(v_task_log_row);
  
  end if;

  commit;   -- autonomously

  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end set_task_state;

-- Gets the state of a batch job
-- Returns null if no state has been assigned to the batch job yet.
function get_task_state( p_task_id  in  schedules.TASK_ID%type) return schedules.state%type
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_task_state';
  v_state             schedules.state%type := null;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select state
    into v_state
    from schedules
   where TASK_ID = p_TASK_ID;
  dbms_application_info.set_module(null,null);
  return v_state;
exception
  when no_data_found then
    return v_state;
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end get_task_state;

------------------------------------------------------------------------------
-- Gets previous state of a task
-- If no previous state exists or task does not exist, return NULL
function get_task_previous_state(p_task_id in schedules.task_id%type)
return schedules.state%type
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_task_previous_state';
  v_prev_state        schedules.prev_state%type;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select s.prev_state
    into v_prev_state
    from schedules s
   where TASK_ID = p_TASK_ID;
  dbms_application_info.set_module(null,null);
  return v_prev_state;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end get_task_previous_state;

------------------------------------------------------------------------------
-- Get the task Id for the friendly group name and operation Id tuple
function get_task_ids(p_group_name in schedules.group_name%type,
                      p_operation_id in schedules.operation_id%type)
return dbms_sql.Number_Table
is
  c_proc_name   constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_task_ids';
  l_task_ids dbms_sql.Number_Table;
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Look up primary keys
  select s.task_id
    bulk collect
    into l_task_ids
    from schedules s
   where upper(s.group_name) = upper(p_group_name)
     and s.operation_id = p_operation_id;
  dbms_application_info.set_module(null,null);
  return l_task_ids;
exception
  when others then
    return l_task_ids;  -- although empty
end get_task_ids;

------------------------------------------------------------------------------
-- Checks if the task has completed by the arrival of a non-null value in 
-- in the RETURN_CODE field
-- Returns gc_succcess if the task has completed, e.g. it is not there any more
-- Returns gc_scheduler_task_busy if job still busy executing
-- Returns gc_scheduler_task_broken if there was problem.
function get_completion_state(p_task_spec in t_schedule_rec, p_ref_date in date)
return utl.global.t_error_code
is
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  v_job               sys.user_jobs.job%type;
  v_broken            sys.user_jobs.broken%type;
  v_oscode            pls_integer;
  v_failures          sys.user_jobs.failures%type;  
begin
  if(p_task_spec.return_code is null)then
    -- Because a return is not there yet, the task is probably still executing
    -- Check that the task has not hung DBMS_JOB:
    begin
      select job,broken,failures
        into v_job,v_broken,v_failures
        from sys.user_jobs
       where job = p_task_spec.queue_id;
      if(v_broken='Y')then
        -- This DBMS_JOB is broken - there was a compilation problem on this task
        -- at the point of when it was submitted
        return utl.pkg_exceptions.gc_scheduler_task_broken;
      elsif(v_failures>0)then
        -- The job caused a failure in DBMS_JOB 
        -- It may have compiled successfully when it was submitted on DBMS_JOB, 
        -- but when it was run a compilation error occurred.
        v_retcode:=utl.pkg_exceptions.gc_scheduler_dbms_job_fail;
      else
        -- Job still on dbms_job queue - could be busy or broken
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy;        
      end if;  
    exception
      when no_data_found then
        -- This is an unlikely scenario but needs to be considered:
        -- The task has already been removed from DBMS_JOB even though no return code was lodged.
        -- Possible causes are:
        -- 1. The task entry may have been manually removed from the DBMS_JOB queue.
        -- 2. The task executed so quickly that it did not have time to lodge a PID to the scheduler
        --    (in the case of O/S Shell tasks)
        -- 
        -- Prematurely considering that this task has actually completed can 
        -- have undesirable consequences so to resolve this situation we wait 
        -- an arbitary time and then consider the task complete.
        --
        -- Wait at least two minutes before deciding that the task has complete
        if(p_ref_date-p_task_spec.started_at<=2/1440)then
          v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy; 
        else
          -- In rare cases the following may happen:
          -- DBMS_JOB has already removed the job from the dbms_job queue, 
          -- but the job may still be executing. 
          -- Final Check if the O/S process is running
          if(p_task_spec.command_type=gc_command_type_EXTERNAL_PROC)then
            if(p_task_spec.process_id is not null)then
              v_oscode:=utl.hostcmd('ps -ef | grep '||p_task_spec.process_id||' | grep -v grep  > /dev/null');
              if(v_oscode=0)then
                -- A O/S process is still to be found. 
                v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy;
              else
                -- The task has completely disappeared. Assume that it failed.
                v_retcode:=utl.pkg_exceptions.gc_scheduler_task_lost;
              end if;            
            end if;
          /*
          else
            -- TODO: Try and get the O/S PID for an Oracle-based procedure        
            --       See how this is done in the ABORT function
            -- The task has completely disappeared. Assume that it failed.
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_lost;              
            */
          end if;              
        end if;
    end;        
  else
    -- A return code was registered
    if(p_task_spec.task_type<>gc_type_PERSISTENT)then
      -- This is a non persistent task
      if (p_task_spec.repeats is not null
      and p_task_spec.repeat_interval is not null
      and p_task_spec.repeat_count<=p_task_spec.repeats)
      then      
        -- A REPATING task
        if(p_task_spec.return_code=0)then
          -- A repeating task is still in the process of performing multiple executions
          v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy;          
        else         
          -- TODO:
          -- Shouldn't asynchronously react to a failed task since DBMS_JOB 
          -- could in the mean time have started the next repeat. 
          -- Need to message queue between the scheduler FSM and DBMS_JOB!
          v_retcode:=utl.pkg_exceptions.gc_scheduler_task_fail;
        end if;
      else
        -- A NON-REPEATING task
        if(p_task_spec.return_code=0)then
           v_retcode:=utl.pkg_exceptions.gc_success;
        else
          v_retcode:=utl.pkg_exceptions.gc_scheduler_task_fail;
        end if;
      end if;
    else
      -- PERSISTENT task    
      if(p_task_spec.return_code=0)then
        v_retcode:=utl.pkg_exceptions.gc_success;
      else
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_fail;
      end if;
    end if;
  end if;
  
/*  
    -- A return code was lodged against the task, after an execution cycle completed.    
    -- Check that no multiple executions ("repeat tasks") are in process    
    if (p_task_spec.repeats is not null
    and p_task_spec.repeat_interval is not null
    and p_task_spec.repeat_count<p_task_spec.repeats
    and p_task_spec.task_type<>gc_type_PERSISTENT)
    then
      -- A repeating task is still in the process of performing multiple executions
      v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy;  
    else
      -- Non-repeating task or repeating task that has completed all its executions.
      -- The completion status of a repeating task is judged on the state of the 
      -- last task execution only.
      if(p_task_spec.return_code=0)then
        v_retcode:=utl.pkg_exceptions.gc_success;
      else
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_fail;
      end if;
    end if;
  end if;
*/    


  return v_retcode;  
exception
  when others then
    -- Something seriously wrong here 
    return utl.pkg_exceptions.gc_scheduler_task_broken;
end get_completion_state;

-- Get the number of executing tasks.
-- On error, returns the maximum number possible
function get_num_executing_tasks return pls_integer is
  --c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_num_executing_tasks';
  v_count         pls_integer;
begin
  select count(*)
    into v_count
    from schedules
   where state = gc_state_EXECUTING;
  return v_count;
exception
  when others then
    return gc_max_executing_tasks;
end get_num_executing_tasks;

------------------------------------------------------------------------------
-- Resumes a task that was suspended from where it left off, due to a
-- max run time timeout or a timeout on the dependencies.
-- This will be manually called from an operator.
-- The checking of pre-conditions is performed here much in the same way that it is
function resume_task(p_TASK_ID in schedules.TASK_ID%type, p_sysdate in date) return UTL.global.t_error_code
is
  c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.resume_task';
  v_retcode       UTL.global.t_error_code := utl.pkg_exceptions.gc_success;
  v_task_spec      t_schedule_rec;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select *
    into v_task_spec
    from vw_fsm_tasks
   where TASK_ID = p_TASK_ID;
  if(   v_task_spec.state=gc_state_ERROR
     or v_task_spec.state=gc_state_SUSPENDED
     or v_task_spec.state=gc_state_TIMEDOUT
     or v_task_spec.state=gc_state_BROKEN
     or v_task_spec.state=gc_state_DISABLED
     or v_task_spec.state=gc_state_UNDEFINED)
  then
    set_task_state(v_task_spec,gc_state_RESUMED,p_sysdate);
  else
    v_retcode:=utl.pkg_exceptions.gc_scheduler_task_resume;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise utl.pkg_exceptions.e_scheduler_task_exist;
end resume_task;

-------------------------------------------------------------------------------
-- Checks if a job is due to be executed for the given scheduler date.
-- Parameters:  p_task_spec         All the task details
--              p_ref_time          The scheduler time reference
-- This function will be called when the state of the job is READY.
-- Returns: TRUE if DUE
--          FALSE if not DUE or if there was an error
function is_task_due(p_task_spec in t_schedule_rec,p_sysdate in date)
return boolean
is
  c_proc_name         constant varchar2(100) :=pc_schema||'.'||pc_package||'.is_task_due';
  v_retcode           boolean;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_sysdate>=p_task_spec.next_due_date)then
    v_retcode:=true;
  else
    v_retcode:=false;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end is_task_due;

------------------------------------------------------------------------------
-- Determines if a job has been running too long after it has started to execute
-- based on the max_runtime value specified in minutes.
-- Return gc_success                        if not timed out
--        gc_scheduler_task_too_long        if timed out or on exception
function is_task_run_too_long(p_task_spec in t_schedule_rec, p_sysdate in date)
return UTL.global.t_error_code
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.is_task_run_too_long';
  v_retcode     UTL.global.t_error_code := utl.pkg_exceptions.gc_success;
  v_runtime     pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Only check if we are in EXECUTING state and if there is a timeout value specified at all
  if(p_task_spec.state = gc_state_EXECUTING and p_task_spec.max_runtime is not null)then
    v_runtime:=(p_sysdate-p_task_spec.state_tmstmp)*1440; -- in munutes
    if(v_runtime>p_task_spec.max_runtime)then
      v_retcode:=utl.pkg_exceptions.gc_scheduler_task_long;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return sqlcode;
end is_task_run_too_long;

-------------------------------------------------------------------------------
-- Checks if the OS process is still runnning
-- This function does not set any states.
-- Returns: gc_success             if the job has completed, because either:
--            * No PID was registered since the task executed so quickly
--            * No trace of the job could be found in the `ps` command
--          gc_scheduler_task_busy if job still busy executing
--          gc_scheduler_trancient_state is stll waiting for the PID to be updated
function is_os_process_done(p_task_spec in t_schedule_rec, p_ref_date in date)
return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100) := pc_schema||'.'||pc_package||'.is_os_process_done';
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  v_os_code           pls_integer;
  v_cmd               varchar2(200);
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_spec.process_id is not null)then
    v_cmd:='ps -ef | grep "'||p_task_spec.command||'" | awk ''{print $2}'' | grep '||p_task_spec.process_id||' > /dev/null';
    v_os_code:=utl.hostcmd(v_cmd);
    if(v_os_code=0)then
      -- The process was found and it is still running/suspended in the O/S
      v_retcode:= utl.pkg_exceptions.gc_scheduler_task_busy;
    end if;
    -- Else the process does not exist and we can conclude that the process has therefore completed.
  else
    -- The process Id has not been updated yet, 
    -- possibly because the task was launched 'a moment ago'
    if(p_task_spec.state_tmstmp+1/1440>p_ref_date)then
      v_retcode:= utl.pkg_exceptions.gc_scheduler_trancient_state;
      -- else..
      -- Failsafe: The launch failed to register a PID for some reason
      --           Assume that the task has completed
    end if;        
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end is_os_process_done;

-------------------------------------------------------------------------------
-- Checks if the Oracle process is still runnning
-- This function does not set any states.
-- Returns: gc_success             if the job has completed, because either:
--            * No PID was registered since the task executed so quickly
--            * No trace of the job could be found in the `ps` command
--          gc_scheduler_task_busy if job still busy executing
function is_sql_process_done(p_task_spec in t_schedule_rec)
return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100) := pc_schema||'.'||pc_package||'.is_sql_process_done';
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  v_os_code           pls_integer;
  v_cmd               varchar2(1000);
  --v_count             pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
   v_cmd:=
'ORACLE_SID=<ORACLE_SID> sqlplus -s / <<!
var retcode number
begin
  select count(*)
    into :retcode
    from dba_jobs_running jr,
         v$session s
   where jr.job='||p_task_spec.queue_id||'
     and s.sid=jr.sid;
exception
  when others then
    :retcode:=sqlcode;
    utl.pkg_logger.log;
end;
quit :retcode
'||chr(47)||'
!
RETCODE=$?
exit $RETCODE
';  
  fit_unix_environment(v_cmd);
  v_os_code:=utl.hostcmd(v_cmd);  
  if(v_os_code<>0)then 
    v_retcode:=utl.pkg_exceptions.gc_scheduler_task_busy;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end is_sql_process_done;

-------------------------------------------------------------------------------
-- Checks if task is modal
function is_task_modal(p_task_spec in t_schedule_rec) return boolean
is
  c_proc_name         constant varchar2(100) :=pc_schema||'.'||pc_package||'.is_task_modal';
  v_retcode           boolean:=false;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_spec.modal='Y')then
    v_retcode:= true;
  end if;
  dbms_application_info.set_module(null,null);  
  return v_retcode;
end is_task_modal;

-------------------------------------------------------------------------------
-- Checks if any modal tasks are running
function is_modal_task_running(p_task_spec in t_schedule_rec) return boolean
is
  c_proc_name         constant varchar2(100) :=pc_schema||'.'||pc_package||'.is_modal_task_running';
  v_retcode           boolean:=false;
  v_count             pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select count(*)
    into v_count
    from schedules    
   where state = gc_state_EXECUTING
     and task_id <> p_task_spec.task_id
     and modal = 'Y';
  if(v_count>0)then
    v_retcode:=true;
  end if;    
  dbms_application_info.set_module(null,null);  
  return v_retcode;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;        
    raise;
end is_modal_task_running;

-------------------------------------------------------------------------------
-- Checks if any tasks (modal or not) are running at all
function is_any_task_executing return boolean
is
  c_proc_name         constant varchar2(100) :=pc_schema||'.'||pc_package||'.is_any_task_executing';
  v_retcode           boolean:=false;
  v_count             pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select count(*)
    into v_count
    from schedules    
   where state = gc_state_EXECUTING;
  if(v_count>0)then
    v_retcode:=true;
  end if;    
  dbms_application_info.set_module(null,null);  
  return v_retcode;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;        
    raise;
end is_any_task_executing;

-- Substitute with configuration variables and environment values
-- Values to be substituted are:
-- <LOGGING>      with ">> configured_log_file"
-- <TMP_DIR>      with "configured_temp_directory"
-- <ORACLE_SID>   with current instance name.
-- <EXPORTS>      with all UNIX environment variables specifed in the config table
--                It is a VBT to add the $ORACLE_SID to the environment variables.
procedure fit_unix_environment(p_cmd in out varchar2)
is
  -- Substitute env vars in shell script
  -- Order of cursor is important!
  cursor c_env_vars is 
    select *
      from utl.config
     where variable like '$%'
     order by length(variable) desc;
  -- Scheduler log file - the file will in all likelyhood be owned by user 'oracle', but check first!
  v_log_file            varchar2(250):=utl.pkg_config.get_variable_string(gc_config_log_file_key);
  -- Temporary directory
  v_tmp_dir             varchar2(250):=nvl(utl.pkg_config.get_variable_string(gc_config_temp_dir_key),'/tmp');
  -- This ORACLE Instance name
  v_oracle_sid          varchar2(60) := utl.pkg_system.get_instance_name;    
  v_envs                varchar2(2000);
begin
    -- Enable output to logging 
    if(v_log_file is null)then
      p_cmd:=replace(p_cmd,'<LOGGING>','');        
    else
      p_cmd:=replace(p_cmd,'<LOGGING>',' >> '||v_log_file);        
    end if;    
    -- Get SID
    p_cmd:=replace(p_cmd,'<ORACLE_SID>',v_oracle_sid);
    -- Set up the temporary file directory
    p_cmd:=replace(p_cmd,'<TMP_DIR>',v_tmp_dir);

    -- Look for all defined environment variables in the config tables - they start with '$' -
    -- and replace in the file, starting with the largest one
    -- Evaluation Order: 1. Strings 2. Integer 3: float 4: date in YYYYMMDD format
    -- Yes, this is crude - Redo this to using RegEx from 10g next time
    for c in c_env_vars loop
      p_cmd:=replace(p_cmd,c.variable,
                nvl(c.string_value,
                    nvl(c.int_value,
                        nvl(c.float_value,to_char(c.date_value,'YYYYMMDD')))));  
      -- Also make up the environment export 
      v_envs:='export '||substr(c.variable,2)||'='||
        nvl(c.string_value,
            nvl(c.int_value,
                nvl(c.float_value,to_char(c.date_value,'YYYYMMDD'))))||chr(10);
                          
    end loop;
    -- Environment Exports 
    p_cmd:=replace(p_cmd,'<EXPORTS>',v_envs);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end fit_unix_environment;

    
------------------------------------------------------------------------------
-- Makes up the sql code to run the job and submit it to the dbms_job queue.
-- The JOB column is updated to the job number that DBMS_JOB
-- assigned to the job when it was submitted and a local Task launch Id is 
-- assigned to the task execution. This is of the same cardinality as that of
-- the job Id returned from dbms_job.submit, but this Id is only known after 
-- the job is actually submitted to dbms_job, so is useless for what we need 
-- to do here. Our own execution Id is returned p_task_lauch_id.
--
-- Returns SUCCESS if the job has completed.
-- Returns gc_scheduler_task_submit job submission failed
--
-- Coding help:
-- Embedded code / here-doc is on the left margin,
-- and some of it is also underlined
function submit_task(p_task_spec in out t_schedule_rec, p_fsm_date in date) 
return UTL.global.t_error_code
is
  pragma autonomous_transaction;
  c_proc_name           constant varchar2(100) := pc_schema||'.'||pc_package||'.submit_task';
  v_retcode             UTL.global.t_error_code:= utl.pkg_exceptions.gc_success;  
  v_task_lauch_id       pls_integer:=get_next_execution_id;     -- Execution code assigned to this task's submission
  v_job_submission      varchar2(4000);   -- Eventual code submitted to DBMS_JOB
  v_wrapper_file_name   varchar2(250);    -- Name of script file that contains all wrapper code to command
  v_task_log_row        task_log%rowtype;

  -- Constructs the ksh wapper code that is wrapped up in the DBMS_JOB job submission code
  -- For this to work, job monitoring should be switched on. If this is not
  -- switched on, add the following to user ORACLE's .profile file:
  -- set -o monitor
  --
  -- This KORN Shell script extracts the PID of the launched process
  -- and updates the scheduler. There are many ways available to get 
  -- the PID after a launch, only one will work:
  -- PID=$(ps -ef | grep $! | grep  -v grep | awk '{print $2}')
  -- The reason that $! cannot be used directly is that in the Oracle 
  -- external proc environment, $! is the PID of the wrapper script, not
  -- of the launched process (this presumably has something to do with 
  -- the Oracle External Procedure handling shell?).
  -- Since the shell lasts as long as process, the actual
  -- PID of the process that we are interested in is uniquely paired with
  -- the Oracle shell's PID.
  --
  -- The temporary wrapper code is created in this file on the O/S
  -- Can't use application-specific environment variables (e.g. $APP_HOME)
  -- in the file name since the file is invoked as user 'oracle' who is not
  -- aware of our environment  - so we have to specify an absolute path:
  --
  -- Shortcomings:
  -- 1. The PID of the external process is not captured when a short process is run.
  --    This is not a problem, since the PID is only used when the task is aborted.
  --    Since the task has completed, there is nothing to abort, so we are OK.
  -- 2. External procedures are run as user 'oracle'. Either this user needs to be
  --    made aware of the application environment variables, or the task needs to
  --    needs to refer to absolute paths etc... The latter is solution is preferred
  --    since the application's interference with the Oracle environment is a
  --    minimal listener configuration change. To make this work, the application
  --    path is a configuration parameter 'ApplicationHome'.
  --    If this is not defined, then it is assumed that the external script is on
  --    'oracle's path.
  --
  --  Reserved key values that may be used by commands and are substitiuted at 
  --  launch time:
  --    <TASK_ID>      Task Id in this scheduler
  --    <ORACLE_SID>   Name of this Oracle Instance
  --    Any Unix environement variable
  function make_unix_external_command return varchar2 is
    v_wrapper_command varchar2(4000);
    
    function make_ksh_wrapper return varchar2 is
      v_ksh_wrapper         varchar2(4000);      
    begin
      -- Make up command that will create the wrapper script.
      -- Note the abhorence of ampersants - chr(38) and the general avoidance of 
      -- bangs - chr(33), and hashes - chr(35), so that this file can be loaded in SQL
      -- WARNING: Don't touch his embedded code unless you know what you are doing!
      v_ksh_wrapper:='cat > '||v_wrapper_file_name||'<<EOF
'||chr(35)||chr(33)||'/bin/ksh
TMP_FILE=<TMP_DIR>/err_<TASK_ID>\$\$
export ORACLE_SID=<ORACLE_SID>
<EXPORTS>
'||chr(35)||' Background process and capture non-zero error code: 
'||chr(35)||' =========================================================
<COMMAND> <LOGGING> 2>'||chr(38)||'1 || echo \$? > \$TMP_FILE '||chr(38)||'
'||chr(35)||' =========================================================
'||chr(35)||' Get the PID of this background process and update to the database
PIDS=\$(ps -ef | grep \$'||chr(33)||' | grep -v grep | grep -v '||v_wrapper_file_name||' | awk ''''{print \$2}'''')
'||chr(35)||' This should deal with all manner of UNIXES
PID=\$(echo \$PIDS | awk ''''{if(\$1>\$2) print \$1; else print \$2}'''')
'||chr(35)||' Still could not get a PID
[[ -z \$PID ]] '||chr(38)||chr(38)||' PID=-1
sqlplus -s / <<'||chr(33)||'
exec '||pc_schema||'.'||pc_package||'.update_task_pid(<TASK_ID>,<EXEC_ID>,\$PID);
'||chr(33)||'

'||chr(35)||' Wait for rendezvous
wait \$'||chr(33)||'

'||chr(35)||' Extract error RETURN CODE if there was one
[[ -a \$TMP_FILE ]] '||chr(38)||chr(38)||' RETCODE=\$(cat \$TMP_FILE) '||chr(38)||chr(38)||' rm -f \$TMP_FILE
[[ -z \$RETCODE ]] '||chr(38)||chr(38)||' RETCODE=0

'||chr(35)||' Update the return code to the scheduler
sqlplus -s / <<'||chr(33)||'
exec '||pc_schema||'.'||pc_package||'.update_task_return_code(<TASK_ID>,<EXEC_ID>,\$RETCODE);
'||chr(33)||'
EOF
';

      -- Put scheduler Task details
      v_ksh_wrapper:=replace(v_ksh_wrapper,'<TASK_ID>',p_task_spec.task_id);
      v_ksh_wrapper:=replace(v_ksh_wrapper,'<EXEC_ID>',v_task_lauch_id);
      -- Set up the command 
      v_ksh_wrapper:=replace(v_ksh_wrapper,'<COMMAND>',trim(p_task_spec.command));
     
      -- Errmm.. That's it I think.
      return v_ksh_wrapper;
    exception
      when others then
        utl.pkg_errorhandler.handle;
        raise;      
    end make_ksh_wrapper;

  begin
    -- Make up the wrapper file name
    v_wrapper_file_name :='<TMP_DIR>/task_'||lpad(p_task_spec.task_id,4,'0')||'_'||lpad(v_task_lauch_id,5,'0')||'.ksh';
    v_wrapper_file_name :=replace(v_wrapper_file_name,'<TMP_DIR>',nvl(utl.pkg_config.get_variable_string(gc_config_temp_dir_key),'/tmp'));
    -- Construct code to be submitted to DBMS_JOB:
    v_wrapper_command:=                   
'  -- Create script
  v_retcode:=utl.hostcmd('''||make_ksh_wrapper()||''');  
  -- Make script executable
  v_retcode:=utl.hostcmd(''chmod 777 '||v_wrapper_file_name||'''); 
  -- Execute the script
  v_retcode:=utl.hostcmd('''||v_wrapper_file_name||''');  
  -- Clean up temporary files
  v_retcode:=utl.hostcmd(''rm -f '||v_wrapper_file_name||''');';
    return v_wrapper_command;
  exception
    when others then
      utl.pkg_errorhandler.handle;
      raise;    
  end make_unix_external_command;

  -- Puts wrapper code around the command to make it ready for submitting to
  -- DBMS_JOB
  function make_dbms_job_wrapper(p_command in varchar2)
  return varchar2
  is
    v_dbms_job_wrapper  varchar2(4000);
  begin
    v_dbms_job_wrapper:=
'-- '||nvl(p_task_spec.description,'No description')||'
declare
  v_retcode pls_integer:=0;
begin
  ';

    if(p_task_spec.command_type=gc_command_type_FUNCTION)then
      v_dbms_job_wrapper:=v_dbms_job_wrapper||'
  v_retcode:='||p_command;
  -----------------------
    else
      v_dbms_job_wrapper:=v_dbms_job_wrapper||
  p_command;
  ---------
    end if;
    
    if(p_task_spec.command_type<>gc_command_type_EXTERNAL_PROC)then
      -- External procs update their return code them selves as soon as they have it
      v_dbms_job_wrapper:=v_dbms_job_wrapper||'
  '||pc_schema||'.'||pc_package||'.update_task_return_code('||p_task_spec.task_id||','||v_task_lauch_id||',v_retcode);  -- Update return code to schedule';
  --------------------------------------------------------------------------------
    end if;

    if( p_task_spec.repeats is not null 
    and p_task_spec.repeat_interval is not null
    and p_task_spec.task_type<>gc_type_PERSISTENT)
    then
      -- The task is REPEATED a number of times by DBMS_JOB:
      v_dbms_job_wrapper:=v_dbms_job_wrapper||'
  '||pc_schema||'.'||pc_package||'.increment_task_repeat_count('||p_task_spec.task_id||','||v_task_lauch_id||');  -- update number of times repeated so far';
  -----------------------------------------------------------------------
    end if;

    v_dbms_job_wrapper:=v_dbms_job_wrapper||'
exception
  when others then
    rollback;
    '||pc_schema||'.'||pc_package||'.update_task_return_code('||p_task_spec.task_id||','||v_task_lauch_id||',sqlcode);
    utl.pkg_logger.log('''||utl.pkg_logger.gc_log_message_error||''',null,''TaskId:'||p_task_spec.task_id||' Exec:'||v_task_lauch_id||''','||p_task_spec.task_id||',sqlcode);
end;';
-------------------------------------------------------------------------------------------------------------------------------------------------------
    return v_dbms_job_wrapper;
  exception
    when others then
      utl.pkg_errorhandler.handle;
      raise;
  end make_dbms_job_wrapper;

  function script_repeat_interval(p_interval in schedules.repeat_interval%type) 
  return varchar2
  is
  begin
    -- Round 
    return 'trunc((sysdate-trunc(sysdate))*1440+'||p_interval||')/1440+trunc(sysdate)';
  end script_repeat_interval;  
  
--------------------------------------------------------
begin -- submit_task
  dbms_application_info.set_module(c_proc_name,null);

  if(p_task_spec.command is null)then
    -- Dummy task, used as a logical node between depencies.
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'DUMMY Task - no actions',null,p_task_spec.task_id);
    v_task_log_row.task_id        :=p_task_spec.task_id;
    v_task_log_row.execution_id   :=v_task_lauch_id;
    v_task_log_row.state          :=p_task_spec.state;
    v_task_log_row.started_at     :=p_fsm_date;
    v_task_log_row.scheduled_time :=p_task_spec.next_due_date;
    v_task_log_row.queue_id       :=p_task_spec.queue_id;
    v_task_log_row.repeat_count   :=nvl(p_task_spec.repeat_count,0);   
    v_task_log_row.what           :=substr(v_job_submission,1,4000);   
    insert_task_event(v_task_log_row);    
  else    
    -- Calculate all dynamic variables
    scheduler_due.calc_dynamic_values(p_task_spec,p_fsm_date);
  
    -- Make up command that will be wrapped in the DBMS_JOB submission code
    if(p_task_spec.command_type=gc_command_type_EXTERNAL_PROC)then
      v_job_submission:=make_unix_external_command;
      -- Substitute with configuration variables and environment values
      -- This is done last since the command may contain environment variables.
      fit_unix_environment(v_job_submission);
    else
      v_job_submission:=trim(p_task_spec.command);
      -- Add trailing semicolon to command if not present
      if(substr(v_job_submission,-1,1)<>';')then
        v_job_submission:=v_job_submission||';';
      end if;
      -- Escape all inverted commas 
     --v_job_submission:=replace(v_job_submission,'''','''''');
    end if;
  
    -- Wrap command for submission to DBMS_JOB
    v_job_submission:=make_dbms_job_wrapper(v_job_submission);
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'Submitting Task Id '||p_task_spec.task_id||' to DBMS_JOB:'||chr(10)||v_job_submission,null,p_task_spec.task_id);
    
    -- Submit jobs to DBMS_JOB, and throws an exception when there code does not parse
    -- Parse the job content right away as it will be run right away in any case.
    -- If we were to leave the parse for when it runs, the job will be broken and will need to
    -- be manually removed. It will also make finding the problem more difficult - if anything
    -- goes wrong, we want to know about it now.
    if(p_task_spec.repeats is not null          and
       p_task_spec.repeat_interval is not null  and
       p_task_spec.task_type<>gc_type_PERSISTENT
    )then
      -- This is a repeating task
      -- Submit job for immediate repetitive execution by DBMS_JOB
      -- For repeating jobs, DBMS_JOB calculates the next occurrance of the 
      -- job at the *beginning* of the job, but there is still some slippage 
      -- over many repeats when this is calculated.
      -- The slippage can be avoided by rounding to date to the nearest minute
      -- e.g. (trunc(sysdate*1440)/1440)+[interval in days]    
      -- The interval is specified in minutes
      --dbms_job.submit(p_task_spec.queue_id,v_job_submission,p_fsm_date,script_repeat_interval(p_task_spec.repeat_interval));
      dbms_job.submit(p_task_spec.queue_id,v_job_submission,p_fsm_date,pc_schema||'.'||pc_package||'.get_next_repeat_time('||p_task_spec.task_id||')');
    else
      -- This is non-repeating (but possibly recurring) task. 
      -- See functional spec for definitions.
      dbms_job.submit(p_task_spec.queue_id,v_job_submission,p_fsm_date);
    end if;  
    -- Add entry to task_log table
    v_task_log_row.task_id      :=p_task_spec.task_id;
    v_task_log_row.execution_id :=v_task_lauch_id;
    v_task_log_row.state        :=p_task_spec.state;
    v_task_log_row.started_at   :=p_fsm_date;
    v_task_log_row.scheduled_time:=p_task_spec.next_due_date;
    v_task_log_row.queue_id     :=p_task_spec.queue_id;
    v_task_log_row.repeat_count :=nvl(p_task_spec.repeat_count,0);   
    v_task_log_row.what         :=substr(v_job_submission,1,4000);   
    insert_task_event(v_task_log_row);    
    
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
      'Launched Task Instance '||v_task_lauch_id||
      ' DBMS_JOB '||p_task_spec.queue_id,null,p_task_spec.task_id);    
  end if;
  commit; -- Do this after log so that events appear in correct order in log
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    p_task_spec.return_code:=sqlcode;
    return utl.pkg_exceptions.gc_scheduler_task_submit;
end submit_task;

------------------------------------------------------------------------
-- The following function are called from the wrapper code when 
-- running inside a DBMS_JOB:
------------------------------------------------------------------------

-- Calculates the next date that a repeating task has to be executed by DBMS_JOB.
-- This method prevents slippage introduced due to the execution duration of a task.
-- It will also ensure that if the execution duration of a task is greater than the 
-- repeat interval, the task is execute immediately again.
-- This function is called by DBMS_JOB when it needs o repeat a task execution
function get_next_repeat_time(p_task_id in scheduler.schedules.task_id%type)
return date
is
  v_started_at      date;
  v_repeat_interval pls_integer;
  v_repeat_count    pls_integer;
  v_repeats         pls_integer;
  
  v_next_execution  date;
  v_sysdate         date := sysdate;
begin
  -- Get task launch time
  -- Default to next-due-date and 
  select nvl(s.started_at,s.next_due_date),
         s.repeat_interval,
         nvl(s.repeat_count,0),         
         nvl(s.repeats,0)
    into v_started_at,
         v_repeat_interval,
         v_repeat_count,
         v_repeats
    from scheduler.schedules s
   where s.task_id = p_task_id;
   
  -- Round to minutes and add 
  v_next_execution:=trunc( 
                      (v_started_at-trunc(v_started_at))*1440   -- minutes since midnight
                    + (v_repeat_interval * v_repeat_count)      -- add minutes
                         )                                      -- round to nearest minute
                    / 1440                                      -- mimutes as a fraction of days
                    + trunc(v_started_at);                      -- Add to midnight  
  -- This date needs to be a time in the future   
  if(v_next_execution<=v_sysdate)then
    -- Add 1 minute to sysdate 
    v_next_execution:=trunc( 
                       (v_sysdate-trunc(v_sysdate))*1440        -- minutes since midnight
                     + 1                                        -- add 1 minute
                            )                                   -- round to nearest minute
                     / 1440                                     -- mimutes as a fraction of days
                     + trunc(v_sysdate);                        -- Add to midnight  
  end if;  

  -- The scheduler FSM is responsible for terminating a repeating task.
  -- If the FSM is not running, this repeating task will run forever until 
  -- the FSM is started again. If the repeat count so far exceeds the repeats count,
  -- then the DBMS_JOB job needs to be force to a standstil. This can be done by 
  -- setting the next execution date way into the future and the FSM will clean 
  -- this job up once it has executed.
  if(v_repeat_count>=v_repeats)then  
    if(v_repeats>1)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
       'The task will have completed the last of its required '||v_repeats||
       ' repetitions when this execution has completed.',null,p_task_id);
     end if;
    v_next_execution:=utl.pkg_date.gc_maxdate;
  end if;
  tx_heartbeat(p_task_id||'->REPEAT['||v_repeat_count||']');  
  return v_next_execution;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return null;  -- No repeats
end get_next_repeat_time;

-- Update the PID of the task for external procedures
-- Use varchar2 instead of numerical 
procedure update_task_pid(p_task_id      in schedules.task_id%type,
                          p_execution_id in varchar2,
                          p_process_id   in varchar2)
is
  c_proc_name   constant varchar2(100)  := pc_schema||'.'||pc_package||'.update_task_pid';
  pragma autonomous_transaction;
  v_repeats       schedules.repeats%type;
  v_repeat_count  schedules.repeat_count%type;
  v_task_type     schedules.task_type%type;
begin
  dbms_application_info.set_module(c_proc_name,null);
  update schedules
     set process_id = p_process_id
   where task_id=p_task_id;
  update task_log t
     set t.process_id = p_process_id
   where t.execution_id=p_execution_id;      
  commit;
  -- Get some task details
  select repeats,
         repeat_count,
         task_type
    into v_repeats,
         v_repeat_count,
         v_task_type
    from schedules
   where task_id = p_task_id;

  -- Operator messages
  if(nvl(v_repeats,1)>1)then
    -- Repeating task
    if(v_task_type<>gc_type_PERSISTENT)then
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||', repeat '||v_repeat_count||'/'||v_repeats||
        ', has PID of '||p_process_id,null,p_task_id);
    else
      -- Persistent task with a defined number of attempts
      if(v_repeat_count<v_repeats)then
        utl.pkg_errorhandler.handle;
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
          'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/'||v_repeats||
          ', has PID of '||p_process_id,null,p_task_id);
      else
        utl.pkg_errorhandler.handle;
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_warn, 
          'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/'||v_repeats||
          ', has PID of '||p_process_id||
          '. If this last attemp to run it fails, the scheduler will put it in the ERROR state.',
          null,p_task_id);
      end if;        
    end if;
  else
    if(v_task_type=gc_type_PERSISTENT)then
      -- Persistent task with undefined number of attempts until it has succeeded
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/unlimited'||
        ', has PID of '||p_process_id,null,p_task_id);
    else
      -- Non-repeating task
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||' has PID of '||p_process_id,null,p_task_id);
      end if;
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise; -- Throw exception back into the dbms_job process so that it can be 'broken'.
end update_task_pid;

-- Update the return code of the task for external procedures
procedure update_task_return_code(p_task_id     in schedules.task_id%type,
                                  p_execution_id in task_log.execution_id%type,
                                  p_return_code  in schedules.return_code%type)
is
  c_proc_name   constant varchar2(100)  := pc_schema||'.'||pc_package||'.update_task_return_code';
  pragma autonomous_transaction;
  v_sysdate date:=sysdate;
  v_repeats       schedules.repeats%type;
  v_repeat_count  schedules.repeat_count%type;
  v_task_type     schedules.task_type%type;  
begin
  dbms_application_info.set_module(c_proc_name,null);
  update schedules
     set return_code = p_return_code,
         finished_at = v_sysdate
   where task_id=p_task_id;
  update task_log t
     set t.return_code = p_return_code,
         t.ended_at    = v_sysdate
   where t.execution_id=p_execution_id
     and nvl(t.repeat_count,0)=(select max(nvl(t2.repeat_count,0)) 
                           from task_log t2
                          where t2.execution_id=p_execution_id
                        );  
  commit;
  -- Get some task details
  select repeats,
         repeat_count,
         task_type
    into v_repeats,
         v_repeat_count,
         v_task_type
    from schedules
   where task_id = p_task_id;
    
  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
    'Task Instance '||p_execution_id||' completed with a RETURN CODE of '||p_return_code,null,p_task_id);
  dbms_application_info.set_module(null,null);
  
  -- Operator messages
  if(nvl(v_repeats,1)>1)then
    -- Repeating task
    if(v_task_type<>gc_type_PERSISTENT)then
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||', repeat '||v_repeat_count||'/'||v_repeats||
        ' completed with a RETURN CODE of '||p_return_code,null,p_task_id);
    else
      -- Persistent task with a defined number of attempts
      if(v_repeat_count<v_repeats)then
        utl.pkg_errorhandler.handle;
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
          'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/'||v_repeats||
          ' completed with a RETURN CODE of '||p_return_code,null,p_task_id);
      else
        utl.pkg_errorhandler.handle;
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_warn, 
          'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/'||v_repeats||
          ' completed with a RETURN CODE of '||p_return_code||
          '. This was the last attempt to run it and  the scheduler will put it in the ERROR state.',
          null,p_task_id);
      end if;        
    end if;
  else
    if(v_task_type=gc_type_PERSISTENT)then
      -- Persistent task with undefined number of attempts until it has succeeded
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||', attempt '||v_repeat_count||'/unlimited'||
        ' completed with a RETURN CODE of '||p_return_code,null,p_task_id);
    else
      -- Non-repeating task
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug, 
        'Task Instance '||p_execution_id||
        ' completed with a RETURN CODE of '||p_return_code,null,p_task_id);
      end if;
  end if;

exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log(null,null,null,p_task_id);
    raise; -- Throw exception back into the dbms_job process
end update_task_return_code;

-- Update the number of counts so far executed for a repeating task
-- This is only used for monitoring the DBMS_JOB repeats of the tasks
-- and not for controlling it - this is done by DBMS_JOB.
-- This function is called by the code that is submitted to DBMS_JOB 
-- *AFTER* the execution
procedure increment_task_repeat_count(p_task_id      in schedules.task_id%type,
                                      p_execution_id in task_log.execution_id%type)
is
  pragma autonomous_transaction;
  c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.increment_task_repeat_count';
  v_task_spec     t_schedule_rec;  
  cursor c_task(p_task_id in schedules.task_id%type) is
    select * 
      from vw_fsm_tasks
     where task_id = p_task_id
       for update of repeat_count;
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Update repeat count
  open c_task(p_task_id);
  fetch c_task into v_task_spec;
  if(c_task%found)then
    if(v_task_spec.repeats is not null)then
      update schedules
         set repeat_count = nvl(repeat_count,0)+1
       where current of c_task;
      close c_task;  
      -- Add a new record to the task_log if another one is required by duplicating the 
      -- the last run's entry
      if(v_task_spec.repeat_count<v_task_spec.repeats)then
        -- Add entry to task_log table based on previous entry with a modified repeat_count
        insert into task_log (
               execution_id,
               task_id,
               repeat_count,
               scheduled_time,
               started_at,
               ended_at,
               queue_id,
               process_id,
               return_code,
               what)
              (select t.execution_id,
                      t.task_id,
                      t.repeat_count+1,
                      t.scheduled_time,
                      sysdate,
                      null,
                      t.queue_id,
                      null,
                      null,
                      t.what
                 from task_log t
                where t.execution_id=p_execution_id
                  and t.repeat_count=(select max(repeat_count)
                                        from task_log
                                       where execution_id=p_execution_id
                                      )
               );
        if(sql%rowcount<>1)then    
          utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
            'Could not add an entry to the task_log table for a repeating task. '||
            'This could be because the task_log has been truncated.');
        end if;
      end if;
      commit;
    else
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_warn,'Task is not a repeating task. '||c_proc_name||' should not have been called.',null,p_task_id);
    end if;
  else
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
'It appears that remnants of an old task still exist on the DBMS_JOB queue even though '||
'there is no corresponding task on the schedule. It is impossible to determine '||
'which task it belongs to and will therefore need to be manually '||
'removed with the following commands:
SQL> select * from user_jobs;
SQL> dbms_job.remove(...);
SQL> commit;
An exception will now be raised to the running DBMS_JOB so that it will not run again.');
    raise utl.pkg_exceptions.e_scheduler_task_exist;  -- Raise into the DBMS_JOB to break it.
  end if;
  dbms_application_info.set_module(null,null);
exception
  when utl.pkg_exceptions.e_scheduler_task_exist then
    rollback;
    raise;
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise; -- Throw exception back into the dbms_job process
end increment_task_repeat_count;

-- Gets the next execution Id for the task when it executes
function get_next_execution_id 
return pls_integer
is
  v_task_lauch_id       pls_integer;      -- Execution code assigned to this task's submission
begin
  -- Get next lauch Id so that we generate a unique script file name
  select sq_task_launch.nextval
    into v_task_lauch_id
    from dual;
  return v_task_lauch_id;    
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return -1;
end get_next_execution_id;      

-- Removes a job from the DBMS_JOB queue, either because it is was flawed 
-- or because it its repeating executions were all done.
procedure safe_job_remove(p_job_id in schedules.queue_id%type)
is
  pragma autonomous_transaction;
  e_job_not_exist exception;
  pragma exception_init(e_job_not_exist, -23421);
begin          
  dbms_job.broken(p_job_id,true); 
  -- TODO: Assumption - check if a job is still running - need read rights on v$session and v$process
  --       job may still be running - see above assumption. 
  --       This where we kill the Oracle session: alter system kill session 'sid,serial#';
  --       Check if the O/S process is still runing and kill this too: kill -9 PID
  dbms_job.remove(p_job_id);
  commit;
exception
  when e_job_not_exist then
    -- DBMS_JOB has already removed this job
    rollback;
  when others then
    -- Another process may have removed the job?
    rollback;
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,null,'DBMS_JOB',p_job_id);
end safe_job_remove;      


end sched;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
