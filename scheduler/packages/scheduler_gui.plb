create or replace package body scheduler.scheduler_gui
as
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- GUI to the Task Scheduler
-- DO NOT BEAUTIFY THIS CODE!!!!!!!
-------------------------------------------------------------------------------

--===========================================================================--
-- PRIVATE FUNCTIONS
--===========================================================================--

-- Calculates the as-of date for the value displayed in the offset field
-- in the date display format
function calc_as_of_date(p_task_id in schedules.task_id%type) return varchar2
is
  v_task_spec sched.t_schedule_rec;
  v_retcode   pls_integer;

  c_proc_name       constant varchar2(100) := pc_schema||'.'||pc_package||'.calc_effective_date';
  v_effective_date  varchar2(30);
begin
  dbms_application_info.set_module(c_proc_name,null); 
  v_task_spec.task_id := p_task_id;
  
  v_retcode := sched.get_task_details(v_task_spec);
  if (v_retcode = utl.pkg_exceptions.gc_success) then
    v_effective_date:= to_char(scheduler_due.calc_dynamic_effective_date(v_task_spec,sysdate), 
                               nvl(utl.pkg_config.get_variable_string('GUIDateFormat'),'YYYY/MM/DD'));
  else
    raise_application_error(v_retcode, null);
  end if;
  
  dbms_application_info.set_module(null,null);
  return v_effective_date;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end calc_as_of_date;

--===========================================================================--
-- Scheduler Control
--===========================================================================--
-- Scheduler Startup, shutdown, abort, resume etc...
-- Thows:   * Illegal botton click
procedure scheduler_startup is
begin
  sched.startup;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end scheduler_startup;

procedure scheduler_shutdown is
begin
  sched.shutdown;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end scheduler_shutdown;

procedure scheduler_resume is
begin
  sched.resume;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end scheduler_resume;

procedure scheduler_suspend is
begin
  sched.suspend;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end scheduler_suspend;

procedure scheduler_abort is
begin
  sched.abort;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end scheduler_abort;

-- Scheduler command and control
function get_scheduler_mode return varchar2 is
begin
  return sched.status;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_scheduler_mode;

--===========================================================================--
-- QUERY TASKS
--===========================================================================--

-- General Task query for the given search criteria
--- group name
--  state
-- Description is a wild-card string. NULL --> no constraint
-- Command is a wild-card string. NULL--> no contraint
-- dependency is a wild-card string. NULL--> no contraint
-- Returns:   
--  task_id        (should be hidden)
--  group_name
--  operation_id
--  task_type
--  description (limited to 30 chars)
--  current state
--  when last changed
--  next due for execution
--  command
--  dependencies
-- ordered by next due date
function get_tasks_list(
  p_description in  schedules.description%type :=null,
  p_command     in  schedules.command%type     :=null,
  p_group_name  in  varchar2                   :=null,
  p_state       in  varchar2                   :=null,
  p_dependency  in  varchar2                   :=null) 
return utl.global.t_result_set
is
begin
  return scheduler_rep.get_tasks_list(p_description,p_command,p_group_name,p_state,p_dependency);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_tasks_list;

function get_tasks_count(
  p_description in  schedules.description%type :=null,
  p_command     in  schedules.command%type     :=null,
  p_group_name  in  varchar2                   :=null,
  p_state       in  varchar2                   :=null,
  p_dependency  in  varchar2                   :=null) 
return pls_integer
is
  v_count pls_integer:=0;
begin
  select count(*)
  into   v_count
  from   schedules s
  where  ((p_description is null) or (p_description is not null and s.description like p_description))
  and    ((p_command is null) or (p_command is not null and s.command like p_command))
  and    ((p_state is null) or (p_state is not null and s.state = p_state))
  and    ((p_group_name is null) or (p_group_name is not null and s.group_name = p_group_name))
  and    ((p_dependency is null) or (p_dependency is not null and s.dependencies like p_dependency));

  return v_count;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_tasks_count;

-- Populates a Picklist for possible task states
-- Returns:   A resultset containig all the possible states that a task can be in
-- Called     * On initial opening of the browse screen
function get_states_list  return utl.global.t_result_set
is
  v_result_set        utl.global.t_result_set;
begin
  open v_result_set for
    select distinct state
      from schedules;
  return v_result_set;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_states_list;

function get_states_count return pls_integer
is
  v_count             pls_integer:=0;
begin
  select count(*)
    into v_count
    from (select distinct state
            from schedules
  );
  return v_count;
exception
  when others then
    return 0;
end get_states_count;

-- Populates a Picklist for all the existing groups
-- Returns:   A resultset containig all the currently used groups in the scheduler
-- Called:    On initial opening of the browse screen
function get_groups_list  return utl.global.t_result_set
is
  v_result_set        utl.global.t_result_set;
begin
  open v_result_set for
    select distinct group_name
      from schedules;
  return v_result_set;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_groups_list;

function get_groups_count return pls_integer
is
  v_count             pls_integer:=0;
begin
  select count(*)
    into v_count
    from (select distinct group_name
            from schedules
         );
  return v_count;
exception
  when others then
    return 0;
end get_groups_count;


-- Get all task details as per table design
-- Returns 1 or 0 records
-- Called:    On Details botton click on Browse screen
function get_task_details_list (p_task_id in schedules.task_id%type) return utl.global.t_result_set
is
  v_result_set        utl.global.t_result_set;
begin
  open v_result_set for
    select s.task_id,
           s.task_type,
           s.group_name,
           s.operation_id,
           s.command,
           s.command_type,
           s.description,
           s.dependencies,
           s.max_waittime,
           s.max_runtime,
           s.queue_id,
           s.process_id,
           s.return_code,
           s.state,
           to_char(s.state_tmstmp,'dd-MON-yyyy hh24:mi:ss'),
           s.prev_state,
           to_char(s.started_at,'dd-MON-yyyy hh24:mi:ss'),
           to_char(s.finished_at,'dd-MON-yyyy hh24:mi:ss'),
           s.year,
           s.month,
           s.day,
           s.hour,
           s.minute,
           s.weekdays,
           s.special_days,
           to_char(s.next_due_date, 'dd-MON-yyyy hh24:mi'),
           s.repeats,
           s.repeat_interval,
           s.repeat_count,
           s.effective_date_offset,
           task_explain(s.task_id),
           calc_as_of_date(s.task_id)
      from schedules s
     where task_id = p_task_id;
  return v_result_set;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_task_details_list;

-- TODO
function get_task_details_count(p_task_id in schedules.task_id%type) return pls_integer
is
begin
  return 1;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_task_details_count;

-- Get task summary
function task_explain(p_task_id in schedules.task_id%type) return varchar2
is
begin
  return scheduler_rep.task_explanation(p_task_id);
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_explain;

-- Get task state
function get_task_state(p_task_id in schedules.task_id%type) return varchar2
is
  v_state varchar2(10):=null;
begin
  select state
    into v_state
    from schedules
   where task_id = p_task_id;  
  return v_state;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_task_state;

--===========================================================================--
-- Level 1: Lauch Log
--===========================================================================--

-- Get all task launches for set of constraints orderd by start date descending
-- Returns a resultset with the following collumns:
-- task id
-- group_name
-- operation_id
-- description (limited to 30 chars)
-- exit task state
-- time started
-- time completed
-- return code
--
--
-- Called:      * Regularly while the screen is being viewed
--              * On initial opening of the screen
--              * On clicking of REFRESH BUTTON
function get_lauch_log_list(
  p_state in  schedules.state%type:=null,
  p_from  in  date:=null,
  p_to    in  date:=null
  ) return utl.global.t_result_set
is
begin
  null;--TODO
end get_lauch_log_list;

function get_launch_log_count(
  p_state in  schedules.state%type:=null,
  p_from  in  date:=null,
  p_to    in  date:=null
) return pls_integer
is
begin
  null;--TODO
end get_launch_log_count;

--===========================================================================--
-- TASK OPERATIONS
--===========================================================================--

-------------------------------------------------------------------------------
-- Delete the task.
--
-- Parameters:  Primary Key Task Id
--              Comma-separated string of Task Id's if multiple tasks selected.
-- Returns:     A non-success return code means that it was impossible
--              to remove the task from the schedule
function task_delete (p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
begin
  scheduler_mod.task_delete(p_task_id);
  return utl.pkg_exceptions.gc_success;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_delete;

function task_delete (p_task_ids in varchar2) return utl.global.t_error_code
is
  l_task_ids  dbms_sql.number_table := utl.pkg_string.string2number_table(p_task_ids);
  v_retcode utl.global.t_error_code := utl.pkg_exceptions.gc_success;
begin
  for i in l_task_ids.first..l_task_ids.last loop
    scheduler_mod.task_delete(l_task_ids(i));
    exit when v_retcode<>utl.pkg_exceptions.gc_success;
  end loop;
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_delete;

-------------------------------------------------------------------------------
-- Aborts the selected task now
--
-- Parameters:  Primary Key Task Id
procedure task_abort(p_task_id in schedules.task_id%type)
is
  v_task_spec sched.t_schedule_rec;
  v_ret_code  pls_integer;
begin
  v_task_spec.task_id := p_task_id;  
  
  v_ret_code := sched.get_task_details(v_task_spec);  
  if (v_ret_code != utl.pkg_exceptions.gc_success) then
    raise_application_error(v_ret_code,null);
  end if;
  
  v_ret_code := scheduler_mod.task_abort(v_task_spec); 
  if (v_ret_code != utl.pkg_exceptions.gc_success)then 
    raise_application_error(v_ret_code,null);
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_abort;

-------------------------------------------------------------------------------
-- Forwards the selected task
--
-- Parameters:  Primary Key Task Id

procedure task_forward(p_task_id in schedules.task_id%type)
is
 v_new_date_due date;   
 l_tasks sched.t_schedules;
begin
  l_tasks:=scheduler_mod.task_forward(p_task_id,v_new_date_due);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_forward;
-------------------------------------------------------------------------------
-- Resets the selected task now
--
-- Parameters:  Primary Key Task Id

procedure task_reset(p_task_id in schedules.task_id%type)
is
  v_retcode pls_integer;
begin
  v_retcode:=scheduler_mod.task_reset(p_task_id);  
  if (v_retcode != utl.pkg_exceptions.gc_success)then
    raise_application_error(v_retcode,null);
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_reset;

-------------------------------------------------------------------------------
-- Runs the selected task now
--
-- Parameters:  Primary Key Task Id
--              Comma-separated string of Task Id's if multiple tasks selected.
-- Returns:     A non-success return code means that it was impossible
--              to lauch the task, or if more than one task was selected,
--              one or more task launches failed.
procedure task_run_now(p_task_id in schedules.task_id%type)
is
  v_retcode utl.global.t_error_code;
begin
  v_retcode:=scheduler_mod.task_run_now(p_task_id);
  if (v_retcode != utl.pkg_exceptions.gc_success)then
    raise_application_error(v_retcode,null);
  end if;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_run_now;

procedure task_run_now(p_task_ids in varchar2)
is
  l_task_ids  dbms_sql.number_table := utl.pkg_string.string2number_table(p_task_ids);
  v_retcode utl.global.t_error_code;
begin
  for i in l_task_ids.first..l_task_ids.last loop
    v_retcode:=scheduler_mod.task_run_now(l_task_ids(i));
  end loop;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_run_now;

-------------------------------------------------------------------------------
-- Editing logic
--
-- Parameters:  Primary Key Task Id
--
-- Returns:     0       success, OK to proceed.
--              -20920  gc_scheduler_task_edit_lock
function is_task_editable(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
begin
  if(scheduler_mod.is_task_editable(p_task_id)=true)then
    return utl.pkg_exceptions.gc_success;
  else
    return utl.pkg_exceptions.gc_scheduler_task_edit_lock;
  end if;
end is_task_editable;

function is_task_edited_by_me return boolean
is
begin
  return scheduler_mod.is_session_edit_locked;
end is_task_edited_by_me;


--===========================================================================--
-- TASK EDIT
--===========================================================================--
function task_edit(
  p_task_id           in schedules.task_id%type,
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
) return utl.global.t_error_code
is
begin
  scheduler_mod.task_edit(p_task_id                  => p_task_id,  
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
                          p_change_reason            =>p_change_reason);
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_edit;

-- Commit editing changes and complete editing operation
function task_commit return utl.global.t_error_code
is
begin
  scheduler_mod.edit_commit;
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_commit;

-- Cancel all editing changes
function task_cancel return utl.global.t_error_code
is
begin
  scheduler_mod.edit_cancel;
  return 0;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end task_cancel;

-- Adds a new task to the schedule
-- The task Id is internally generated.
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
    p_change_reason            =>p_change_reason);
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    dbms_output.put_line(sqlerrm);
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end task_add;

-- Alters and commits all changes to an existing task
-- This approach does an edit and commit all in one operation
function alter_task(
  p_task_id           in schedules.task_id%type,
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
  c_proc  constant varchar2(100)  := pc_schema||'.'||pc_package||'.alter_task';
begin
  dbms_application_info.set_module(c_proc,null);

  -- Do all edits in one go
  scheduler_mod.task_edit(
    p_task_id                  =>p_task_id,
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
    p_change_reason            =>p_change_reason);
  -- .. and commit all edits
  scheduler_mod.edit_commit;
  dbms_application_info.set_module(null,null);
  return 0;
exception
  when others then
    dbms_output.put_line(sqlerrm);
    if(mod(sqlcode,256)=0)then
      return 1;
    else
      return mod(sqlcode,256);
    end if;
end alter_task;

--===========================================================================--
-- VCR-specific User Interface
--===========================================================================--

function get_data_file_table (p_file_spec in varchar2) return utl.t_varchar2
is
  v_shell_command varchar2(200);
  v_tmp_file      varchar2(100);
  v_home_dir      varchar2(200):=utl.pkg_config.get_variable_string('$APP_HOME');
  v_file_handle   utl_file.file_type;
  v_file_name     varchar2(32767);
  t_file_list     utl.t_varchar2 := utl.t_varchar2();  
  v_count         pls_integer:=0;  
  v_retcode       binary_integer;  
  c_nofile_retcode binary_integer := 512;
begin
  select 'datafilelist.'||to_char(systimestamp, 'ddMONyyyyhh24missss.ff')
    into v_tmp_file
    from dual;
  
  if p_file_spec is null then 
    v_shell_command := 'ls -lt '||v_home_dir||'/data/* > '||v_home_dir||'/log/'||v_tmp_file;
  else
    v_shell_command := 'ls -lt '||v_home_dir||'/data/'||p_file_spec||' > '||v_home_dir||'/log/'||v_tmp_file;
  end if;

  v_retcode := utl.hostcmd(v_shell_command);
  
  if (v_retcode = utl.pkg_exceptions.gc_success)
  or (v_retcode = c_nofile_retcode)
  then
    begin
      v_file_handle := utl_file.fopen(utl.pkg_config.get_variable_string('LogFileDir'), v_tmp_file, 'r', 32767);
    
      loop
        begin
          utl_file.get_line(v_file_handle,v_file_name,32767);
        exception
          when no_data_found then
            exit;     
        end;        
        v_count := v_count + 1;      
        t_file_list.extend(1);      
        t_file_list(v_count) := v_file_name;
      end loop;
    
      utl_file.fclose(v_file_handle);
    exception
      when others then
        utl_file.fclose(v_file_handle);
        raise;
    end;
    
    v_shell_command := 'rm -f '||v_home_dir||'/log/'||v_tmp_file;    
    v_retcode := utl.hostcmd(v_shell_command);  
    if (v_retcode != utl.pkg_exceptions.gc_success) then
      raise_application_error(utl.pkg_exceptions.gc_host_command_err, v_shell_command || ' failed with return code ' || v_retcode);   
    end if;
  else
    raise_application_error(utl.pkg_exceptions.gc_host_command_err, v_shell_command || ' failed with return code ' || v_retcode);
  end if;
  
  return t_file_list;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise; 
end get_data_file_table;

-------------------------------------------------------------------------------
-- Populates a Picklist for all unloaded data files that match a file spec
--
-- Returns:   A resultset containig all unloaded data files
--
-- Called     * On initial opening of the browse screen
function get_data_files_list (p_file_spec in varchar2:=null) return utl.global.t_result_set
is
  cur_result_set utl.global.t_result_set;
  t_file_list    utl.t_varchar2 := utl.t_varchar2();
  v_home_dir     varchar2(200):=utl.pkg_config.get_variable_string('$APP_HOME');
begin
  t_file_list := get_data_file_table(p_file_spec);
  
  open cur_result_set for
    select substr(value(files), instr(value(files),v_home_dir)+length(v_home_dir)+6) filename, 
           value(files) filedetails
      from table(cast(t_file_list as utl.t_varchar2)) files;    
  return cur_result_set;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise; 
end get_data_files_list;

function get_data_files_count(p_file_spec in varchar2:=null) return pls_integer
is
  t_file_list    utl.t_varchar2 := utl.t_varchar2();
  v_count        pls_integer;
begin
  t_file_list := get_data_file_table(p_file_spec);  
  v_count := t_file_list.count;  
  return v_count;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;   
end get_data_files_count;


-- create and schedule one-off task to load a file or files on a adhoc basis
-- This creates 4 specific tasks that together perform an ad-hoc load.
-- Each  task is dependent on the other.
--
-- TODO: Needs transactioning - if one task creation fails, all newly-created tasks should be rolled back.
--       Can't use SAVEPOINT since currently-used edit procedures are autonomous.
--       Enhance editing procedures maybe?
--
-- Parameters:
-- p_type (1 default, 2 version over-ride, 3 specific file(s)
-- p_source_name mandatory
-- p_as_of_date  mandatory
-- p_basis       mandatory
-- p_version     mandatory for type 2
-- p_file        mandatory for type 3
-- p_source_file_type mandatory for type 3
--
-- Returns: any error code explicitly trapped during the process or success
function schedule_adhoc_file_load( p_type             in integer,
                                   p_source_name      in varchar2,
                                   p_as_of_date       in date,
                                   p_basis            in varchar2,
                                   p_version          in varchar2,
                                   p_file             in varchar2,
                                   p_source_file_type in varchar2) 
return utl.global.t_error_code
is
  v_retcode utl.global.t_error_code;
begin
  v_retcode:=scheduler_mod.schedule_adhoc_file_load(
    p_type=>p_type,
    p_source_name=>p_source_name,
    p_as_of_date=>p_as_of_date,
    p_basis=>p_basis,
    p_version=>p_version,
    p_file=>p_file,
    p_source_file_type=>p_source_file_type);
  return v_retcode;
exception
  when others then    
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;  
end schedule_adhoc_file_load;


end scheduler_gui;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
