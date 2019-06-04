create or replace package body scheduler.scheduler_mod
as
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Interaction with the Scheduler data
--
-- DO NOT BEAUTIFY THIS CODE!
-------------------------------------------------------------------------------

--===========================================================================--
-- PRIVATE FUNCTIONS
--===========================================================================--

-------------------------------------------------------------------------------
-- Checks a task lock for editing
procedure check_task_lock(p_task_id in schedules.task_id%type)
is
  c_proc              constant varchar2(100)  := pc_schema||'.'||pc_package||'.edit_begin';
  v_task_spec         sched.t_schedule_rec;
  v_old_task_spec     sched.t_schedule_rec;
begin
  dbms_application_info.set_module(c_proc,null);
  -- Get full task spec
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;

  -- Check if the task is not already being edited
  if(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
    raise utl.pkg_exceptions.e_scheduler_task_edit_lock;
  end if;

  -- Check if the task is not already executing
  if(v_task_spec.state=sched.gc_state_EXECUTING)then
    raise utl.pkg_exceptions.e_scheduler_task_busy;
  end if;

  -- A session can only edit one task at a time
  -- Check if there is an abandoned edit in this session and reset it
  if(is_session_edit_locked)then
    v_old_task_spec.task_id:=gv_session_task_edit_lock;
    if(sched.get_task_details(v_old_task_spec)<>utl.pkg_exceptions.gc_success)then
      -- Spurious, this task has since been removed so we ignore it.
      release_session_lock;
    else
      if(v_old_task_spec.state=sched.gc_state_EDIT_LOCK)then
        -- Have to wait for scheduler FSM to abandon the edit
        raise utl.pkg_exceptions.e_scheduler_task_edit_user;
      else
        -- This task was locked in the session but not in data. Release lock.
        release_session_lock;
      end if;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end check_task_lock;

------------------------------------------------------------------------------
-- Checks if a task is editable
-- Returns:   TRUE if an edit lock *could* be gotten
--            FALSE is not or of the task does not exist
function is_task_editable(p_task_id in schedules.task_id%type) return boolean is
  v_task_spec  sched.t_schedule_rec;
begin
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    return false;
  end if;
  return is_task_editable(v_task_spec);
exception
  when others then
    return false;
end is_task_editable;

function is_task_editable(p_task_spec in sched.t_schedule_rec) return boolean is
begin
  if(p_task_spec.state in (sched.gc_state_EDIT_LOCK, sched.gc_state_EXECUTING))then
    return false;
  end if;
  return true;
exception
  when others then
    return false;
end is_task_editable;

function is_session_edit_locked return boolean is
begin
  if(gv_session_task_edit_lock is null)then
    return false;
  end if;
  return true;
end is_session_edit_locked;

-- Check if all child tasks in the dependency net are editable
-- Return TRUE if so. If there are no child tasks, return TRUE
function are_tasks_editable(p_tasks in sched.t_schedules) return boolean is
begin
  if(p_tasks.count()>0)then
    for i in p_tasks.first..p_tasks.last loop
      if(p_tasks(i).state in (sched.gc_state_EDIT_LOCK, sched.gc_state_EXECUTING))then
        return false;
      end if;
    end loop;
  end if;
  return true;
end are_tasks_editable;

-- Gets the task Id that is currently being edited in this sessions
function get_session_lock return schedules.task_id%type is
begin
  return gv_session_task_edit_lock;
end get_session_lock;

-- Gets the task that is currently being edited in this sessions
function get_session_lock_task return sched.t_schedule_rec is
begin
  return gv_edit_schedule_rec;
end get_session_lock_task;

-- Get next primary key for task
function get_next_task_id return schedules.task_id%type is
  v_task_id schedules.task_id%type;
begin
  select sq_schedule_id.nextval
    into v_task_id
    from dual;
  return v_task_id;
end;

-- Advisory locking system:
-- This session can only edit one task at a time.
-- Assign this task to the session
procedure set_session_lock(p_task_id in schedules.task_id%type) is
begin
  if(gv_session_task_edit_lock is not null)then
    raise utl.pkg_exceptions.e_scheduler_task_edit_user;
  end if;
  gv_session_task_edit_lock:=p_task_id;
end set_session_lock;

-- Un-Assign this task to the session
procedure release_session_lock is
begin
  gv_session_task_edit_lock:=null;
end release_session_lock;


-- Clear work space
procedure init_workspace(p_task_spec in out sched.t_schedule_rec)
is
begin
  gv_edit_schedule_rec:=p_task_spec;
  /*
  -- For 8i
  p_task_spec.task_id:=null;
  p_task_spec.task_type:=null;
  p_task_spec.group_name:=null;
  p_task_spec.operation_id:=null;
  p_task_spec.command:=null;
  p_task_spec.command_type:=null;
  p_task_spec.description:=null;
  p_task_spec.dependencies:=null;
  p_task_spec.max_waittime:=null;
  p_task_spec.max_runtime:=null;
  p_task_spec.process_id:=null;
  p_task_spec.state:=null;
  p_task_spec.state_tmstmp:=null;
  p_task_spec.prev_state:=null;
  p_task_spec.started_at:=null;
  p_task_spec.finished_at:=null;
  p_task_spec.year:=null;
  p_task_spec.month:=null;
  p_task_spec.day:=null;
  p_task_spec.hour:=null;
  p_task_spec.minute:=null;
  p_task_spec.weekdays:=null;
  p_task_spec.special_days:=null;
  p_task_spec.next_due_date:=null;
  p_task_spec.repeats:=null;
  p_task_spec.repeat_interval:=null;
  p_task_spec.repeat_count:=null;
  p_task_spec.effective_date_offset:=null;
  */
end init_workspace;

-- Gets the next Operation Id for a given group name
-- By convention, the Operation Id's are multiples of 10.
function calc_next_operation_id(p_group_name in schedules.group_name%type)
return schedules.operation_id%type
is
  v_next_op_id schedules.operation_id%type;
begin
  select round(nvl(max(operation_id),0),-1)+10
    into v_next_op_id
    from schedules
   where group_name = p_group_name;
  return v_next_op_id;
end calc_next_operation_id;

-------------------------------------------------------------------------------
-- Begin the inserting of new task
-- Inserting a new task is similar to editing a task, except that we do not
-- yet have a task Id, so we hold a notional one of -1. Only when we update
-- and commit the task is the task inserted to the schedule. The inserting
-- operation can be cancelled.
-- It is not possible to insert a task during a session while an edit session
-- is in progress.
procedure insert_begin is
  v_old_task_spec     sched.t_schedule_rec;
begin
  -- A session can only edit one task at a time
  -- Check if there is an abandoned edit in this session and reset it
  if(is_session_edit_locked)then
    v_old_task_spec.task_id:=gv_session_task_edit_lock;
    if(sched.get_task_details(v_old_task_spec)<>utl.pkg_exceptions.gc_success)then
      -- Spurious, this task has since been removed so we ignore it.
      release_session_lock;
    else
      if(v_old_task_spec.state=sched.gc_state_EDIT_LOCK)then
        -- Have to wait for scheduler FSM to abandon the edit
        raise utl.pkg_exceptions.e_scheduler_task_edit_user;
      else
        -- This task was locked in the session but not in data. Release lock.
        release_session_lock;
      end if;
    end if;
  end if;

  set_session_lock(gc_insert_task_id);
  -- Clear workspace
  init_workspace(gv_edit_schedule_rec);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end insert_begin;

-------------------------------------------------------------------------------
-- Adds task to scheduler
-- Checks validity and only adds it all task criteria makes sense
--
-- Parameters:
--  All required parameters for the type of task to be specified in p_task_spec.
--  The task_id value will be ignored, and a new value will be assigned when the
--  task is added.
procedure insert_task(p_task_spec in out sched.t_schedule_rec,
                      p_task_peers        in varchar2:=null,
                      p_group_priority    in task_groups.group_priority%type:=null,
                      p_commit    in boolean := true)
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.insert_task';
  v_task_spec         sched.t_schedule_rec := p_task_spec;
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Task state when loaded is always INITIAL
  v_task_spec.state     :=sched.gc_state_INITIAL;
  if(scheduler_val.validate_task(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_spec;
  end if;

  -- Calculate the next due date of this task based on the time that it was added to the schedule:
  --         ******* No! This is calculated by the scheduler as and when it is required. *********
  --if(scheduler_due.calc_next_due_date(v_task_spec,sysdate,v_task_spec.next_due_date)<>utl.pkg_exceptions.gc_success)then
  --  raise utl.pkg_exceptions.e_scheduler_next_due_date;
  --end if;
  if(scheduler_dep.validate_dependencies(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_spec;
  end if;

  -- Insert into the table before circular referencing can be checked
  -- If the check fails, then simply roll back.
  insert into schedules(
         task_id,
         modal,
         submitted_by,
         task_type,
         task_priority,
         group_name,
         operation_id,
         dependencies,
         max_waittime,
         command,
         command_type,
         max_runtime,
         process_id,
         state,
         state_tmstmp,
         prev_state,
         started_at,
         finished_at,
         ignore_error,
         year,
         month,
         day,
         hour,
         minute,
         weekdays,
         special_days,
         next_due_date,
         repeats,
         repeat_interval,
         repeat_count,
         description,
         effective_date_offset,
         change_reason
         )
  values(v_task_spec.task_id,
         v_task_spec.modal,
         v_task_spec.submitted_by,
         v_task_spec.task_type,
         v_task_spec.task_priority,
         v_task_spec.group_name,
         v_task_spec.operation_id,
         v_task_spec.dependencies,
         v_task_spec.max_waittime,
         v_task_spec.command,
         v_task_spec.command_type,
         v_task_spec.max_runtime,
         v_task_spec.process_id,
         v_task_spec.state,
         sysdate,
         v_task_spec.prev_state,
         v_task_spec.started_at,
         v_task_spec.finished_at,
         v_task_spec.ignore_error,
         v_task_spec.year,
         v_task_spec.month,
         v_task_spec.day,
         v_task_spec.hour,
         v_task_spec.minute,
         v_task_spec.weekdays,
         v_task_spec.special_days,
         v_task_spec.next_due_date,
         v_task_spec.repeats,
         v_task_spec.repeat_interval,
         v_task_spec.repeat_count,
         v_task_spec.description,
         v_task_spec.effective_date_offset,
         v_task_spec.change_reason
         );
  if(p_commit)then
    commit;
  end if;
  task_peers(v_task_spec.task_id, p_task_peers,p_commit);
  group_priority(v_task_spec.group_name,p_group_priority,p_commit);
  
  p_task_spec := v_task_spec;
  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end insert_task;


-------------------------------------------------------------------------------
-- Commits changes and releases a Lock on a task that was inserted
procedure insert_commit is
  pragma autonomous_transaction;
  c_proc_name      constant varchar2(100)  := pc_schema||'.'||pc_package||'.insert_commit';
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(not is_session_edit_locked)then
    raise utl.pkg_exceptions.e_scheduler_task_not_edit;
  end if;
  -- Get next task Id
  gv_edit_schedule_rec.task_id:=get_next_task_id;
  insert_task(gv_edit_schedule_rec);
  release_session_lock;
  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end insert_commit;

-------------------------------------------------------------------------------
-- Edit attributes of an existing task
-- This function can be called multiple times during an editing session.
-- Update only those attributes that have not changed on this function call -
--  this allows the function to be repeatedly called during an edit session.
procedure set_task_attr(
  p_submitted_by      in schedules.submitted_by%type:=null,
  p_task_type         in schedules.task_type%type:=null,
  p_task_priority     in schedules.task_priority%type:=null,
  p_group_name        in schedules.group_name%type:=null,
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
) is
  v_task_spec sched.t_schedule_rec;
begin
  -- Only override the most recently set values with new values if thet are set:
  -- ( Argh! There must be a better to do this? )
  v_task_spec.submitted_by:=                p_submitted_by;
  v_task_spec.task_type:=                   p_task_type;   
  v_task_spec.task_priority:=               p_task_priority;
  v_task_spec.group_name:=                  p_group_name;  
  v_task_spec.operation_id:=                p_operation_id;
  v_task_spec.command:=                     p_command;     
  v_task_spec.command_type:=                p_command_type;
  v_task_spec.description:=                 p_description; 
  v_task_spec.dependencies:=                p_dependencies;
  v_task_spec.max_waittime:=                p_max_waittime;
  v_task_spec.max_runtime:=                 p_max_runtime; 
  v_task_spec.year:=                        p_year;        
  v_task_spec.month:=                       p_month;       
  v_task_spec.day:=                         p_day;         
  v_task_spec.hour:=                        p_hour;        
  v_task_spec.minute:=                      p_minute;      
  v_task_spec.weekdays:=                    p_weekdays;    
  v_task_spec.special_days:=                p_special_days;
  v_task_spec.next_due_date:=               p_next_due_date;
  v_task_spec.repeats:=                     p_repeats;     
  v_task_spec.repeat_interval:=             p_repeat_interval;
  v_task_spec.repeat_periodic:=             p_repeat_periodic;
  v_task_spec.effective_date_offset:=       p_effective_date_offset;
  v_task_spec.modal:=                       p_modal;       
  v_task_spec.ignore_error:=                p_ignore_error;
  v_task_spec.change_reason:=               p_change_reason;
  set_task_attr(v_task_spec);  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end set_task_attr;

-- Edit attributes of an existing task
procedure set_task_attr(p_task_spec in sched.t_schedule_rec)
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.set_task_attr';
  v_pattern_changed   boolean:=false;
begin
  -- Check if any time values have been changed:
  
  if(gv_edit_schedule_rec.year <>nvl(p_task_spec.year,gv_edit_schedule_rec.year)
  or gv_edit_schedule_rec.month<>nvl(p_task_spec.month,gv_edit_schedule_rec.month)
  or gv_edit_schedule_rec.day  <>nvl(p_task_spec.day,gv_edit_schedule_rec.day)
  or gv_edit_schedule_rec.hour <>nvl(p_task_spec.hour,gv_edit_schedule_rec.hour)
  or gv_edit_schedule_rec.minute<>nvl(p_task_spec.minute,gv_edit_schedule_rec.minute)
  or gv_edit_schedule_rec.weekdays<>nvl(p_task_spec.weekdays,gv_edit_schedule_rec.weekdays)
  or gv_edit_schedule_rec.special_days<>nvl(p_task_spec.special_days,gv_edit_schedule_rec.special_days))
  then
    v_pattern_changed:=true;
  end if;
  
  -- Only override the most recently set values with new values if thet are set:
  gv_edit_schedule_rec.submitted_by:=                nvl(p_task_spec.submitted_by,gv_edit_schedule_rec.submitted_by);
  gv_edit_schedule_rec.task_type:=                   nvl(p_task_spec.task_type,gv_edit_schedule_rec.task_type);
  gv_edit_schedule_rec.task_priority:=               nvl(p_task_spec.task_priority,gv_edit_schedule_rec.task_priority);
  gv_edit_schedule_rec.modal:=                       nvl(p_task_spec.modal,gv_edit_schedule_rec.modal);
  gv_edit_schedule_rec.group_name:=                  nvl(p_task_spec.group_name,gv_edit_schedule_rec.group_name);
  gv_edit_schedule_rec.operation_id:=                nvl(p_task_spec.operation_id,gv_edit_schedule_rec.operation_id);
  gv_edit_schedule_rec.dependencies:=                nvl(p_task_spec.dependencies,gv_edit_schedule_rec.dependencies);
  gv_edit_schedule_rec.command:=                     nvl(p_task_spec.command,gv_edit_schedule_rec.command);
  gv_edit_schedule_rec.command_type:=                nvl(p_task_spec.command_type,gv_edit_schedule_rec.command_type);
  gv_edit_schedule_rec.description:=                 nvl(p_task_spec.description,gv_edit_schedule_rec.description);
  gv_edit_schedule_rec.max_waittime:=                nvl(p_task_spec.max_waittime,gv_edit_schedule_rec.max_waittime);
  gv_edit_schedule_rec.max_runtime:=                 nvl(p_task_spec.max_runtime,gv_edit_schedule_rec.max_runtime);
  gv_edit_schedule_rec.ignore_error:=                nvl(p_task_spec.ignore_error,gv_edit_schedule_rec.ignore_error);
  gv_edit_schedule_rec.year:=                        nvl(p_task_spec.year,gv_edit_schedule_rec.year);
  gv_edit_schedule_rec.month:=                       nvl(p_task_spec.month,gv_edit_schedule_rec.month);
  gv_edit_schedule_rec.day:=                         nvl(p_task_spec.day,gv_edit_schedule_rec.day);
  gv_edit_schedule_rec.hour:=                        nvl(p_task_spec.hour,gv_edit_schedule_rec.hour);
  gv_edit_schedule_rec.minute:=                      nvl(p_task_spec.minute,gv_edit_schedule_rec.minute);
  gv_edit_schedule_rec.weekdays:=                    nvl(p_task_spec.weekdays,gv_edit_schedule_rec.weekdays);
  gv_edit_schedule_rec.special_days:=                nvl(p_task_spec.special_days,gv_edit_schedule_rec.special_days);  
  if(v_pattern_changed and p_task_spec.next_due_date is null)then
    gv_edit_schedule_rec.next_due_date:=null;
  else
    gv_edit_schedule_rec.next_due_date:=             nvl(p_task_spec.next_due_date,gv_edit_schedule_rec.next_due_date);
  end if;  
  gv_edit_schedule_rec.repeats:=                     nvl(p_task_spec.repeats,gv_edit_schedule_rec.repeats);
  gv_edit_schedule_rec.repeat_interval:=             nvl(p_task_spec.repeat_interval,gv_edit_schedule_rec.repeat_interval);
  gv_edit_schedule_rec.repeat_periodic:=             nvl(p_task_spec.repeat_interval,gv_edit_schedule_rec.repeat_periodic);
  gv_edit_schedule_rec.effective_date_offset:=       nvl(p_task_spec.effective_date_offset,gv_edit_schedule_rec.effective_date_offset);
  gv_edit_schedule_rec.change_reason:=               nvl(p_task_spec.change_reason,gv_edit_schedule_rec.change_reason);  
exception
  when no_data_found then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log(null,null,null,null,utl.pkg_exceptions.gc_scheduler_task_exist,c_proc);
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end set_task_attr;

--===========================================================================--
-- PUBLIC FUNCTIONS
--===========================================================================--


-- Edit a task
procedure task_edit(
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
)
is
  c_proc              constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_edit';
  v_task_spec         sched.t_schedule_rec;
  --v_old_task_spec     sched.t_schedule_rec;
  --v_current_user      varchar2(30);
begin
  dbms_application_info.set_module(c_proc,null);

  -- Get full task spec
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;

  if(get_session_lock is null)then
    -- If no session lock, then this is the FIRST TIME
    if(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
      -- Is the task already being edited by someone else?
      raise utl.pkg_exceptions.e_scheduler_task_edit_lock;
    end if;

    sched.set_task_state(v_task_spec,sched.gc_state_EDIT_LOCK);
    set_session_lock(p_task_id);
    -- Create a memory-based task image
    init_workspace(v_task_spec);
  else
    -- We have already started editing a task in this session.
    if(get_session_lock<>p_task_id)then
      -- Can only set attributes to the task that started editing
      raise utl.pkg_exceptions.e_scheduler_task_not_edit;
    end if;
  end if;

  -- Update the memory-based image
  set_task_attr(p_submitted_by             =>p_submitted_by,
                p_task_type                =>p_task_type,
                p_task_priority            =>p_task_priority,
                p_group_name               =>p_group_name,
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
                

  task_peers(p_task_id, p_task_peers);
  group_priority(p_group_name,p_group_priority);  
  
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_edit;

-------------------------------------------------------------------------------
-- Commits changes and releases a Lock on a task that was edited
procedure edit_commit is
  pragma autonomous_transaction;
  c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.edit_commit';
  v_task_spec     sched.t_schedule_rec;
  v_sql           varchar2(4000);
  c_not_null      constant varchar2(10):='nOtNuLl';
  c_not_null_num  constant number:=10666601;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(not is_session_edit_locked)then
    raise utl.pkg_exceptions.e_scheduler_task_not_edit;
  end if;

  -- UPDATE changes after a TASK EDIT
  v_task_spec.task_id:=get_session_lock;
  if(sched.get_task_details(v_task_spec)<>utl.pkg_exceptions.gc_success)then
    -- The task may since have been removed? Impossible!
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  else
    -- Final test: If the task has been too long in the EDIT_LOCK mode,
    --             the scheduler will have reset it to the previous state and
    --             all changes are effectively abandoned.
    if(v_task_spec.state<>sched.gc_state_EDIT_LOCK)then
      raise utl.pkg_exceptions.e_scheduler_task_edit_abandon;
    else
      -- Validate edited values
      if(scheduler_val.validate_task(gv_edit_schedule_rec)<>utl.pkg_exceptions.gc_success) then
        raise utl.pkg_exceptions.e_scheduler_task_spec;
      else
        -- Update changed values and return to unlock from EDIT_LOCK state
        -- Make up dynamic SQL
        v_sql:='update schedules set '||chr(10);                
        if(nvl(gv_edit_schedule_rec.submitted_by,c_not_null)<>nvl(v_task_spec.submitted_by,c_not_null))then
          v_sql:=v_sql||'submitted_by='''||gv_edit_schedule_rec.submitted_by||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.task_type,c_not_null)<>nvl(v_task_spec.task_type,c_not_null))then
          v_sql:=v_sql||'task_type='''||gv_edit_schedule_rec.task_type||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.task_priority,c_not_null)<>nvl(v_task_spec.task_priority,c_not_null))then
          v_sql:=v_sql||'task_priority='''||gv_edit_schedule_rec.task_priority||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.group_name,c_not_null)<>nvl(v_task_spec.group_name,c_not_null))then
          v_sql:=v_sql||'group_name='''||gv_edit_schedule_rec.group_name||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.operation_id,c_not_null_num)<>nvl(v_task_spec.operation_id,c_not_null_num))then
          v_sql:=v_sql||'operation_id='''||gv_edit_schedule_rec.operation_id||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.command,c_not_null)<>nvl(v_task_spec.command,c_not_null))then
          v_sql:=v_sql||'command='''||gv_edit_schedule_rec.command||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.command_type,c_not_null)<>nvl(v_task_spec.command_type,c_not_null))then
          v_sql:=v_sql||'command_type='''||gv_edit_schedule_rec.command_type||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.description,c_not_null)<>nvl(v_task_spec.description,c_not_null))then
          v_sql:=v_sql||'description='''||gv_edit_schedule_rec.description||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.dependencies,c_not_null)<>nvl(v_task_spec.dependencies,c_not_null))then
          v_sql:=v_sql||'dependencies='''||gv_edit_schedule_rec.dependencies||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.max_waittime,c_not_null_num)<>nvl(v_task_spec.max_waittime,c_not_null_num))then
          v_sql:=v_sql||'max_waittime='''||gv_edit_schedule_rec.max_waittime||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.max_runtime,c_not_null_num)<>nvl(v_task_spec.max_runtime,c_not_null_num))then
          v_sql:=v_sql||'max_runtime='''||gv_edit_schedule_rec.max_runtime||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.year,c_not_null_num)<>nvl(v_task_spec.year,c_not_null_num))then
          v_sql:=v_sql||'year='''||gv_edit_schedule_rec.year||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.month,c_not_null_num)<>nvl(v_task_spec.month,c_not_null_num))then
          v_sql:=v_sql||'month='''||gv_edit_schedule_rec.month||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.day,c_not_null_num)<>nvl(v_task_spec.day,c_not_null_num))then
          v_sql:=v_sql||'day='''||gv_edit_schedule_rec.day||''',';
        end if;        
        if(nvl(gv_edit_schedule_rec.hour,c_not_null_num)<>nvl(v_task_spec.hour,c_not_null_num))then
          v_sql:=v_sql||'hour='''||gv_edit_schedule_rec.hour||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.minute,c_not_null_num)<>nvl(v_task_spec.minute,c_not_null_num))then
          v_sql:=v_sql||'minute='''||gv_edit_schedule_rec.minute||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.weekdays,c_not_null)<>nvl(v_task_spec.weekdays,c_not_null))then
          v_sql:=v_sql||'weekdays='''||gv_edit_schedule_rec.weekdays||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.special_days,c_not_null)<>nvl(v_task_spec.special_days,c_not_null))then
          v_sql:=v_sql||'special_days='''||gv_edit_schedule_rec.special_days||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.next_due_date,to_date(c_not_null_num,'J'))<>nvl(v_task_spec.next_due_date,to_date(c_not_null_num,'J')))then
          v_sql:=v_sql||'next_due_date='''||gv_edit_schedule_rec.next_due_date||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.repeats,c_not_null_num)<>nvl(v_task_spec.repeats,c_not_null_num))then
          v_sql:=v_sql||'repeats='''||gv_edit_schedule_rec.repeats||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.repeat_interval,c_not_null_num)<>nvl(v_task_spec.repeat_interval,c_not_null_num))then
          v_sql:=v_sql||'repeat_interval='''||gv_edit_schedule_rec.repeat_interval||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.repeat_periodic,c_not_null_num)<>nvl(v_task_spec.repeat_periodic,c_not_null_num))then
          v_sql:=v_sql||'repeat_periodic='''||gv_edit_schedule_rec.repeat_periodic||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.effective_date_offset,c_not_null_num)<>nvl(v_task_spec.effective_date_offset,c_not_null_num))then
          v_sql:=v_sql||'effective_date_offset='''||gv_edit_schedule_rec.effective_date_offset||''',';
        end if;
        if(nvl(gv_edit_schedule_rec.modal,c_not_null)<>nvl(v_task_spec.modal,c_not_null))then
          v_sql:=v_sql||'modal='''||gv_edit_schedule_rec.modal||''',';
        end if;        
        if(nvl(gv_edit_schedule_rec.ignore_error,c_not_null)<>nvl(v_task_spec.ignore_error,c_not_null))then
          v_sql:=v_sql||'ignore_error='''||gv_edit_schedule_rec.ignore_error||''',';
        end if;        
        v_sql:=v_sql||'change_reason='''||gv_edit_schedule_rec.change_reason||''',';
        v_sql:=v_sql||'dependency_sql = null where task_id = '||gv_session_task_edit_lock; 
        execute immediate v_sql;
      end if;
    end if;
  end if;
  -- Commit changes if no errors
  commit;
  -- Undo locks
  sched.set_task_state(gv_edit_schedule_rec,gv_edit_schedule_rec.prev_state);
  release_session_lock;
  sched.tx_heartbeat(gv_edit_schedule_rec.task_id||'->EDIT_COMMIT');
  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end edit_commit;

-------------------------------------------------------------------------------
-- Cancels changes and releases a Lock on a task that was edited
procedure edit_cancel
is
begin
  if(gv_session_task_edit_lock is null)then
    raise utl.pkg_exceptions.e_scheduler_task_not_edit;
  else
    -- Set previous state without affecting the data
    sched.set_task_state(gv_edit_schedule_rec,gv_edit_schedule_rec.prev_state);
    gv_session_task_edit_lock := null;
    sched.tx_heartbeat(gv_edit_schedule_rec.task_id||'->EDIT_CANCEL');
  end if;
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
end edit_cancel;

-------------------------------------------------------------------------------
-- Removes task and peer definitions from scheduler
-- Parameters:
--  Specify task_id in p_task_id
procedure task_delete(p_task_id in schedules.task_id%type)
is
  pragma   autonomous_transaction;
  c_proc_name  constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_delete';
  v_task_spec  sched.t_schedule_rec;
begin
  dbms_application_info.set_module(c_proc_name,null);
  check_task_lock(p_task_id);
  -- Task peers
  delete task_peers
   where task_peer1 = p_task_id
      or task_peer2 = p_task_id;
  -- Group peers
  delete task_group_peers
   where (    group_peer1 = v_task_spec.group_name
           or group_peer2 = v_task_spec.group_name
         )
     and ( select count(*)  
             from schedules
            where group_name = v_task_spec.group_name
         ) = 0; -- This was the last VOLATILE task of this group name         
  -- Task itself         
  delete schedules
   where task_id=p_task_id;
  if(sql%rowcount=0)then
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;   
  commit;
  sched.tx_heartbeat(p_task_id||'->DELETE');
  dbms_application_info.set_module(null,null);
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_delete;


-------------------------------------------------------------------------------
-- Suspend task, causing the task and any dependent tasks not to execute.
-- If the tasks is already running, the tasks will, however, complete.
function task_suspend(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
  v_task_spec sched.t_schedule_rec;
  v_retcode    utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
    if(v_task_spec.state=sched.gc_state_INITIAL
    or v_task_spec.state=sched.gc_state_WAITING
    or v_task_spec.state=sched.gc_state_DUE
    or v_task_spec.state=sched.gc_state_READY
    or v_task_spec.state=sched.gc_state_EXCLUDED
    or v_task_spec.state=sched.gc_state_RETRY
    or v_task_spec.state=sched.gc_state_DONE)
    then
      sched.set_task_state(v_task_spec,sched.gc_state_SUSPENDED,sysdate);
    else
      v_retcode:=utl.pkg_exceptions.gc_scheduler_inv_state;
    end if;
  end if;
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_suspend;

-------------------------------------------------------------------------------
-- Resume task if it was in SUSPENDED state
-- Console program
function task_resume(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
  c_proc_name  constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_resume';
  v_task_spec  sched.t_schedule_rec;
  v_retcode    utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
    if(v_task_spec.state=sched.gc_state_SUSPENDED)then
      sched.set_task_state(v_task_spec,sched.gc_state_RESUMED,sysdate);
    else
      v_retcode:=utl.pkg_exceptions.gc_scheduler_inv_state;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_resume;

-------------------------------------------------------------------------------
-- Disable task, regardless of the state of the task
function task_disable(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_disable';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
    if(v_task_spec.state=sched.gc_state_INITIAL
    or v_task_spec.state=sched.gc_state_WAITING
    or v_task_spec.state=sched.gc_state_DUE
    or v_task_spec.state=sched.gc_state_READY
    or v_task_spec.state=sched.gc_state_SUSPENDED
    or v_task_spec.state=sched.gc_state_RESUMED
    or v_task_spec.state=sched.gc_state_EXCLUDED
    or v_task_spec.state=sched.gc_state_ABORTED
    or v_task_spec.state=sched.gc_state_RETRY
    or v_task_spec.state=sched.gc_state_ERROR
    or v_task_spec.state=sched.gc_state_BROKEN
    or v_task_spec.state=sched.gc_state_TIMEDOUT
    or v_task_spec.state=sched.gc_state_DONE)
    then
      sched.set_task_state(v_task_spec,sched.gc_state_DISABLED,sysdate);
    else
      v_retcode:=utl.pkg_exceptions.gc_scheduler_user_operation;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_disable;

-------------------------------------------------------------------------------
-- Reset a hung task to the READY state
function task_reset(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_reset';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_task_spec.task_id:=p_task_id;
  v_retcode:=sched.get_task_details(v_task_spec);
  if(v_retcode=utl.pkg_exceptions.gc_success)then
    if(v_task_spec.state in (sched.gc_state_ERROR,
                             sched.gc_state_TIMEDOUT,
                             sched.gc_state_DISABLED,
                             sched.gc_state_UNDEFINED,
                             sched.gc_state_BROKEN,
                             sched.gc_state_INITIAL,
                             sched.gc_state_DONE,
                             sched.gc_state_RETRY,
                             sched.gc_state_WAITING,
                             sched.gc_state_ABORTING,
                             sched.gc_state_ABORTED))
    then
      sched.safe_job_remove(v_task_spec.queue_id);
      sched.set_task_state(v_task_spec,sched.gc_state_INITIAL,sysdate);
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'Task has been reset from '||v_task_spec.state||' to '||sched.gc_state_INITIAL,null,p_task_id);
    else
      v_retcode:=utl.pkg_exceptions.gc_scheduler_user_operation;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_reset;

------------------------------------------------------------------------------
-- Set the task to pre-launch regardless of constraints
function task_run_now(p_task_id in schedules.task_id%type) return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_run_now';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  l_child_nodes       sched.t_schedules;        -- List of tree nodes
  v_node_count        pls_integer;  
  v_pos               pls_integer;  
  v_msg               varchar2(2000);
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
    if(v_task_spec.state not in(sched.gc_state_EXECUTING,
                                sched.gc_state_ABORTING,
                                sched.gc_state_BROKEN,
                                sched.gc_state_ERROR,
                                sched.gc_state_TIMEDOUT,
                                sched.gc_state_DONE,
                                sched.gc_state_DISABLED,
                                sched.gc_state_UNDEFINED,
                                sched.gc_state_EDIT_LOCK) )
    then
      sched.set_task_state(v_task_spec,sched.gc_state_READY,sysdate);
      -- Helpful diagnosis
      if(v_task_spec.next_due_date is null)then
        v_msg:='User forced task to execute now. The task does not have a ''next due date'', '||
               'because no recurring pattern has been defined.';          
      else
        v_msg:='User forced task to execute now, although it due to execute at '||
                to_char(v_task_spec.next_due_date,gc_datetime_format);
      end if;
      -- Show effect on child tasks      
      v_node_count:=scheduler_dep.get_child_nodes(p_task_id,l_child_nodes);
      v_pos:=l_child_nodes.first(); 
      v_pos:=l_child_nodes.next(v_pos);  -- start at second task
      if(v_pos is not null)then
        v_msg:=v_msg||chr(10)||'The following child tasks are affected by this action:';
        while(v_pos is not null)loop
          v_msg:=v_msg||chr(10)||' - Task Id '||l_child_nodes(v_pos).task_id||' '||
                                 l_child_nodes(v_pos).group_name||':'||l_child_nodes(v_pos).operation_id;
          v_pos:=l_child_nodes.next(v_pos); 
        end loop;
      else
        v_msg:=v_msg||' No other tasks are affected by this action.';
      end if;      
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,v_msg,null,p_task_id);
    else
      v_retcode:=utl.pkg_exceptions.gc_scheduler_user_operation;
    end if;
  else
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_run_now;

-- Forwards a task to the next due date
-- Returns a list of all tasks that were forwarded
function task_forward(p_task_id in schedules.task_id%type,p_date_due out date) return sched.t_schedules
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_forward';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  l_child_nodes       sched.t_schedules;        -- List of tree nodes
  v_tasks_ok          boolean;
  v_node_count        pls_integer;
  v_ref_date          date;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(not is_session_edit_locked)then
    v_task_spec.task_id:=p_task_id;
    v_retcode:=sched.get_task_details(v_task_spec);
    if(v_retcode=utl.pkg_exceptions.gc_success)then
      -- Sanity check - can this task actually be forwarded?
      if(v_task_spec.year is not null and v_task_spec.month is not null and 
         v_task_spec.day is not null  and v_task_spec.hour is not null and v_task_spec.minute is not null
      )then              
        raise utl.pkg_exceptions.e_scheduler_task_forward;
      end if;
    
      -- Get all child nodes from this task
      v_node_count:=scheduler_dep.get_child_nodes(p_task_id,l_child_nodes);
      -- Check if all tasks can be locked for editing
      if(are_tasks_editable(l_child_nodes))then
        -- Check that tasks in tree can be forwarded
        for i in l_child_nodes.first..l_child_nodes.last loop
          if(l_child_nodes(i).state=sched.gc_state_EDIT_LOCK)then
            raise utl.pkg_exceptions.e_scheduler_task_edit_lock;
          end if;
          if(l_child_nodes(i).state=sched.gc_state_DISABLED)then
            raise utl.pkg_exceptions.e_scheduler_task_disable;
          end if;
          if(l_child_nodes(i).state=sched.gc_state_EXECUTING)then
            raise utl.pkg_exceptions.e_scheduler_task_busy;
          end if;
        end loop;
        begin
          -- Attempt to Lock all tasks for editing including this task
          for i in  l_child_nodes.first..l_child_nodes.last loop
            sched.set_task_state(l_child_nodes(i),sched.gc_state_EDIT_LOCK);
            -- Update local cursor value
            l_child_nodes(i).prev_state:=l_child_nodes(i).state;
            l_child_nodes(i).state:=sched.gc_state_EDIT_LOCK;
          end loop;
          -- Forward tasks                        
          -- Calculate the forwarded dates for all the tasks before we update them
          -- Use the reference date in case this task hasnot run yet
          v_ref_date:=sysdate;
          for i in  l_child_nodes.first..l_child_nodes.last loop
            -- Not all tasks, particularly child tasks have a next-due-date set, since they 
            -- have not recurring patterns. This is because they will simply execute immediately 
            -- after the parent task has completed.
            if(l_child_nodes(i).next_due_date is not null)then
              v_retcode:=scheduler_due.calc_next_due_date(l_child_nodes(i),nvl(l_child_nodes(i).next_due_date,v_ref_date),p_date_due);
              if(v_retcode=utl.pkg_exceptions.gc_success)then
                if(l_child_nodes(i).next_due_date is null)then
                  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,
                    'The next due date for the task to be forwarded has not yet been calculated. '||
                    'The task will be forwarded based on the current system time',null,l_child_nodes(i).task_id);
                end if;
                utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'Forwarding task from '||
                  to_char(nvl(l_child_nodes(i).next_due_date,v_ref_date),gc_datetime_format)||' to '||
                  to_char(p_date_due,gc_datetime_format),null,l_child_nodes(i).task_id);
                l_child_nodes(i).next_due_date:=p_date_due;
              else
                raise utl.pkg_exceptions.e_scheduler_next_due_date;
              end if;
            end if;
          end loop;
          -- Update this and all child tasks in dependency tree
          for i in  l_child_nodes.first..l_child_nodes.last loop
            update schedules
               set next_due_date=l_child_nodes(i).next_due_date
             where task_id = l_child_nodes(i).task_id;
          end loop;
          commit;
          -- Undo all locks by setting to the previous state
          -- All tasks are set to the WAITING state - the scheduler will sort the rest out
          for i in  l_child_nodes.first..l_child_nodes.last loop
            -- Update table's state
            sched.set_task_state(l_child_nodes(i),sched.gc_state_INITIAL);
            l_child_nodes(i).state:=sched.gc_state_INITIAL;
          end loop;
        exception
          when others then
            -- Undo all locks
            for i in  l_child_nodes.first..l_child_nodes.last loop
              sched.set_task_state(l_child_nodes(i),l_child_nodes(i).prev_state);
            end loop;
            raise;
        end;
      else
        raise utl.pkg_exceptions.e_scheduler_task_edit_lock;
      end if;
    else
      raise utl.pkg_exceptions.e_scheduler_user_operation;
    end if;
  else
    raise utl.pkg_exceptions.e_scheduler_task_edit_user;
  end if;
  dbms_application_info.set_module(null,null);
  return l_child_nodes;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_forward;

-------------------------------------------------------------------------------
-- Aborts the currently-running task
--
-- Parameters:
--  Specify task_id in p_task_id
--
-- Returns:
--  0 on success
--  gc_scheduler_task_exist   if task does not exist
--  gc_scheduler_task_timeout if no confirmation was received that the task is not running
--  gc_scheduler_task_busy    if the FSM should retry aborting the task again
--  gc_scheduler_task_abort   if the task is not executing
function task_abort(p_task_id in schedules.task_id%type, p_ref_date in date:=sysdate)
return utl.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_abort';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_task_spec.task_id:=p_task_id;
  if(sched.get_task_details(v_task_spec)=utl.pkg_exceptions.gc_success)then
    v_retcode:=task_abort(v_task_spec,p_ref_date);
  else
    raise utl.pkg_exceptions.e_scheduler_task_exist;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    raise;
end task_abort;

-- Aborts a currently-executing task
--  0. Prevent further processing by the FSM by putting task state to ABORTING
--  1. Break the task in DBMS_JOB
--  2. Remove the task from DBMS_JOB
-- To abort a SQL task:
--  3. Get SID  and SERIAL# of the process
--  4. As Oracle user sys: alter system kill session 'sid,serial#'
--  5. Check killed state of session for a while
--  6. Return on confirmation or timeout
-- To abort an O/S SHELL task:
--  3. Get the O/S PID for the task
--  4. As O/S user oracle: kill -9 PID
--  5. Check killed state of process for a while
--  6. Return on confirmation or timeout
function task_abort(p_task_spec in  sched.t_schedule_rec, p_ref_date in date:=sysdate)
return utl.global.t_error_code
is
  pragma autonomous_transaction;
  c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_abort';
  v_retcode       utl.global.t_error_code := utl.pkg_exceptions.gc_success;
  v_oscode        pls_integer;
  v_task_spec     sched.t_schedule_rec    := p_task_spec;
  v_kill_cmd      varchar2(2000);
  v_pid           schedules.process_id%type;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_task_spec.state<>sched.gc_state_EXECUTING)then
    raise utl.pkg_exceptions.e_scheduler_task_abort;
  end if;

  -- Put the DBMS_JOB on hold ASAP
  begin
    dbms_job.broken(p_task_spec.queue_id,TRUE);
    commit;
  exception
    when others then
      -- The job may already be removed.
      null;
  end;

  -- Stop any FSM processing for this task right now
  sched.set_task_state(v_task_spec,sched.gc_state_ABORTING,p_ref_date);

  if(v_task_spec.command_type=sched.gc_command_type_EXTERNAL_PROC)then
    -- Shell process
    -- Break and remove task from DBMS_JOB
    sched.safe_job_remove(v_task_spec.queue_id);
    v_retcode:=sched.is_os_process_done(v_task_spec,p_ref_date);
    if(v_retcode=utl.pkg_exceptions.gc_scheduler_trancient_state)then
      -- Task just got launched and we have not got the PID yet - wait a minute
      v_retcode := utl.pkg_exceptions.gc_success;
      if(p_task_spec.state_tmstmp+1/1440>p_ref_date)then
        -- A minute has expired. The PID is not known, and the task cannot possibly still be
        -- in a just-launched transcient state!
        -- Have to assume that the task may actually be running
        -- Kill the task the crude way by killing all similarly-named processes
        v_oscode:=utl.hostcmd('kill -9 $(ps -ef | grep '||p_task_spec.command||' | awk ''{print $2}'') > /dev/null');
        if(v_oscode<>0)then
          raise utl.pkg_exceptions.e_scheduler_task_lost;
        end if;
        -- Check if the task was removed
        dbms_lock.sleep(1);
        v_oscode:=utl.hostcmd('kill -9 $(ps -ef | grep '||p_task_spec.command||' | awk ''{print $2}'') > /dev/null');
        if(v_oscode=0)then
          -- The process is still there and still in the process of aborting
          raise utl.pkg_exceptions.e_scheduler_task_busy;
        end if;
      else
        -- The task is still in a just-launched transcient state,
        -- (probably because it just got launched? :-)
        -- and this abort_task procedure will need to be called again
        raise utl.pkg_exceptions.e_scheduler_trancient_state;
      end if;
    elsif(v_retcode= utl.pkg_exceptions.gc_scheduler_task_busy)then
      if(p_task_spec.process_id=-1)then
        -- The PID was not caught when the process was launched.
        v_retcode := utl.pkg_exceptions.gc_scheduler_task_lost;
      else
        v_retcode := utl.pkg_exceptions.gc_success;
        -- We have the O/S PID and know that the process is still running on the O/S
        -- Kill process off:
        v_oscode:=utl.hostcmd('kill -9 '||p_task_spec.process_id||' > /dev/null 2>'||chr(38)||'1');
        if(v_oscode=0)then
          -- Killing the process off was probably successful
          -- Check if it really is gone after a short while
          dbms_lock.sleep(1);
          v_oscode:=utl.hostcmd('kill -9 '||p_task_spec.process_id||' > /dev/null 2>'||chr(38)||'1');
          if(v_oscode=0)then
            -- The process is still there and still in the process of aborting
            raise utl.pkg_exceptions.e_scheduler_task_busy;
          end if;
        else
          -- Task may already have terminated
          raise utl.pkg_exceptions.e_scheduler_task_lost;
        end if;
      end if;
    --else
      -- The process has actually completed - do nothing further
    end if;
  else
    -- This is a SQL-based process. Get SID and SERIAL# and kill the process.
    v_kill_cmd:=
'ORACLE_SID=<ORACLE_SID> sqlplus -s / <LOGGING> <<!
set echo off
var retcode number;
declare
  v_cmd varchar2(1000);
  v_sid     number;
  v_serial# number;
  v_spid    number;
  v_status  varchar2(20);
begin
  :retcode:=1;
  for r in (select p.sid     as sid,
                   p.serial# as serial#,
                   p.spid    as spid
              from dba_jobs_running jr,
                   (select p.spid,
                           s.sid,
                           s.serial#
                      from v\$process p,
                           v\$session s
                     where p.addr = s.paddr) p
             where jr.job='||p_task_spec.queue_id||'
               and p.sid=jr.sid)
  loop
    v_sid     :=r.sid;
    v_serial# :=r.serial#;
    v_spid    :=r.spid;
    utl.pkg_logger.log('''||utl.pkg_logger.gc_log_message_info||''',''Killing Oracle session ''||v_sid||'',''||v_serial#||'', Process Id ''||v_spid||'', for Task Id '||p_task_spec.task_id||'.'');

    -- Update SPID to the schedule table
    update scheduler.schedules s
       set s.process_id=r.spid
     where s.task_id='||p_task_spec.task_id||';
    commit;
    v_cmd:=''alter system kill session ''''''||r.sid||'',''||r.serial#||'''''''';
    execute immediate v_cmd;
    :retcode:=0;
  end loop;
exception
  when others then
    :retcode:=sqlcode;
    if(v_serial# is not null)then
      select status
        into v_status
        from v\$session
       where sid=v_sid
         and serial#=v_serial#;
      if(v_status=''KILLED'')then
        utl.pkg_logger.log('''||utl.pkg_logger.gc_log_message_info||''',:retcode);
      else
        utl.pkg_logger.log('''||utl.pkg_logger.gc_log_message_error||''',:retcode);
      end if;
    else
      utl.pkg_logger.log('''||utl.pkg_logger.gc_log_message_error||''',''Could not kill Oracle session for Task Id '||p_task_spec.task_id||'.'');
    end if;
end;
'||chr(47)||'
quit :retcode
!
RETCODE=$?
exit $RETCODE
';

    sched.fit_unix_environment(v_kill_cmd);
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'Killing task Id '||p_task_spec.task_id||' using the command:'||chr(10)||v_kill_cmd);
    v_oscode:=utl.hostcmd(v_kill_cmd);
    -- Break and remove task from DBMS_JOB - but only now since the v_kill_cmd relies on the DBMS_JOB to still be there.
    sched.safe_job_remove(v_task_spec.queue_id);
    if(mod(v_oscode,256)<>0)then
      -- Could not kill sid,serial#
      raise utl.pkg_exceptions.e_scheduler_task_abort;
    else
      -- Managed to kill Oracle Session
      -- Now kill the O/S process off
      begin
        select s.process_id
          into v_pid
          from schedules s
         where s.task_id = p_task_spec.task_id;
        if(v_pid is not null)then
          v_oscode:=utl.hostcmd('kill -9 '||v_pid||' > /dev/null 2>'||chr(38)||'1');
          -- Check if the task was removed
          dbms_lock.sleep(1);
          v_oscode:=utl.hostcmd('kill -9 '||v_pid||' > /dev/null 2>'||chr(38)||'1');
          if(v_oscode=0)then
            -- The process is still there and still in the process of aborting
            raise utl.pkg_exceptions.e_scheduler_task_busy;
          end if;
        end if;
      exception 
        when others then
          null;
      end;
    end if;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log(p_parent_id => p_task_spec.task_id);
    raise;
end task_abort;

-- Adds a task to the schedule
-- Returns the resulting task_id
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
  p_change_reason     in schedules.change_reason%type:=null,
  p_commit            in boolean := true
) return schedules.task_id%type
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_add';
  --v_retcode       pls_integer:=1;
begin
  dbms_application_info.set_module(c_proc,null);
  insert_begin;
  set_task_attr(p_submitted_by             =>p_submitted_by,
                p_task_type                =>p_task_type,
                p_task_priority            =>p_task_priority,
                p_group_name               =>p_group_name,
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
                p_change_reason            =>p_change_reason);  -- populates gv_edit_schedule_rec

  if(not is_session_edit_locked)then
    raise utl.pkg_exceptions.e_scheduler_task_not_edit;
  end if;
  -- Get next task Id
  gv_edit_schedule_rec.task_id:=get_next_task_id;
  insert_task(gv_edit_schedule_rec,p_task_peers,p_group_priority,p_commit);  
  release_session_lock;   
  dbms_application_info.set_module(null,null);
  return gv_edit_schedule_rec.task_id;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_add;

-- Adds a task to the schedule
-- Returns the resulting task_id
function task_add(p_task_spec in out sched.t_schedule_rec,
                  p_task_peers        in varchar2:=null,
                  p_group_priority    in task_groups.group_priority%type:=null,
                  p_commit    in boolean := true)
return schedules.task_id%type
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_add';
  --v_retcode       pls_integer:=1;
begin
  dbms_application_info.set_module(c_proc,null);
  insert_begin;
  set_task_attr(p_task_spec);   -- populates gv_edit_schedule_rec
  
  if(not is_session_edit_locked)then
    raise utl.pkg_exceptions.e_scheduler_task_not_edit;
  end if;
  -- Get next task Id
  gv_edit_schedule_rec.task_id:=get_next_task_id;
  insert_task(gv_edit_schedule_rec,p_task_peers,p_group_priority,p_commit);
  release_session_lock;
  dbms_application_info.set_module(null,null);
  return gv_edit_schedule_rec.task_id;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_add;

-- Insert or Update task peer values for the given comma-delimited 
-- list of task Id's
procedure task_peers(p_task_id in schedules.task_id%type, p_task_peers in varchar2, p_commit boolean:=true)
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_add';
  l_list dbms_sql.varchar2s;
begin
  dbms_application_info.set_module(c_proc,null);
  if(p_task_peers is not null)then
    delete task_peers
     where task_peer1=p_task_id
        or task_peer2=p_task_id;
    l_list:=utl.pkg_string.string2list(p_task_peers);
    for i in l_list.first..l_list.last loop
      if(l_list(i) is not null)then
        insert into task_peers(task_peer1,task_peer2)
        values (p_task_id,upper(l_list(i)));      
      end if;
    end loop;
    -- Remove duplicate references
    -- First sort alphanumerically
    update task_peers
       set task_peer1 = task_peer2,
           task_peer2 = task_peer1
     where task_peer2 < task_peer1;
    delete task_peers a
     where rowid <> (
             select max(rowid)
               from task_peers b             
              where a.task_peer1 = b.task_peer1
                and a.task_peer2 = b.task_peer2
           ); 
    if(p_commit)then
      commit;
    end if;           
  end if;           
  dbms_application_info.set_module(null,null);                 
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end task_peers;

-------------------------------------------------------------------------------
-- GROUP MODIFICATION
-------------------------------------------------------------------------------

-- Inserts or updates task group's priority
procedure group_priority(p_group_name task_groups.group_name%type,
                         p_priority   task_groups.group_priority%type:=0,
                         p_commit boolean:=true)
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.group_priority';
begin
  dbms_application_info.set_module(c_proc,null);
  if(p_group_name is not null)then
    update task_groups
       set group_priority=nvl(p_priority,0)    
     where group_name = upper(p_group_name);
    if(sql%rowcount=0)then
      insert into task_groups(group_name,group_priority)
      values (upper(p_group_name),nvl(p_priority,0));
    end if;         
    if(p_commit)then
      commit;
    end if;                   
  end if;
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end group_priority;

-- Peers task groups
procedure group_peers(p_group_name in task_groups.group_name%type,
                      p_group_peers in varchar2,
                      p_commit boolean:=true)
is
  c_proc          constant varchar2(100)  := pc_schema||'.'||pc_package||'.group_peers';
  l_list          dbms_sql.varchar2s;
begin 
  dbms_application_info.set_module(c_proc,null);                    
  if(p_group_peers is not null)then
    delete task_group_peers
     where group_peer1=upper(p_group_name)
        or group_peer2=upper(p_group_name);
    l_list:=utl.pkg_string.string2list(p_group_peers);
    for i in l_list.first..l_list.last loop
      if(l_list(i) is not null)then
        insert into task_group_peers(group_peer1,group_peer2)
        values (p_group_name,upper(l_list(i)));      
      end if;
    end loop;
    -- Remove duplicate references 
    -- First sort alphanumerically
    update task_group_peers
       set group_peer1 = group_peer2,
           group_peer2 = group_peer1
     where group_peer2 < group_peer1;
    delete task_group_peers a
     where rowid <> (
             select max(rowid)
               from task_group_peers b             
              where a.group_peer1 = b.group_peer1
                and a.group_peer2 = b.group_peer2
           ); 
    if(p_commit)then
      commit;
    end if;           
  end if;           
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end group_peers;
                     

-------------------------------------------------------------------------------
-- AD HOC MODS
-------------------------------------------------------------------------------
        
-- Artificially add peer groups against this task group for all task groups 
-- that contain the same name as that of the file source, e.g. BOB                    
-- These items are removed from the task_group_peers once all the tasks have
-- completed executing.
--
-- This relies on the casual understanding that the source names are not 
-- ambiguous and also that they correctly appear in the task group names 
-- for regular tasks.
procedure peer_groups_by_name(p_group_name  in  task_groups.group_name%type,
                              p_source_name in  varchar2)
is
  l_other_groups  dbms_sql.varchar2s;
begin
  select distinct s.group_name
    bulk collect
    into l_other_groups
    from schedules s
   where instr(upper(s.group_name),upper(p_source_name))>0
     and upper(s.group_name) <> upper(p_group_name)
     and substr(p_group_name,1,length(p_group_name)-length(p_source_name))
      <> substr(s.group_name,1,length(p_group_name)-length(p_source_name));
     -- This Above:
     -- Avoid peering of ADHOCPROCESSSS with ADHOCPROCESSBOB
     --                              ~~                ~~ 
     -- because of the reoccurance of 'SS' in both group names.
     -- Remember: this is a slightly crude rule to automate peering of ad-hoc
     -- tasks with tasks for the same source-file-type.
     
  for i in l_other_groups.first..l_other_groups.last loop
    insert into task_group_peers(
           group_peer1,
           group_peer2,
           change_reason)
    values (p_group_name,
           l_other_groups(i),
           'Group peering against Ad hoc task group');
  end loop;   
  -- commited in the calling function below...
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;  
end peer_groups_by_name;                               

-------------------------------------------------------------------------------
-- wrapper procedure to create and schedule one-off task to load
-- a file or files on a adhoc basis
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
function schedule_adhoc_file_load(p_type             in integer,
                                   p_source_name      in varchar2,
                                   p_as_of_date       in date,
                                   p_basis            in varchar2,
                                   p_version          in varchar2,
                                   p_file             in varchar2,
                                   p_source_file_type in varchar2
) return utl.global.t_error_code is
  pragma autonomous_transaction;
  v_now             date := sysdate;
  v_last_op_id      schedules.operation_id%type;
  
  v_create_dims_op_id schedules.operation_id%type;
  v_bb_op_id          schedules.operation_id%type;
  v_calc_op_id        schedules.operation_id%type;
  
  v_group_name      schedules.group_name%type;
  v_new_task_id     scheduler.schedules.task_id%type;
  v_task_spec       sched.t_schedule_rec;
  c_task_spec_null  constant sched.t_schedule_rec:=null;
  v_adhoc_load_command varchar2(50) := utl.pkg_config.get_variable_string(gc_adhoc_load_command_cf_key);
begin
  if v_adhoc_load_command is null then
    -- Insufficient parameters provided for adhoc file load
    raise utl.pkg_exceptions.e_incomplete_adhoc_params;
  end if;

  if(is_session_edit_locked)then
    -- A task is already being edited in this session
    raise utl.pkg_exceptions.e_scheduler_task_edit_user;
  end if;

  gv_edit_schedule_rec:=c_task_spec_null;
    
  -- make up group name
  v_group_name := 'ADHOCPROCESS'||upper(p_source_name);

  -- If there is already a adhoc process for this source get the op id of the
  -- last task and make a dependency for the first task of the new request on
  -- that last task this is to prevent parallel adhoc loads for the same source.
  select max(operation_id)
    into v_last_op_id
    from schedules
   where group_name = v_group_name;
  
  -- Common change reason
  v_task_spec.change_reason := 'Ad Hoc Process';

  IF p_type = 4 -- if revalidation; task to create load run
  THEN
    v_task_spec  := c_task_spec_null;

    v_task_spec.operation_id := calc_next_operation_id(v_group_name);
    v_task_spec.task_type    := sched.gc_type_VOLATILE;
    v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
    v_task_spec.year         := to_char(v_now, 'YYYY');
    v_task_spec.month        := to_char(v_now, 'MM');
    v_task_spec.day          := to_char(v_now, 'DD');
    v_task_spec.hour         := to_char(v_now, 'hh24');
    v_task_spec.minute       := to_char(v_now, 'mi');
    v_task_spec.description  := 'Adhoc Process - Create Load Run';
    v_task_spec.group_name   := v_group_name;
    v_task_spec.command      := 'app.pkg_source_load_run_mod.create_load_run(to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''),'''||upper(p_source_name)||''','''||upper(p_basis)||''')';
    
    if v_last_op_id is not null then
      v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
    end if;   
  
    v_new_task_id:=task_add(v_task_spec,null,10,false);
    if(v_new_task_id<0)then
      return v_new_task_id;
    end if;
  ELSE -- otherwise, stage, load and create dims
    -- format task spec for feed2oracle task
    v_task_spec              := c_task_spec_null;
    v_task_spec.operation_id := calc_next_operation_id(v_group_name);
    v_task_spec.task_type    := sched.gc_type_VOLATILE;
    v_task_spec.command_type := sched.gc_command_type_EXTERNAL_PROC;
    v_task_spec.year         := to_char(v_now, 'YYYY');
    v_task_spec.month        := to_char(v_now, 'MM');
    v_task_spec.day          := to_char(v_now, 'DD');
    v_task_spec.hour         := to_char(v_now, 'hh24');
    v_task_spec.minute       := to_char(v_now, 'mi');
    v_task_spec.description  := 'Adhoc Process - Feed2Oracle';
    v_task_spec.group_name   := v_group_name;
  
    if p_type = gc_adhoc_load_default then
      if p_source_name is null
      or p_as_of_date  is null
      or p_basis       is null
      then
        raise utl.pkg_exceptions.e_incomplete_adhoc_params;
      else
        v_task_spec.command := v_adhoc_load_command || ' -s "' || lower(p_source_name)||'"'
                                                    || ' -b "' || lower(p_basis)||'"'
                                                    || ' -a ' || to_char(p_as_of_date, 'yyyymmdd');
      end if;
    elsif p_type = gc_adhoc_load_version_oride then
      if p_source_name is null
      or p_as_of_date  is null
      or p_basis       is null
      or p_version     is null
      then
        raise utl.pkg_exceptions.e_incomplete_adhoc_params;
      else
        v_task_spec.command := v_adhoc_load_command || ' -s "' || lower(p_source_name)||'"'
                                                    || ' -b "' || lower(p_basis)||'"'
                                                    || ' -a ' || to_char(p_as_of_date, 'yyyymmdd')
                                                    || ' -v ' || p_version;
      end if;
    elsif p_type = gc_adhoc_load_specific_file then
      if p_source_name is null
      or p_as_of_date  is null
      or p_basis       is null
      or p_file        is null
      or p_source_file_type is null
      then
        raise utl.pkg_exceptions.e_incomplete_adhoc_params;
      else
        v_task_spec.command := v_adhoc_load_command || ' -s "' || lower(p_source_name)||'"'
                                                    || ' -b "' || lower(p_basis)||'"'
                                                    || ' -a ' || to_char(p_as_of_date, 'yyyymmdd')
                                                    || ' -f "' || p_file||'"'
                                                    || ' -sf "' || lower(p_source_file_type)||'"';
      end if;
    else
      raise utl.pkg_exceptions.e_incomplete_adhoc_params;
    end if;
  
    if v_last_op_id is not null then
      v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
    end if;   
  
    v_new_task_id:=task_add(v_task_spec,null,10,false);
    if(v_new_task_id<0)then
      return v_new_task_id;
    end if;
  
    v_last_op_id := v_task_spec.operation_id;
    v_task_spec  := c_task_spec_null;
  
    v_task_spec.operation_id := calc_next_operation_id(v_group_name);
    v_task_spec.task_type    := sched.gc_type_VOLATILE;
    v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
    v_task_spec.year         := to_char(v_now, 'YYYY');
    v_task_spec.month        := to_char(v_now, 'MM');
    v_task_spec.day          := to_char(v_now, 'DD');
    v_task_spec.hour         := to_char(v_now, 'hh24');
    v_task_spec.minute       := to_char(v_now, 'mi');
    v_task_spec.description  := 'Adhoc Process - Load From Staging';
    v_task_spec.group_name   := v_group_name;
    v_task_spec.command      := 'app.pkg_loader.load('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
    v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
  
    v_new_task_id:=task_add(v_task_spec,null,10,false);
    if(v_new_task_id<0)then
      return v_new_task_id;
    end if;
  
    -- Save OPId for use in child's dependency expression
    v_last_op_id := v_task_spec.operation_id;
  
    v_task_spec              := c_task_spec_null;
    v_task_spec.operation_id := calc_next_operation_id(v_group_name);
    v_task_spec.task_type    := sched.gc_type_VOLATILE;
    v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
    v_task_spec.year         := to_char(v_now, 'YYYY');
    v_task_spec.month        := to_char(v_now, 'MM');
    v_task_spec.day          := to_char(v_now, 'DD');
    v_task_spec.hour         := to_char(v_now, 'hh24');
    v_task_spec.minute       := to_char(v_now, 'mi');
    v_task_spec.description  := 'Adhoc Process - Create Dimensions';
    v_task_spec.group_name   := v_group_name;
    v_task_spec.command      := 'app.pkg_dim_creater.create_dims('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
    v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
  
    v_new_task_id:=task_add(v_task_spec,null,10,false);
    if(v_new_task_id<0)then
      return v_new_task_id;
    end if;
  END IF;
  
 -- Save OPId for use in child's dependency expression
  v_last_op_id := v_task_spec.operation_id;
  v_create_dims_op_id := v_last_op_id;
  
  v_task_spec              := c_task_spec_null;
  v_task_spec.operation_id := calc_next_operation_id(v_group_name);
  v_task_spec.task_type    := sched.gc_type_VOLATILE;
  v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
  v_task_spec.year         := to_char(v_now, 'YYYY');
  v_task_spec.month        := to_char(v_now, 'MM');
  v_task_spec.day          := to_char(v_now, 'DD');
  v_task_spec.hour         := to_char(v_now, 'hh24');
  v_task_spec.minute       := to_char(v_now, 'mi');
  v_task_spec.description  := 'Adhoc Process - Get Webmark Data';
  v_task_spec.group_name   := v_group_name;
  v_task_spec.command      := 'app.pkg_ref_object_mod.get(''WEBMARK'',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
  v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
  
  v_new_task_id:=task_add(v_task_spec,null,10,false);
  if(v_new_task_id<0)then
    return v_new_task_id;
  end if;
  
 -- Save OPId for use in child's dependency expression
  v_last_op_id := v_task_spec.operation_id;
  
  v_task_spec              := c_task_spec_null;
  v_task_spec.operation_id := calc_next_operation_id(v_group_name);
  v_task_spec.task_type    := sched.gc_type_VOLATILE;
  v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
  v_task_spec.year         := to_char(v_now, 'YYYY');
  v_task_spec.month        := to_char(v_now, 'MM');
  v_task_spec.day          := to_char(v_now, 'DD');
  v_task_spec.hour         := to_char(v_now, 'hh24');
  v_task_spec.minute       := to_char(v_now, 'mi');
  v_task_spec.description  := 'Adhoc Process - Calculate MTD RoR';
  v_task_spec.group_name   := v_group_name;
  v_task_spec.command      := 'app.pkg_calculator.calculate('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
  v_task_spec.dependencies := v_group_name||':'||v_last_op_id;
  
  v_new_task_id:=task_add(v_task_spec,null,10,false);
  
  if(v_new_task_id<0)then
    return v_new_task_id;
  end if;
  
 -- Save OPId for use in child's dependency expression
  v_calc_op_id := v_task_spec.operation_id;  
  
  IF p_type != 4 -- only get bloomberg prices if loading new data
  THEN
    v_task_spec              := c_task_spec_null;
    v_task_spec.operation_id := calc_next_operation_id(v_group_name);
    v_task_spec.task_type    := sched.gc_type_VOLATILE;
    v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
    v_task_spec.year         := to_char(v_now, 'YYYY');
    v_task_spec.month        := to_char(v_now, 'MM');
    v_task_spec.day          := to_char(v_now, 'DD');
    v_task_spec.hour         := to_char(v_now, 'hh24');
    v_task_spec.minute       := to_char(v_now, 'mi');
    v_task_spec.description  := 'Adhoc Process - Get Bloomberg Prices';
    v_task_spec.group_name   := v_group_name;
    v_task_spec.command      := 'app.pkg_bloomberg.get_closing_prices('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
    v_task_spec.dependencies := v_group_name||':'||v_create_dims_op_id;
  
    v_new_task_id:=task_add(v_task_spec,null,10,false);
    
    if(v_new_task_id<0)then
      return v_new_task_id;
    end if;
     -- Save OPId for use in child's dependency expression
    v_bb_op_id := v_task_spec.operation_id;
  END IF;
  
  v_task_spec              := c_task_spec_null;
  v_task_spec.operation_id := calc_next_operation_id(v_group_name);
  v_task_spec.task_type    := sched.gc_type_VOLATILE;
  v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
  v_task_spec.year         := to_char(v_now, 'YYYY');
  v_task_spec.month        := to_char(v_now, 'MM');
  v_task_spec.day          := to_char(v_now, 'DD');
  v_task_spec.hour         := to_char(v_now, 'hh24');
  v_task_spec.minute       := to_char(v_now, 'mi');
  v_task_spec.description  := 'Adhoc Process - Validation';
  v_task_spec.group_name   := v_group_name;
  v_task_spec.command      := 'app.pkg_validator.validate('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy''))';
  
  
  IF v_bb_op_id IS NOT NULL -- if we have got bb prices wait for this to finish before starting validation
  THEN
    v_task_spec.dependencies := v_group_name||':'||v_calc_op_id || ' AND ' || v_group_name||':'||v_bb_op_id;
  ELSE
    v_task_spec.dependencies := v_group_name||':'||v_calc_op_id;
  END IF;

  v_new_task_id:=task_add(v_task_spec,null,10,false);
  if(v_new_task_id<0)then
    return v_new_task_id;
  end if;

  -- Save OPId for use in child's dependency expression
  v_last_op_id := v_task_spec.operation_id;

  v_task_spec              := c_task_spec_null;
  v_task_spec.operation_id := calc_next_operation_id(v_group_name);
  v_task_spec.task_type    := sched.gc_type_VOLATILE;
  v_task_spec.command_type := sched.gc_command_type_PROCEDURE;
  v_task_spec.year         := to_char(v_now, 'YYYY');
  v_task_spec.month        := to_char(v_now, 'MM');
  v_task_spec.day          := to_char(v_now, 'DD');
  v_task_spec.hour         := to_char(v_now, 'hh24');
  v_task_spec.minute       := to_char(v_now, 'mi');
  v_task_spec.description  := 'Adhoc Process - Complete Load';
  v_task_spec.group_name   := v_group_name;
  
  IF p_type = 4 -- if revalidation then completion is different
  THEN
      v_task_spec.command := 'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy'')),''Y'')';
  ELSE 
      v_task_spec.command := 'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run('''||upper(p_source_name)||''','''||upper(p_basis)||''',to_date('''||to_char(p_as_of_date,'dd-MON-yyyy')||''',''dd-MON-yyyy'')),''N'')';
  END IF;
    
  v_task_spec.dependencies := v_group_name||':'||v_last_op_id;

  v_new_task_id:=task_add(v_task_spec,null,10,false);
  if(v_new_task_id<0)then
    return v_new_task_id;
  end if;  

  -- The task group needs to be peered with other task groups so that only one
  -- AD-HOC load operation can run at a time
  peer_groups_by_name(v_group_name,p_source_name);

  -- Commit our inserts
  commit;
  
  return utl.pkg_exceptions.gc_success;
exception
  when others then
    rollback;
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident => false);
    raise;
end schedule_adhoc_file_load;

begin
  gc_datetime_format      :=nvl(utl.pkg_config.get_variable_string(gc_config_key_datetimeformat),'YYYY/MM/DD HH24:MI');
end scheduler_mod;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
