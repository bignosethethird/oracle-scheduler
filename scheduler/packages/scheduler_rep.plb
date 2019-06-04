create or replace package body scheduler.scheduler_rep
as

------------------------------------------------------------------------
------------------------------------------------------------------------
-- Reporting functions
------------------------------------------------------------------------

--===========================================================================--
-- GLOBAL SESSION-WIDE VARIABLES
--===========================================================================--

--===========================================================================--
-- PUBLIC FUNCTIONS
--===========================================================================--

-------------------------------------------------------------------------------
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
  p_dependency  in  varchar2                   := null
  ) return utl.global.t_result_set
is
  v_result_set        utl.global.t_result_set;
begin
  open v_result_set for
    select s.task_id,
           s.group_name,
           s.operation_id,
           s.task_type,
           s.description,
           s.state,
           to_char(s.state_tmstmp, 'dd-MON-yyyy hh24:mi:ss'),
           to_char(s.next_due_date, 'dd-MON-yyyy hh24:mi'),
           decode(s.command, null, null, decode(floor(length(s.command)/30),0,s.command,rpad(s.command,30)||'...')),
           decode(s.dependencies, null, null, decode(floor(length(s.dependencies)/30),0,s.dependencies,rpad(s.dependencies,30)||'...'))
    from   schedules s
    where  ((p_description is null) or (p_description is not null and s.description like p_description))
    and    ((p_command is null) or (p_command is not null and s.command like p_command))
    and    ((p_state is null) or (p_state is not null and s.state = p_state))
    and    ((p_group_name is null) or (p_group_name is not null and s.group_name = p_group_name))
    and    ((p_dependency is null) or (p_dependency is not null and s.dependencies like p_dependency))
    order by s.group_name, s.operation_id;

  return v_result_set;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_errorhandler.log_sqlerror(p_incident=>FALSE);
    raise;
end get_tasks_list;

------------------------------------------------------------------------------
-- Report circular references between tasks.
--
-- Every time that the scheduler is started or a task is added or amended,
-- a process checks if any tasks make circular references to one-another
-- through their dependency expressions. All offending tasks are disabled
-- and need to be manually resolved.
-- Circular references can be shown as follows, grouped by circle:
--
-- Example:
-- exec scheduler.report_circular_tasks
-- Circular Referenced tasks
-- -------------------------
-- Circle 1:
-- Task Id 1001 (DAILY:30)
-- Task Id 1002 (DAILY:40)
-- Circle 2:
-- Task Id 1003 (DAILY:50)
-- Task Id 1004 (DAILY:60)
-- Task Id 1005 (DAILY:70)
procedure circular_tasks
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.circular_tasks';
  v_result_set        utl.global.t_result_set;
begin
  -- Check if there are any circular references at all
  begin
    open v_result_set for
      select level,
             task_id
        from schedules
     connect by instr(dependencies,group_name||':'||prior operation_id)>0
       order by level,task_id;
  exception
    when others then
      -- Try to work out where the offending trees are
      -- TODO: 
      null;
  end;
end circular_tasks;

-------------------------------------------------------------------------------
-- Get english explanation of when and under which circumstances the task
-- will run. 
--
-- Parameters:
--  Specify task_id
--
-- Returns:
--  Verbose explanation of the task
function task_explanation(p_task_id in schedules.task_id%type)
return varchar2
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_explanation';
  v_exp               varchar2(32000);
  v_len               pls_integer;
  v_task_spec         scheduler.sched.t_schedule_rec;
  v_dependencies      varchar2(1000);
  v_peers             varchar2(1000);
  v_count             pls_integer;
  v_other_task_id     pls_integer;
  v_sysdate           date:=sysdate;
  b_one_off_task      boolean:=false;

  -- Makes up a string that describes the recurring pattern
  function recurring_pattern_desc return varchar2 is
    v_str varchar2(200);
    v_weekdays varchar2(20);
    v_weekday_count pls_integer:=0;
    v_day pls_integer;
    v_hour pls_integer;
    v_minute pls_integer;
  begin
    if( v_task_spec.year   is not null 
    and v_task_spec.month  is not null 
    and v_task_spec.day    is not null 
    and v_task_spec.hour   is not null 
    and v_task_spec.minute is not null)
    then
      v_str:= 'only once on '||
        to_char(to_date(
          v_task_spec.year||v_task_spec.month||v_task_spec.day||v_task_spec.hour||v_task_spec.day,'YYYYMMDDHH24MI'),gc_datetime_format);
    elsif(v_task_spec.month is not null) then     
      if(v_task_spec.day<0)then
        v_day:=utl.pkg_date.days_in_month(v_task_spec.month)-v_task_spec.day;
      else
        v_day:=v_task_spec.day;
      end if;
      v_str:= 'every year on '||
        to_char(to_date(
          v_task_spec.month||v_day||v_task_spec.hour||v_task_spec.minute,'MMDDHH24MI'),gc_datetime_format);
    elsif(v_task_spec.day is not null)then
      v_str:='every month on the '||utl.pkg_math.cardinal2ordinal(v_task_spec.day);
      if(v_task_spec.day<0)then
        v_str:=v_str||'-to-last day of the month';
      else
        v_str:=v_str||' ';      
      end if;
    elsif(v_task_spec.hour is not null)then
      if(v_task_spec.hour<0)then
        v_hour:=24-v_task_spec.hour;
      else
        v_hour:=v_task_spec.hour;
      end if;
      if(v_task_spec.minute<0)then
        v_minute:=60-v_task_spec.minute;
      else
        v_minute:=v_task_spec.minute;
      end if;
      return 'every day at '||v_hour||':'||v_minute;
    elsif(v_task_spec.minute is not null)then
      if(v_task_spec.minute<0)then
        v_str:= 'every hour at '||v_task_spec.minute||' minutes to the hour';
      else
        v_str:= 'every hour at '||v_task_spec.minute||' minutes past the hour';
      end if;
    else
      v_str:='once every minute';
    end if;
    if(v_task_spec.weekdays is not null)then
      v_str:=v_str||' only on ';
      v_weekdays:=replace(v_task_spec.weekdays,'0','7');      
      for i in 0..7 loop
        if(instr(v_task_spec.weekdays,i)>0)then
          v_weekday_count:=v_weekday_count+1;
          case i
            when '1' then v_str:=v_str||'Mondays';
            when '2' then v_str:=v_str||'Tuesdays';
            when '3' then v_str:=v_str||'Wednesdays';
            when '4' then v_str:=v_str||'Thursdays';
            when '5' then v_str:=v_str||'Fridays';
            when '6' then v_str:=v_str||'Saturdays';
            when '7' then v_str:=v_str||'Sundays';
            else v_str:=v_str||'*UNDEFINED DAY*';
          end case;
          v_str:=v_str||', ';
        end if;
      end loop;
      if(v_weekday_count>1)then
        v_str:=v_str||'when the date coincides with one of these days of the week';
      else
        v_str:=v_str||'when the date coincides with this day of the week';
      end if;
    end if;
    return v_str;
  end recurring_pattern_desc;  

  function task_type_string return varchar2 is
  begin
    if(v_task_spec.task_type=sched.gc_type_VOLATILE)then
      return ' and will be removed from the schedule after it has executed or when the scheduler is restarted';
    elsif(v_task_spec.task_type=sched.gc_type_PERSISTENT)then
      if(v_task_spec.repeats is null)then
        return ' and will repeatedly be executed, once every '||v_task_spec.repeat_interval||
               ' minutes until the task runs successfully. This task will continue retrying when the schedule is restarted';
      else
        return ' and will repeatedly be executed, once every '||v_task_spec.repeat_interval||
               ' minutes until the task runs successfully or until it has repeated '||v_task_spec.repeats||
               ' time. This task will continue retrying when the schedule is restarted';
      end if;
    elsif(v_task_spec.task_type=sched.gc_type_TIMECRITICAL)then
      return ' and will not be executed at all if the scheduler is not running at the time that the task is due';
    end if;
    return ''; -- Default: v_task_spec.task_type=gc_type_DURABLE
  end task_type_string;

  -- Get peer groups of which this task is part of  
  function get_peer_groups return varchar2 is
    l_peers dbms_sql.varchar2s;
  begin
    select distinct group_peer
      bulk collect
      into l_peers
      from (
             select tgp1.group_peer1 group_peer
               from task_group_peers tgp1
              where tgp1.group_peer2 = v_task_spec.group_name
              union
             select tgp2.group_peer2 group_peer
               from task_group_peers tgp2
              where tgp2.group_peer1 = v_task_spec.group_name              
           );  
    return utl.pkg_string.list2string(l_peers);
  end get_peer_groups;
  
  -- Look for peer tasks and returns a comma-delimited list 
  function get_peer_tasks return varchar2 is
    l_peers dbms_sql.varchar2s;
  begin
    select s.task_id||' ('||s.group_name||':'||s.operation_id||')'
      bulk collect
      into l_peers
      from schedules s
     where s.task_id in (
             select distinct task_id
               from (
                     select tp1.task_peer1 task_id
                       from task_peers tp1
                      where tp1.task_peer2 = p_task_id
                      union
                     select tp2.task_peer2 task_id
                       from task_peers tp2
                      where tp2.task_peer1 = p_task_id
                    )
           )
     union          
    -- Tasks that are referred to in the peer table but do not exist
    select distinct task_id||' (this task does not exist)'
      from (
            select tp1.task_peer1 task_id
              from task_peers tp1
             where tp1.task_peer2 = p_task_id
             union
            select tp2.task_peer2 task_id
              from task_peers tp2
             where tp2.task_peer1 = p_task_id
           );
           
           
    return utl.pkg_string.list2string(l_peers,', ');
  end get_peer_tasks;
    
begin
  -- Lookup task details
  begin  
    select *
      into v_task_spec
      from vw_fsm_tasks
     where task_id = p_task_id;
  exception
    when no_data_found then
      utl.pkg_logger.log;
      return null;
  end;
  
  if(v_task_spec.year is not null and v_task_spec.month is not null and v_task_spec.day is not null and
     v_task_spec.hour is not null and v_task_spec.minute is not null)
  then
    b_one_off_task:=true;
  end if;
  
  -- Intro:
  v_exp:='+-----------------------------------------------------------------------------+
TaskId:'||lpad(v_task_spec.task_id,7)||' OperationId:'||v_task_spec.operation_id||' TaskGroup:'||v_task_spec.group_name||'
TaskAlias:     '||v_task_spec.group_name||':'||v_task_spec.operation_id||'
Description:   '||nvl(v_task_spec.description,'None')||'
Current State: '||nvl(v_task_spec.state,'Unknown')||'
+-----------------------------------------------------------------------------+
This task ';

  -- Describe current state of task
  if(v_task_spec.state=sched.gc_state_INITIAL)then
    v_exp:=v_exp||'has recently been added to the schedule and has not yet executed';
  elsif(v_task_spec.state=sched.gc_state_WAITING)then
    v_exp:=v_exp||'is now waiting to become due at '||to_char(v_task_spec.next_due_date,gc_datetime_format);
  elsif(v_task_spec.state=sched.gc_state_DUE)then
    v_exp:=v_exp||'is now due ready for launching';
    if(v_dependencies is not null)then
      v_exp:=v_exp||' as soon as all its dependencies have been met';
    end if;
  elsif(v_task_spec.state=sched.gc_state_READY)then
    v_exp:=v_exp||'is currently being launched for execution';
  elsif(v_task_spec.state=sched.gc_state_EXECUTING)then
    v_exp:=v_exp||'started executing at '||to_char(v_task_spec.started_at,gc_datetime_format);
    if(v_task_spec.command_type=sched.gc_command_type_EXTERNAL_PROC)then
      if(v_task_spec.process_id is not null)then
        v_exp:=v_exp||' and runs as O/S Process ID '||v_task_spec.process_id;
      else
        v_exp:=v_exp||'. No Process ID has been registered this task''s instance';
        if((v_sysdate-v_task_spec.started_at)>1/1440)then
          v_exp:=v_exp||', which suggests that there was a problem with the scheduler. You should reattempt the launch by resetting this task';
        else
           v_exp:=v_exp||' since it was launched only moments ago';
        end if;
      end if;
    end if;      
  elsif(v_task_spec.state=sched.gc_state_ABORTING)then
    v_exp:=v_exp||'is currently aborting and may leave data in an indeterminate state';    
  elsif(v_task_spec.state=sched.gc_state_SUSPENDED)then
    v_exp:=v_exp||'has been suspended';
  elsif(v_task_spec.state=sched.gc_state_RESUMED)then
    v_exp:=v_exp||'was previously suspended and has now been resumed';
  elsif(v_task_spec.state=sched.gc_state_EXCLUDED)then
    v_exp:=v_exp||'has been excluded for the current task peer group';
  elsif(v_task_spec.state=sched.gc_state_BROKEN)then
    v_exp:=v_exp||'is broken';
  elsif(v_task_spec.state=sched.gc_state_ERROR)then
    v_exp:=v_exp||'is in an error state';
  elsif(v_task_spec.state=sched.gc_state_TIMEDOUT)then
    v_exp:=v_exp||'or one of the tasks that this task is dependent on, has timed out';
  elsif(v_task_spec.state=sched.gc_state_DONE)then
    v_exp:=v_exp||'has completed its most recent successful execution at '||to_char(v_task_spec.finished_at,gc_datetime_format);
  elsif(v_task_spec.state=sched.gc_state_DISABLED)then
    v_exp:=v_exp||'has been disabled and any tasks dependent on this task will not be executed';
  elsif(v_task_spec.state=sched.gc_state_UNDEFINED)then
    v_exp:=v_exp||'is in an undefined state';
  elsif(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
    v_exp:=v_exp||'is currently locked for editing';
  else
    v_exp:=v_exp||'is in an unknown state';
  end if;

  -- Describe possible reasons for being in thie state
  if(v_task_spec.state in (sched.gc_state_ABORTING,
                           sched.gc_state_SUSPENDED,
                           sched.gc_state_RESUMED,
                           sched.gc_state_EXCLUDED,
                           sched.gc_state_BROKEN,
                           sched.gc_state_ERROR,
                           sched.gc_state_DISABLED,
                           sched.gc_state_UNDEFINED,
                           sched.gc_state_EDIT_LOCK))
  then                           
    v_exp:=v_exp||' because ';
    if(v_task_spec.state=sched.gc_state_ABORTING)then
      v_exp:=v_exp||'either the scheduler is aborting or the task execution has timed out';    
    elsif(v_task_spec.state=sched.gc_state_SUSPENDED)then
      v_exp:=v_exp||'it was manually suspended';
    elsif(v_task_spec.state=sched.gc_state_RESUMED)then
      v_exp:=v_exp||'it was previously suspended and is now being resumed';
    elsif(v_task_spec.state=sched.gc_state_EXCLUDED)then
      v_exp:=v_exp||'another peer task (same group and operation id) is currently running';
    elsif(v_task_spec.state=sched.gc_state_BROKEN)then
      v_exp:=v_exp||'DBMS_JOB could not compile the task when it was launched, or because DBMS_JOB failed on 16 occasions to execute it';
    elsif(v_task_spec.state=sched.gc_state_ERROR)then
      if(nvl(v_task_spec.return_code,0)<>0)then
        v_exp:=v_exp||'it returned a non-zero exit code of '||v_task_spec.return_code||' on its last run';
      else
        v_exp:=v_exp||'it has an erroneous task specification or DBMS_JOB could not compile the task command';
      end if;
    elsif(v_task_spec.state=sched.gc_state_TIMEDOUT)then
      v_exp:=v_exp||'or one of the tasks that this task is dependent on, has timed out';
    elsif(v_task_spec.state=sched.gc_state_DISABLED)then
      if(b_one_off_task)then    
        v_exp:=v_exp||'it was scheduled to execute only once ever and it has done so';
      else      
        if(v_task_spec.next_due_date is null)then
          v_exp:=v_exp||'it possibly is a newly-created task';
        else
          v_exp:=v_exp||'it was probably manually disabled';
        end if;
      end if;
    elsif(v_task_spec.state=sched.gc_state_UNDEFINED)then
      v_exp:=v_exp||'a serious error occurred in the scheduler''s Finite State Machine';
    elsif(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
      v_exp:=v_exp||'only one user at a time is allowed to modify a task';
    end if;
    v_exp:=v_exp||'. ';  

    -- Describe remedy for being in thie state
    v_exp:=v_exp||'The suggested course of action for dealing with this task is to ';
    if(v_task_spec.state=sched.gc_state_ABORTING)then
      v_exp:=v_exp||'wait for this operation to complete';    
    elsif(v_task_spec.state=sched.gc_state_SUSPENDED)then
      v_exp:=v_exp||'resume the task if you want to continue running it. It will return to its original state before it was suspended';
    elsif(v_task_spec.state=sched.gc_state_RESUMED)then
      v_exp:=v_exp||'let the task return to the state that it was in before it was suspended';
    elsif(v_task_spec.state=sched.gc_state_EXCLUDED)then
      v_exp:=v_exp||'allow the peer task that prevented this task from running to complete';
    elsif(v_task_spec.state=sched.gc_state_BROKEN)then
      v_exp:=v_exp||'manually remove the job from DBMS_JOB and find out what caused the problem by running the scheduler in debug mode and analysing the script that was submitted to DBMS_JOB';
    elsif(v_task_spec.state=sched.gc_state_ERROR)then
      v_exp:=v_exp||'look at the error log and investigate the events surrounding the error. ';
      if(v_task_spec.task_type=sched.gc_type_PERSISTENT and nvl(v_task_spec.repeats,utl.pkg_math.gc_max_int)>v_task_spec.repeat_count)then
        v_exp:=v_exp||'The scheduler will retry the task in '
                    ||ceil((sysdate-v_task_spec.state_tmstmp)*utl.pkg_date.gc_mins_per_hour-v_task_spec.repeat_interval)
                    ||' minute(s) from now. You can force an immediate retry by resetting the task';
      else      
        v_exp:=v_exp||'The scheduler will not retry this task. You can force a retry by resetting the task';
      end if;
    elsif(v_task_spec.state=sched.gc_state_TIMEDOUT)then
      v_exp:=v_exp||'reset the task if you really wish to run it';
    elsif(v_task_spec.state=sched.gc_state_DISABLED)then
      if(b_one_off_task)then
        v_exp:=v_exp||'remove this task from the schedule, unless you want to keep it for later re-use';
      else
        v_exp:=v_exp||'enable the task when you are ready for it become part of the schedule';
      end if;
    elsif(v_task_spec.state=sched.gc_state_UNDEFINED)then
      v_exp:=v_exp||'contact support';
    elsif(v_task_spec.state=sched.gc_state_EDIT_LOCK)then
      v_exp:=v_exp||'wait until the editing operation has completed';
    end if;
    v_exp:=v_exp||'.'||chr(10);  
  else
    v_exp:=v_exp||'.'||chr(10);  
  end if;        
  
  -- Describe frequency
  v_exp:=v_exp||'Under normal circumstances, the task runs ';
  if(v_task_spec.special_days=sched.gc_special_day_INCLUDE)then
    v_exp:=v_exp||recurring_pattern_desc||'and additionally on all '||sched.gc_special_day_name;
  elsif(v_task_spec.special_days=sched.gc_special_day_EXCLUDE)then
    v_exp:=v_exp||recurring_pattern_desc||', excluding all '||sched.gc_special_day_name;
  elsif(v_task_spec.special_days=sched.gc_special_day_ONLY)then
    v_exp:=v_exp||recurring_pattern_desc||', but only on '||sched.gc_special_day_name;
  elsif(v_task_spec.special_days=sched.gc_special_day_AFTER 
    or  v_task_spec.special_days=sched.gc_special_day_BEFORE) 
    then
    if(v_task_spec.day is not null)then
      v_exp:=v_exp||v_task_spec.day||' day(s) '||v_task_spec.special_days||' every '||sched.gc_special_day_name||', at '||v_task_spec.hour||':'||v_task_spec.minute;
    else
      v_exp:=v_exp||v_task_spec.hour||' hour(s) and '||v_task_spec.minute||' minutes '||v_task_spec.special_days||' the onset of each '||sched.gc_special_day_name ; 
    end if;
  else
    v_exp:=v_exp||recurring_pattern_desc;
  end if;
  v_exp:=v_exp||'. The task ';
  -- Describe the task action
  if(v_task_spec.command_type=sched.gc_command_type_SQL)then
    v_exp:=v_exp||'executes a piece of SQL';
  elsif(v_task_spec.command_type=sched.gc_command_type_FUNCTION)then
    v_exp:=v_exp||'executes a PL/SQL function';
  elsif(v_task_spec.command_type=sched.gc_command_type_PROCEDURE)then
    v_exp:=v_exp||'executes a PL/SQL procedure';
  elsif(v_task_spec.command_type=sched.gc_command_type_EXTERNAL_PROC)then
    v_exp:=v_exp||'executes a UNIX shell command';
  else
    v_exp:=v_exp||'is a dummy task and does not actually do anything';
  end if;
  v_exp:=v_exp||'. '||chr(10);
  
  -- Get modal status
  if(v_task_spec.modal='Y')then
    v_exp:=v_exp||'The task is also modal. This means that no other task can execute while this task is executing, nor can this task be lauched when any other tasks are executing, be they modal or non-modal.'||chr(10);
  end if;
 
  -- Mutually exclusive set of tasks
  select count(*)
    into v_count
    from schedules
   where group_name = v_task_spec.group_name
     and operation_id = v_task_spec.operation_id;
  if(v_count>1)then
    v_exp:=v_exp||'This tasks belongs to a set of mutually exclusive peer tasks of which only one of the ['||v_count||'] tasks may execute in a given execution cycle';
    begin
      select task_id
        into v_other_task_id
        from (select task_id
                from schedules
               where group_name = v_task_spec.group_name
                 and operation_id = v_task_spec.operation_id
                 and task_id <> v_task_spec.task_id
                 and state = sched.gc_state_DONE
               union
              select task_id
                from schedules
               where group_name = v_task_spec.group_name
                 and operation_id = v_task_spec.operation_id
                 and task_id <> v_task_spec.task_id
                 and state = sched.gc_state_executing
              );         
      v_exp:=v_exp||' and will not execute in this cycle, as Task Id ['||v_other_task_id||'] has already taken presidence';
    exception
      when no_data_found then
        null;
      when too_many_rows then
        v_exp:=v_exp||', although for an unknown reason, multiple mutually-exclusive peer tasks are currently executing';
      when others then
        v_exp:=v_exp||sqlerrm;
    end;
    v_exp:=v_exp||'.'||chr(10);
  end if;
  
  -- Peer tasks
  v_peers:=get_peer_tasks;
  if(v_peers is not null)then
    v_exp:=v_exp||'The following tasks are peer tasks to this tasks and will not simultaneously'||
                  ' execute when this tasks is executing (and conversely): ';
    v_exp:=v_exp||v_peers||'. ';    
    -- Look for other peer task currently executing  
    begin
      select s.task_id
        into v_other_task_id
        from schedules s
       where s.task_id in (       
               select distinct task_id
                 from (
                        select tp1.task_peer1 task_id
                          from task_peers tp1
                         where tp1.task_peer2 = p_task_id
                         union
                        select tp2.task_peer2 task_id
                          from task_peers tp2
                         where tp2.task_peer1 = p_task_id
                      )
             )
         and s.state=sched.gc_state_EXECUTING;
      v_exp:=v_exp||'Currently, peer Task Id ['||v_other_task_id||'] is executing. '; 
    exception
      when no_data_found then
        null;
      when too_many_rows then
        v_exp:=v_exp||'For an unknown reason, multiple peer tasks are currently executing. ';
      when others then 
        null;
    end;
  end if;

  -- Task in peered group
  v_peers:=get_peer_groups;
  if(v_peers is not null)then
    v_exp:=v_exp||'The group that the task is in is peered with the following tasks groups: ';
    v_exp:=v_exp||v_peers||'. If any tasks in one of the peered groups is executing, then neither'||
                           ' this task nor any other tasks in its groups will be allowed to execute'||
                           ' (and conversely). ';
  end if;
  
  -- Max run-time
  if(v_task_spec.max_runtime is not null)then
    v_exp:=v_exp||'The execution time of the task may not exceed '||v_task_spec.max_runtime||' minutes. ';
    if(v_task_spec.state=sched.gc_state_EXECUTING)then
      v_exp:=v_exp||'It has already been running for '||(sysdate-v_task_spec.state_tmstmp)*1440||' minutes.';
    end if;
    v_exp:=v_exp||chr(10);
  end if;

  -- Repeats
  if(v_task_spec.task_type=sched.gc_type_PERSISTENT)then
    -- PERSISTENT task
    v_exp:=v_exp||'The task is persistently executed until it succeeds';
    if(v_task_spec.repeats is not null)then 
      v_exp:=v_exp||' and will attempt this a maximum of '||v_task_spec.repeats||' times, once every ';
      if(nvl(v_task_spec.repeat_interval,1)>1)then
        v_exp:=v_exp||v_task_spec.repeat_interval||' minutes ';
      else
        v_exp:=v_exp||'minute ';
      end if;
      v_exp:=v_exp||'before completely giving up. ';
    else
      v_exp:=v_exp||' and will continue attempting this ad infinitum.';
    end if;  
  else
    -- REPEATING Task (Non-Persistent)
    if(v_task_spec.repeats is not null and v_task_spec.repeat_interval is not null)then    
      v_exp:=v_exp||'The task is repeatedly executed '||to_char(v_task_spec.repeats)||
                    ' times, once every '||to_char(v_task_spec.repeat_interval)||' minutes. ';
      if(v_task_spec.state=sched.gc_state_EXECUTING)then
        v_exp:=v_exp||'Currently, it is executing for the '||utl.pkg_math.cardinal2ordinal(v_task_spec.repeat_count-1)||' time. ';
      elsif(v_task_spec.state=sched.gc_state_ERROR)then
        v_exp:=v_exp||'It executed '||to_char(v_task_spec.repeat_count-1)||' times before it an error occurred. ';
      else
        v_exp:=v_exp||'It has executed '||to_char(v_task_spec.repeat_count-1)||' times. ';
      end if;
    end if;    
  end if;
  v_exp:=v_exp||chr(10);

  -- Give some history of the task
  if(v_task_spec.finished_at is not null)then
    if(v_task_spec.repeats is not null and v_task_spec.repeat_interval is not null)then    
      v_exp:=v_exp||'The last time that the task executed its '||to_char(v_task_spec.repeats)
                  ||' repeats was on '||to_char(v_task_spec.finished_at,gc_datetime_format)||'. ';
    else
      v_exp:=v_exp||'The last time that the task executed was on '
                  ||to_char(v_task_spec.finished_at,gc_datetime_format)||'. ';
    end if;
  else
    if(v_task_spec.state<>sched.gc_state_EXECUTING)then
      v_exp:=v_exp||'This task has not executed yet. ';
    end if;
  end if;
  
  -- Get tasks that this task is dependent on executing
  v_exp:=v_exp||'Note that ';
  if(v_task_spec.dependencies is null)then
    v_exp:=v_exp||'it is not dependent on the completion of any other tasks';
  else
    v_exp:=v_exp||'it is dependent on the completion of the following operations: '||chr(10);
    v_exp:=v_exp||'  '||v_task_spec.dependencies||'. '||chr(10);
    if(v_task_spec.max_waittime is not null)then
      v_exp:=v_exp||'If this dependency cannot be satisfied within '||v_task_spec.max_waittime
                  ||' minutes of this task becoming due, the task will not be executed. '||chr(10);
    end if;
  end if;

  -- Get tasks that have a dependency on this task
  -- TODO: Need to show parent tasks too           
  declare
    v_list  dbms_sql.varchar2s;
    v_index pls_integer;
  begin
    -- Get dependency tree, even if there are circular dependecies - although they won't be shown.
    select '|'||lpad(level,5)||' |'||lpad(task_id,6)||' | '||lpad(' ',level*4-4,' ')||'['||group_name||':'||operation_id||']'
      bulk collect
      into v_list
      from schedules
     start with (task_id=p_task_id)
   connect by trim(substr(dependencies, 
                          instr(dependencies, prior(group_name||':'||operation_id) )-1,
                          length( prior(group_name||':'||operation_id) )+1)
                   ) = trim( prior(group_name||':'||operation_id) )
           -- Note: In 10g, use  connect by nocycle ... and drop this bit:   
           -- This prevents failures due to circular references              
       and trim(substr(dependencies, 
                       instr(dependencies, group_name||':'||operation_id)-1,
                       length(group_name||':'||operation_id)+1)
               ) <> trim(group_name||':'||operation_id)                
     order by level;                 
    if(v_list.count<=1)then
      v_exp:=v_exp||' and no other tasks have a dependency on this task.'||chr(10);
      v_exp:=v_exp||' +-----------------------------------------------------------------------------+';
    else
      if(v_task_spec.dependencies is null)then
        v_exp:=v_exp||', however, the';
      else
        v_exp:=v_exp||'The';
      end if;
      v_exp:=v_exp||' following tasks are dependent on the completion of this task:
+------+-------+--------------------------------------------------------------+
|Level |TaskId | Child Relations                                              |
+------+-------+--------------------------------------------------------------+'||chr(10);     
      v_index:=v_list.first;
      while(v_index is not null)loop
        v_exp:=v_exp||v_list(v_index)||chr(10);
        v_index:=v_list.next(v_index);
      end loop;
      v_exp:=v_exp||'
+------+-------+--------------------------------------------------------------+
';
    end if;

  exception
    when others then
      v_exp:=v_exp||'** WARNING ** There are circular references in this task''s dependendents, and need to be resolved. ';
  end;
  v_exp:=v_exp||chr(10);

  return v_exp;
exception
  when others then
    utl.pkg_logger.log;
    return sqlerrm;
end task_explanation;

-------------------------------------------------------------------------------
-- Task checker
--
-- All the tasks can manually be checked and a report generated. The following
-- aspects of tasks are checked for:
--  * Invalid tasks
--  * Orphaned tasks
--  * Circular references
-- sched.check_all_tasks
-- Checking all tasks...
-- Task [1][DEFAULT:1]
-- OK
-- Task [2][DEFAULT:2]
-- OK
-- Task [3][DEFAULT:3]
-- There circular references in this task's chain of dependencies.
-- Task [4][DEFAULT:4]
-- OK
-- Checked 4 tasks.
-- 1 failure
-- 3 successes
procedure check_all_tasks
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.check_all_tasks';
begin
  null; --TODO
end check_all_tasks;


-- Shows tasks content
function task_show(p_task_id in schedules.task_id%type) 
return varchar2
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_show';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code;
begin
  v_task_spec.task_id:=p_task_id;
  v_retcode:=sched.get_task_details(v_task_spec);
  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    return task_show(v_task_spec);
  else
    raise_application_error(v_retcode,null);
  end if;
end task_show;

function task_show(p_task_spec in sched.t_schedule_rec) 
return varchar2
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_show';
  v_dump varchar2(32000);
  l_tree dbms_sql.varchar2s;
begin
  v_dump:=
'+---------------------+-------------------------------------------------------+
| Task '||nvl(to_char(p_task_spec.task_id),'NULL')||' dump:
+---------------------+-------------------------------------------------------+
|Parameter            | Value                                                 |
+---------------------+-------------------------------------------------------+
|Task Id:              '||nvl(to_char(p_task_spec.task_id),'NULL')||'
|  Settings:
|    task_type:        '||nvl(p_task_spec.task_type,'NULL')||'
|    task_priority     '||nvl(p_task_spec.task_priority,'0')||'
|    modal:            '||nvl(p_task_spec.modal,'N')||'
|    group_name:       '||nvl(p_task_spec.group_name,'NULL')||'
|    group_priority:   '||nvl(p_task_spec.group_priority,'0')||'
|    operation_id:     '||nvl(to_char(p_task_spec.operation_id),'NULL')||'
|    command:          '||nvl(replace(p_task_spec.command,'''',''''''),'NULL')||'
|    command_type:     '||nvl(p_task_spec.command_type,'NULL')||'
|    description:      '||nvl(p_task_spec.description,'NULL')||'
|    dependencies:     '||nvl(p_task_spec.dependencies,'NULL')||'
|    max_waittime:     '||nvl(to_char(p_task_spec.max_waittime),'NULL')||'
|    max_runtime:      '||nvl(to_char(p_task_spec.max_runtime),'NULL')||'
|    ignore_error:     '||nvl(to_char(p_task_spec.ignore_error),'NULL')||'
|    submitted_by:     '||nvl(to_char(p_task_spec.submitted_by),'NULL')||'
|  Most recent execution:
|    queue_id:         '||nvl(to_char(p_task_spec.queue_id),'NULL')||'
|    process_id:       '||nvl(to_char(p_task_spec.process_id),'NULL')||'
|    return_code:      '||nvl(to_char(p_task_spec.return_code),'NULL')||'
|    state:            '||nvl(p_task_spec.state,'NULL')||'
|    state_tmstmp:     '||nvl(to_char(p_task_spec.state_tmstmp,'YYYYMMDD HH24:MI:SS'),'NULL')||'
|    started_at:       '||nvl(to_char(p_task_spec.started_at,'YYYYMMDD HH24:MI:SS'),'NULL')||'
|    finished_at:      '||nvl(to_char(p_task_spec.finished_at,'YYYYMMDD HH24:MI:SS'),'NULL')||'
|    next_due_date:    '||nvl(to_char(p_task_spec.next_due_date,'YYYYMMDD HH24:MI:SS'),'NULL')||'
|  Recurring pattern:
|    year:             '||nvl(to_char(p_task_spec.year),'NULL')||'
|    month:            '||nvl(to_char(p_task_spec.month),'NULL')||'
|    day:              '||nvl(to_char(p_task_spec.day),'NULL')||'
|    hour:             '||nvl(to_char(p_task_spec.hour),'NULL')||'
|    minute:           '||nvl(to_char(p_task_spec.minute),'NULL')||'
|    weekdays:         '||nvl(p_task_spec.weekdays,'NULL')||'
|    special_days:     '||nvl(p_task_spec.special_days,'NULL')||'
|  Repeats:
|    repeats:          '||nvl(to_char(p_task_spec.repeats),'NULL')||'
|    repeat_interval:  '||nvl(to_char(p_task_spec.repeat_interval),'NULL')||'
|    repeat_periodic:  '||nvl(p_task_spec.repeat_periodic,'Y')||'
|    repeat_count:     '||nvl(to_char(p_task_spec.repeat_count),'NULL')||'
|  Dynamic Variables:
|    effective_date_offset: '||nvl(to_char(p_task_spec.effective_date_offset),'NULL')||'
+-----------------------------------------------------------------------------+';

  -- TODO: Need to show parent tasks too

  -- Get dependency tree, even if there are circular dependecies - although they won't be shown.
  select '|'||lpad(level,5)||' |'||lpad(task_id,6)||' | '||lpad(' ',level*4,' ')||'['||group_name||':'||operation_id||']'
    bulk collect
    into l_tree
    from schedules
   start with (task_id=p_task_spec.task_id)
 connect by trim(substr(dependencies, 
                        instr(dependencies, prior(group_name||':'||operation_id) )-1,
                        length( prior(group_name||':'||operation_id) )+1)
                 ) = trim( prior(group_name||':'||operation_id) )
         -- Note: In 10g, use  connect by nocycle ... and drop this bit:                 
     and trim(substr(dependencies, 
                     instr(dependencies, group_name||':'||operation_id)-1,
                     length(group_name||':'||operation_id)+1)
             ) <> trim(group_name||':'||operation_id)                
   order by level;             
  if(l_tree.count>1)then      
    v_dump:=v_dump||chr(10)||'| '||l_tree.count||' tasks are dependent on this task:
+------+-------+--------------------------------------------------------------+
|Level |TaskId | Child Relationships                                          |
+------+-------+--------------------------------------------------------------+'||chr(10);
    for i in l_tree.first..l_tree.last loop
      v_dump:=v_dump||l_tree(i)||chr(10);
    end loop;
    v_dump:=v_dump||
'+-----------------------------------------------------------------------------+'||chr(10);    
  end if;

  -- Look for peer tasks
  l_tree.delete;
  
  select '|'||lpad(s.task_id,7,' ')||' '||s.group_name||':'||s.operation_id
    bulk collect
    into l_tree
    from schedules s
   where s.task_id in (
           select distinct task_id
             from (
                   select tp1.task_peer1 task_id
                     from task_peers tp1
                    where tp1.task_peer2 = p_task_spec.task_id
                    union
                   select tp2.task_peer2 task_id
                     from task_peers tp2
                    where tp2.task_peer1 = p_task_spec.task_id
                  )
         );
         /*
   union          
  -- Tasks that are referred to in the peer table but do not exist
  select distinct '|'||lpad(task_id,7,' ')||' (this task does not exist)'
    from (
          select tp1.task_peer1 task_id
            from task_peers tp1
           where tp1.task_peer2 = p_task_spec.task_id
           union
          select tp2.task_peer2 task_id
            from task_peers tp2
           where tp2.task_peer1 = p_task_spec.task_id
         );
     */    
  if(l_tree.count>0)then
    v_dump:=v_dump||'
| Peer tasks that have been peered with this task:                            |
+-----------------------------------------------------------------------------+'||chr(10);
    for i in l_tree.first..l_tree.last loop
      v_dump:=v_dump||l_tree(i)||chr(10);
    end loop;
    v_dump:=v_dump||
'+-----------------------------------------------------------------------------+'||chr(10);    
  end if;
  
  return v_dump;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    null;
end task_show;

-- Dumps tasks content
function task_dump(p_task_id in schedules.task_id%type) 
return varchar2
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_dump';
  v_task_spec         sched.t_schedule_rec;
  v_retcode           utl.global.t_error_code;
begin
  v_task_spec.task_id:=p_task_id;
  v_retcode:=sched.get_task_details(v_task_spec);
  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    return task_dump(v_task_spec);
  else
    raise_application_error(v_retcode,null);
  end if;
end task_dump;

function task_dump(p_task_spec in sched.t_schedule_rec) 
return varchar2
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.task_dump';
  v_dump varchar2(32000);
  l_tree dbms_sql.varchar2s;
begin
v_dump:=
'task_id                '||nvl(to_char(p_task_spec.task_id),'NULL')||'
task_type              '||nvl(p_task_spec.task_type,'NULL')||'
task_priority          '||nvl(p_task_spec.task_priority,'0')||'
modal                  '||nvl(p_task_spec.modal,'N')||'
group_name             '||nvl(p_task_spec.group_name,'NULL')||'
group_priority         '||nvl(p_task_spec.group_priority,'0')||'
operation_id           '||nvl(to_char(p_task_spec.operation_id),'NULL')||'
command                '||nvl(replace(p_task_spec.command,'''',''''''),'NULL')||'
command_type           '||nvl(p_task_spec.command_type,'NULL')||'
description            '||nvl(p_task_spec.description,'NULL')||'
dependencies           '||nvl(p_task_spec.dependencies,'NULL')||'
max_waittime           '||nvl(to_char(p_task_spec.max_waittime),'NULL')||'
max_runtime            '||nvl(to_char(p_task_spec.max_runtime),'NULL')||'
ignore_error           '||nvl(to_char(p_task_spec.ignore_error),'NULL')||'
submitted_by           '||nvl(to_char(p_task_spec.submitted_by),'NULL')||'
queue_id               '||nvl(to_char(p_task_spec.queue_id),'NULL')||'
process_id             '||nvl(to_char(p_task_spec.process_id),'NULL')||'
return_code            '||nvl(to_char(p_task_spec.return_code),'NULL')||'
state                  '||nvl(p_task_spec.state,'NULL')||'
state_tmstmp           '||nvl(to_char(p_task_spec.state_tmstmp,'YYYYMMDD HH24:MI:SS'),'NULL')||'
started_at             '||nvl(to_char(p_task_spec.started_at,'YYYYMMDD HH24:MI:SS'),'NULL')||'
finished_at            '||nvl(to_char(p_task_spec.finished_at,'YYYYMMDD HH24:MI:SS'),'NULL')||'
next_due_date          '||nvl(to_char(p_task_spec.next_due_date,'YYYYMMDD HH24:MI:SS'),'NULL')||'
year                   '||nvl(to_char(p_task_spec.year),'NULL')||'
month                  '||nvl(to_char(p_task_spec.month),'NULL')||'
day                    '||nvl(to_char(p_task_spec.day),'NULL')||'
hour                   '||nvl(to_char(p_task_spec.hour),'NULL')||'
minute                 '||nvl(to_char(p_task_spec.minute),'NULL')||'
weekdays               '||nvl(p_task_spec.weekdays,'NULL')||'
special_days           '||nvl(p_task_spec.special_days,'NULL')||'
repeats                '||nvl(to_char(p_task_spec.repeats),'NULL')||'
repeat_interval        '||nvl(to_char(p_task_spec.repeat_interval),'NULL')||'
repeat_periodic        '||nvl(p_task_spec.repeat_periodic,'Y')||'
repeat_count           '||nvl(to_char(p_task_spec.repeat_count),'NULL')||'
effective_date_offset  '||nvl(to_char(p_task_spec.effective_date_offset),'NULL');
  return v_dump;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    null;
end task_dump;


-- Dumps all the tasks in the schedule 
function schedule_dump 
return varchar2
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.schedule_dump';
  v_dump varchar2(32000);
  l_tree dbms_sql.varchar2s;
begin
  -- TODO: Need to show parent tasks too           
  /*
  select  lpad(rnum,4)||' '||
          rpad(nvl(substr( scbp2, 1, instr(scbp2,',')-1),' '),7)||' '||      -- Parent Reference
          rpad(task_id,7)||' '||                                             -- Task Id
          rpad(task_level,2)||' '||                                          -- Heirarchical Level                    
          rpad(substr(state,1,8),8,' ')||' '||
          group_name||':'||operation_id                                      -- Task Alias
  */          
  select  rnum||':'||
          nvl(substr(scbp2,1,instr(scbp2,',')-1),'')||':'||          -- Parent Reference
          task_id||':'||                                             -- Task Id
          task_level||':'||                                          -- Heirarchical Level                    
          state||':'||
          group_name||':'||operation_id                              -- Task Alias  
    bulk collect
    into l_tree                     
    from (select a.*,
                 substr( scbp, instr(scbp, ',', -1, 2 )+1 ) scbp2
            from (select rownum rnum, 
                         level task_level, 
                         task_id, 
                         state,
                         dependencies,
                         group_name,
                         operation_id,
                         sys_connect_by_path( task_id, ',' ) scbp  -- dummy string for use by outside query
                    from schedules
                   start with dependencies is null
                 connect by trim(substr(dependencies, 
                                        instr(dependencies, prior(group_name||':'||operation_id) )-1,
                                        length( prior(group_name||':'||operation_id) )+1)
                                 ) = trim( prior(group_name||':'||operation_id) )               
                 ) a
           );
  if(l_tree.count()>0)then                               
    for i in l_tree.first..l_tree.last loop
      v_dump:=v_dump||l_tree(i)||chr(10);
    end loop;
  end if;
  -- Return nothing if table was empty
  return v_dump;  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    null;
end schedule_dump;


-- Shows all the tasks in the schedule 
function schedule_show return varchar2 is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.schedule_show';
  v_dump varchar2(32000);
  l_tree dbms_sql.varchar2s;
  c_step pls_integer:=1;
begin
  -- TODO: Need to show parent tasks too           
  select '|'||lpad(rnum,4)||'|'||                                                 -- Row Number
          lpad(nvl(substr( scbp2, 1, instr(scbp2,',')-1),' '),6)||'|'||           -- Parent Reference
          lpad(task_level,5)||'|'||                                               -- Heirarchical Level
          lpad(task_id,6)||'|'||                                                  -- Task Id
          rpad(substr(state,1,8),8,' ')||'|'||
          lpad(' ',task_level*c_step-c_step,' ')||group_name||':'||operation_id   -- Task Details
    bulk collect
    into l_tree                     
    from (select a.*,
                 substr( scbp, instr(scbp, ',', -1, 2 )+1 ) scbp2
            from (select rownum rnum, 
                         level task_level, 
                         task_id, 
                         state,
                         dependencies,
                         group_name,
                         operation_id,
                         sys_connect_by_path( rownum, ',' ) scbp  -- dummy string for use by outside query
                    from schedules
                   start with dependencies is null
                 connect by trim(substr(dependencies, 
                                        instr(dependencies, prior(group_name||':'||operation_id) )-1,
                                        length( prior(group_name||':'||operation_id) )+1)
                                 ) = trim( prior(group_name||':'||operation_id) )               
                 ) a
           );
  if(l_tree.count()>0)then                    
    v_dump:=
'+-----------------------------------------------------------------------------+
| Scheduled Tasks and Child Relationships:                                    |
+----+------+-----+------+----------------------------------------------------+
|Row |Parent|Level|TaskId|State   |Task Alias                                 |
+----+------+-----+------+--------+-------------------------------------------+
';
    for i in l_tree.first..l_tree.last loop
      v_dump:=v_dump||l_tree(i)||chr(10);
    end loop;
    v_dump:=v_dump||
'+----+------+-----+------+--------+-------------------------------------------+
';
  else
    v_dump:='No matching tasks found.'||chr(10);
  end if;
  return v_dump;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    null;
end schedule_show;

-- Shows all the tasks in the schedule that partially match the
-- given task description search string
function schedule_query(p_search in varchar2) return varchar2
is
  c_proc_name   constant varchar2(100) := pc_schema||'.'||pc_package||'.schedule_query';
  v_dump varchar2(32000);
  l_tree dbms_sql.varchar2s;
  c_step pls_integer:=1;
  v_search schedules.description%type:='%'||utl.pkg_string.clean4query(p_search)||'%';
begin
  select * 
    bulk collect
    into l_tree                       
    from (select '+---+------+---------+---------------------+----------------------------------+'||chr(10)||
                 '|'||lpad(rownum,3)||'|'||                                              -- Row Number
                 lpad(task_id,6)||'|'||                                                  -- Task Id
                 rpad(substr(state,1,9),9,' ')||'|'||                                    -- State
                 case when length(group_name||':'||operation_id)>21 then
                   substr(group_name||':'||operation_id,1,18)||'...'
                 else                                    
                   rpad(group_name||':'||operation_id,21)
                 end||'|'||                           -- Group:OpId         
                 case when length(description)>34 then
                   substr(description,1,31)||'...'
                 else
                   rpad(description,34)                  -- Description         
                 end||'|'||chr(10)||'|'||
                 case when length(command)>77 then
                   substr(command,1,74)||'...'
                 else
                   rpad(substr(nvl(command,' '),1,77),77)                                       -- Command         
                 end ||'|'
            from schedules
           where description like v_search
              or group_name like v_search
              or state like v_search
              or command like v_search
            order by task_id
          )
   where rownum <=1000;

  if(l_tree.count()>0)then         
    v_dump:=
'+-----------------------------------------------------------------------------+
| Tasks matching '||rpad('['||substr(p_search,1,58)||']:',61)||'|
+---+------+---------+---------------------+----------------------------------+
|Row|TaskId|State    |Task Alias           |Description                       |
|Command                                                                      |
';
    for i in l_tree.first..l_tree.last loop
      v_dump:=v_dump||l_tree(i)||chr(10);
    end loop;
    v_dump:=v_dump||
'+---+------+---------+---------------------+----------------------------------+
';
  else
    v_dump:='No matching tasks found.'||chr(10);
  end if;
  return v_dump;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    null;
end schedule_query;



begin
  gc_datetime_format      :=nvl(utl.pkg_config.get_variable_string(gc_config_key_datetimeformat),'YYYY/MM/DD HH24:MI');
end scheduler_rep;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
