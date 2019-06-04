create or replace package body scheduler.scheduler_dep
as
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Dependency management functions for the scheduler
--
-- Package Architecture:
--
--  +---------------+       +---------------+        +---------------+
--  | SCHEDULER_FSM +---+---+ SCHEDULER_MOD +---+----+ SCHEDULER_GUI |
--  +---------------+   |   +---------------+   |    +---------------+
--                      |                       |
--                      |   +---------------+   |    +---------------+
--                      +---+ SCHEDULER_REP |   +----+ SCHEDULER_CON |
--                      |   +---------------+        +---------------+
--                      |
--                      |   +---------------+
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
-------------------------------------------------------------------------------
-- DO NOT EVEN THINK OF RUNNING A SO_CALLED CODE-BEAUTIFYER ON THIS CODE!
-------------------------------------------------------------------------------

-- Determines the task hierarchical position in the dependency net
-- PARENT         Has child tasks only
-- PARENTCHILD    Has parent tasks and child tasks
-- CHILD          Has parent tasks but no child tasks
-- INDEPENDENT    No parent or child tasks
function get_hierarchy(p_task_id in schedules.task_id%type) return varchar2
is
  v_child_count pls_integer;
  v_parent_count pls_integer;
  l_nodes sched.t_schedules;
begin
  v_child_count:=get_child_nodes(p_task_id,l_nodes);  
  v_parent_count:=get_parent_nodes(p_task_id,l_nodes);
  if(v_child_count=1 and v_parent_count=1)then
    return gc_independent;
  elsif(v_child_count>1 and v_parent_count=1)then
    return gc_parent;
  elsif(v_child_count=1 and v_parent_count>1)then
    return gc_child;
  else
    return gc_parentchild;
  end if;
end get_hierarchy;

-- For a given node in the tree, get all the child nodes
-- including the current node
-- Returns the number of child nodes or 0 if none found
function get_child_nodes(p_task_id in schedules.task_id%type, p_child_nodes in out sched.t_schedules)
return pls_integer
is
  --c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_child_nodes';
begin
  select *
    bulk collect
    into p_child_nodes
    from vw_fsm_tasks s1
           start with s1.task_id=p_task_id
         connect by trim(substr(s1.dependencies,
                              instr(s1.dependencies, prior(s1.group_name||':'||s1.operation_id) )-1,
                              length( prior(s1.group_name||':'||s1.operation_id) )+1))
                  = trim( prior(s1.group_name||':'||s1.operation_id) )
             and trim(substr(s1.dependencies,
                       instr(s1.dependencies, (s1.group_name||':'||s1.operation_id) )-1,
                       length(s1.group_name||':'||s1.operation_id)+1))
              <> trim(s1.group_name||':'||s1.operation_id);
  return p_child_nodes.count;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return 0;
end get_child_nodes;

-- For a given node in the tree, get all parent nodes
-- including the current node
-- Returns the number of parent nodes or 0 if none found
function get_parent_nodes(p_task_id in schedules.task_id%type, p_parent_nodes in out sched.t_schedules)
return pls_integer
is
  --c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.get_parent_nodes';
begin
  select s2.*
    bulk collect
    into p_parent_nodes
    from vw_fsm_tasks s2
   start with s2.task_id=p_task_id
 connect by trim(substr( prior s2.dependencies,
                        instr( prior s2.dependencies,  (s2.group_name||':'||s2.operation_id) )-1,
                        length( (s2.group_name||':'||s2.operation_id) )+1)
                 ) = trim( (s2.group_name||':'||s2.operation_id) );
  return p_parent_nodes.count;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return 0;
end get_parent_nodes;

-------------------------------------------------------------------------------
-- Makes up the SQL required to check for the completion of parent tasks
-- Examples:
--  The dependency expression G:1 OR G:2 makes the SQL:
--      select count(*)  result
--        from (
--              select count(*) s
--                from schedules
--               where (operation_id = 1 and group_name='G' and state = 'DONE')
--              INTERSECT
--              select count(*) s
--                from schedules
--               where (operation_id = 2 and group_name='G' and state = 'DONE')
--             )
--       where s=1
--
--  The dependency expression G:1 AND G:2 makes the SQL:
--      select count(*) result
--        from (
--              select count(*) s
--                from schedules
--               where (operation_id = 1 and group_name='G' and state = 'DONE')
--              UNION
--              select count(*) s
--                from schedules
--               where (operation_id = 2 and group_name='G' and state = 'DONE')
--             )
--       where s=1
--
--  The dependency expression (G:1 AND G:2) OR G:3 makes the SQL:
--      select count(*) result
--        from (
--              (
--               select count(*) s
--                from (
--                      select count(*) s
--                        from schedules
--                       where (operation_id = 1 and group_name='G' and state = 'DONE')
--                      INTERSECT
--                      select count(*) s
--                        from schedules
--                       where (operation_id = 2 and group_name='G' and state = 'DONE')
--                     )
--               where s=1
--              )
--              UNION
--              select count(*) s
--                from schedules
--               where (operation_id = 3 and group_name='G' and state = 'DONE')
--            )
--       where s=1
-- Or even better:
--      select count(*) result
--        from (
--              select count(*) s
--                from schedules
--               where (operation_id = 1 and group_name='G' and state = 'DONE')
--              INTERSECT
--              select count(*) s
--                from schedules
--               where (operation_id = 2 and group_name='G' and state = 'DONE')
--              UNION
--              select count(*) s
--                from schedules
--               where (operation_id = 3 and group_name='G' and state = 'DONE')
--            )
--       where s=1
--
-- Note the reuse of the same alias. This is not a problem! Oracle knows what it means.
--
-- Parameters:  This task's Dependency expression for the parents
-- Returns:     The complete sql statement for checking for the completion
--              of a logical path. This is indicated when one or more records
--              are returned from the sql.
-- Notes:       This sql can be stored in the schedule table
--              to avoid having to recalculate this every time
--              however it must be updated every time that a change is made to
--              the dependency statement
-- Assumptions: The group names do no contain digits.
-- Future:      Use Regular Expressions of Oracle 10g
function make_dependency_sql(p_dependency in varchar2)
return varchar2
is
  c_proc_name   constant varchar2(100)  := pc_schema||'.'||pc_package||'.make_dependency_sql';
  v_pos         pls_integer :=1;
  v_last_pos    pls_integer :=1;
  v_operation_id pls_integer;
  v_num_len     number;
  v_sql         varchar2(2000);
  v_end_dep     schedules.dependencies%type;  -- end bit of the dep expression
  v_group_name  schedules.group_name%type;
  v_group_name_pos  pls_integer;
  v_group_name_in_term boolean;    -- do not have to default group name for a term
  v_operator    varchar2(100);
  v_dependency  varchar2(1000):=p_dependency;
begin
  if(p_dependency is null)then
    return null;
  end if;
  
  dbms_application_info.set_module(c_proc_name,null);
  
  -- Clean up expression a little further to ease parsing 
  v_dependency:=replace(v_dependency,'(',' ( ');
  v_dependency:=replace(v_dependency,')',' ) ');

  v_sql:=
'select count(*) result
   from (
';

  while(utl.pkg_string.parse_number(v_dependency,v_pos,v_operation_id,v_num_len))loop
    -- Extract group name, if any, from before the operation [GROUP_NAME]:[OPERATION_ID]
    if(substr(v_dependency,v_pos-v_num_len-1,1)=':')then
      v_group_name_in_term:=true;
      -- Found ':' preceeding operation Id
      v_group_name_pos:=instr(substr(v_dependency,1,v_pos-v_num_len-1),' ',-1)+1;
      v_group_name:=trim(substr(v_dependency,v_group_name_pos,v_pos-v_group_name_pos-v_num_len-1));
      if(v_group_name is null)then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Group name not found before the '':''-delimiter in dependecy expression: ['||v_dependency||']');
        raise utl.pkg_exceptions.e_scheduler_task_spec;
      end if;
    else
      -- No group name defined in group:operation pair - use default group name
      v_group_name_in_term:=false;
      v_group_name:=sched.gc_default_group_name;
    end if;

    -- Get Operator and parenthesis
    if(v_group_name_in_term)then
      v_operator:=upper(trim(substr(v_dependency,v_last_pos,v_pos-v_last_pos-length(v_operation_id)-length(v_group_name)-1 )));
    else
      v_operator:=upper(trim(substr(v_dependency,v_last_pos,v_pos-v_last_pos-length(v_operation_id) )));
    end if;

    -- Map operator to grouping set
    if(v_operator is not null)then
      v_operator:=replace(v_operator,'AND','INTERSECT');
      v_operator:=replace(v_operator,'OR','UNION');
      v_sql:=v_sql||v_operator||chr(10); 
    end if;

    -- Create set
    v_sql:=v_sql||
'        select decode(count(*),0,0,1) s
          from schedules
         where (operation_id = '||v_operation_id||'
           and group_name='''||v_group_name||'''
           and state = '''||sched.gc_state_DONE||''')
';
    v_last_pos:=v_pos;
  end loop;
  v_end_dep:=trim(substr(v_dependency,v_last_pos));
  if(v_end_dep is not null)then
    v_sql:=v_sql||'          '||v_end_dep||chr(10); -- add on remaining unparsable depenendency expression
  end if;
  v_sql:=v_sql||
'        )
 where s=1';                -- close everything off
  dbms_application_info.set_module(null,null);
  return v_sql;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end make_dependency_sql;

-- Get first order direct parents and children from this task
-- Ignores current task
procedure get_direct_tasks(
  p_curr_net_tasks in out sched.t_schedules,
  p_task_id in schedules.task_id%type)
is
  l_new_tasks   sched.t_schedules;
  v_last_j      pls_integer;
  v_found       boolean;
begin
  select distinct s.*
    bulk collect 
    into l_new_tasks
    from (
          select s2.*       -- Get parent tasks from this task
            from vw_fsm_tasks s2
           where s2.task_id<>p_task_id                         
           start with s2.task_id=p_task_id             
         connect by trim(substr( prior s2.dependencies,
                                 instr( prior s2.dependencies,  (s2.group_name||':'||s2.operation_id) )-1,
                                 length( (s2.group_name||':'||s2.operation_id) )+1)
                        ) = trim( (s2.group_name||':'||s2.operation_id) )                                
           union
          select s1.*          -- Get child tasks from this task
            from vw_fsm_tasks s1
           where s1.task_id<>p_task_id
           start with s1.task_id=p_task_id           
         connect by trim(substr(s1.dependencies,
                              instr(s1.dependencies, prior(s1.group_name||':'||s1.operation_id) )-1,
                              length( prior(s1.group_name||':'||s1.operation_id) )+1)
                        ) = trim( prior(s1.group_name||':'||s1.operation_id) )
         ) s
   order by s.task_id asc;       
   
  -- Append to exiting list if it does not exit in list
  -- Note that the exiting list is not necessarily sorted
  -- however, the new list is in ascending order
  if(l_new_tasks.count>0)then
    if(p_curr_net_tasks.count=0)then    
      -- No previous items exist - simply bolt it on
      p_curr_net_tasks:=l_new_tasks;
    else
      for i in l_new_tasks.first..l_new_tasks.last loop
        v_found:=false;
        for j in p_curr_net_tasks.first..p_curr_net_tasks.last loop
          if(p_curr_net_tasks(j).task_id=l_new_tasks(i).task_id)then
            v_found:=true;
            exit;
          end if;
        end loop;
        -- Not found: add to *end* of p_curr_net_tasks list    
        if(not v_found)then
          p_curr_net_tasks(p_curr_net_tasks.last+1):=l_new_tasks(i);
        end if;
      end loop;
    end if;
  end if;
      
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;         
end get_direct_tasks;

-- Get all the tasks in the dependency net
-- Recursive function
-- Returns when no new tasks have been added
-- Important:
-- Only returns a list containing something when there is a 
-- dependency net, i.e. When the task is not a part of a net, 
-- then it returns an empty list.
procedure get_net_tasks(
  p_curr_net_tasks in out sched.t_schedules,  -- Current accumulated set of tasks
  p_prev_net_tasks in out sched.t_schedules,  -- Set of accumulated at previous recursion  
  p_task_id in schedules.task_id%type,        -- Task Id to start the next exploration from 
  p_curr_pos in pls_integer:=1)               -- current position in p_curr_net_tasks to iterate from 
is
  v_prev_count  pls_integer;
  v_curr_count  pls_integer:=0;
  v_last_pos    pls_integer:=0;
begin
  -- 1. Get the next hierarchy of tasks directly related to the current task
  --    adding to the current list of tasks in the tree
  get_direct_tasks(p_curr_net_tasks,p_task_id);
  
  -- 2. Quick sanity check - there are no parent and child tasks related to 
  --    this task
  if(p_curr_net_tasks.count=0)then    
    return; 
  end if;
  
  -- 3. Stop recursing this branch when no new tasks have been added,
  v_curr_count:=p_curr_net_tasks.count;
  v_prev_count:=p_prev_net_tasks.count;  
  if(v_prev_count=v_curr_count)then    
    return;   -- No change - exit from recursion branch
  end if;
  
  -- 4. Copy current state into previous state
  p_prev_net_tasks:=p_curr_net_tasks;  

  -- 5. For each directly-related task, get their directly related tasks
  --    and accumulate it to the total task net
  v_last_pos:=p_curr_net_tasks.last;
  for i in p_curr_pos..v_last_pos loop   
    get_net_tasks(p_curr_net_tasks,p_prev_net_tasks,p_curr_net_tasks(i).task_id,i);
  end loop;
  
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;         
end get_net_tasks;

-------------------------------------------------------------------------------
-- Checks if all tasks in the tree have completed.
-- Returns all tasks in tree including the task that the tree is generated around.
-- We do not 100% know if a task is in a dependency tree, so we call this for
-- every task in when it has reached the done state.
--
-- Cannot allow any tasks in a dependency tree to move from the DONE state until
-- all the tasks in the tree have reached this state.
-- Note:      Tasks that have been disabled are not considered here.
--
-- Returns:   TRUE    Dependents are completed, or there were no dependents
--            FALSE   Dependents are not yet done.
--
-- Exception: This function will return FALSE.
--            Errored tasks will cause the dependency chain to break.
--            This is so by design.
function is_dependency_tree_done(p_task_spec in sched.t_schedule_rec, p_tasks_in_tree out sched.t_schedules)
return boolean
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.is_dependency_tree_done';
  v_retcode           boolean:=true;        -- All dependent tasks have been done, or there were none
  n_task_spec         sched.t_schedule_rec; -- task tree node
  i_task_spec         binary_integer;       -- task tree iterator
  v_pos               pls_integer;          -- list iterator
  l_dummy             sched.t_schedules;    -- used for preversing state in recursive call
begin
  dbms_application_info.set_module(c_proc_name,null);

  -- Get entire dependency net that contains this task 
  -- TODO: This net can be persisted to so that this process does 
  --       not need to be repeated all the time.
  --       The place to do this is is in the scheduler_mod package
  --       when a task is modified or updated, similarly to when the 
  --       update process of dependency SQL.
  get_net_tasks(p_tasks_in_tree,l_dummy,p_task_spec.task_id);
  
  if(p_tasks_in_tree.count=0)then
    if( p_task_spec.state<>sched.gc_state_DONE
    and p_task_spec.state<>sched.gc_state_DISABLED)then
      v_retcode:=false;
    else
      -- Add this task to the list since FSM expects this task to appear in the list
      p_tasks_in_tree(1):=p_task_spec;
    end if;
  else
  
    -- Check for uncompleted tasks in tree
    -- ignoring those in the disabled state
    i_task_spec:=p_tasks_in_tree.first;
    while(i_task_spec is not null)loop
      n_task_spec:=p_tasks_in_tree(i_task_spec);
      if( n_task_spec.state<>sched.gc_state_DONE
      and n_task_spec.state<>sched.gc_state_DISABLED)   -- we ignore disabled tasks in the dependency net
      then
        -- Found a task in the net that could not be ignored 
        -- and has not completed
        v_retcode:=false;
        exit;
      end if;
      i_task_spec:=p_tasks_in_tree.next(i_task_spec);
    end loop;
  end if;  
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end is_dependency_tree_done;

------------------------------------------------------------------------------
-- Determines what the dependancies for this job are, and whether they
-- have successfully been completed. If the job is due, but the dependencies
-- have not been met, then the job will not be executed.
-- Returns:   TRUE    Dependency condition has been met
--            FALSE   Dependency condition has not been met
-- On Error, this function will return FALSE.
--
-- Returns TRUE if the dependancies for this job are completed
-- Returns FALSE if not, or other error
function is_dependencies_satisfied(p_task_spec in out sched.t_schedule_rec)
return boolean
is
  pragma autonomous_transaction;
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.is_dependencies_satisfied';
  v_retcode           boolean:=false;
  v_count             pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(p_task_spec.dependencies is not null)then    
    -- Dynamic SQL to check which dependent jobs are not complete
    if(p_task_spec.dependency_sql is null)then
      -- Generate dependency SQL if required
      p_task_spec.dependency_sql:=make_dependency_sql(p_task_spec.dependencies);
      -- Update genrated SQL
      update schedules s
         set s.dependency_sql=p_task_spec.dependency_sql
       where s.task_id = p_task_spec.task_id;
      commit;
    end if;

    -- Apply dependency SQL
    begin
      execute immediate p_task_spec.dependency_sql into v_count;
      if(v_count>0)then
        v_retcode:=true;
      end if;
    exception
      when others then
        utl.pkg_errorhandler.handle;
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Offending SQL:'||chr(10)||p_task_spec.dependency_sql);
    end;
  else
    -- This task has no dependencies
    v_retcode:=true;
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    -- RULE: We do not raise exceptions into the FSM - we only return failure codes.
    return false;
end is_dependencies_satisfied;


-- Check if any mutually-excluded tasks are running
function is_mutual_task_running(p_task_spec in out sched.t_schedule_rec) return boolean
is
  c_proc_name           constant varchar2(100)  := pc_schema||'.'||pc_package||'.is_mutual_task_running';
  v_count pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);
  select count(*)
    into v_count
    from schedules s
   where s.group_name   =  p_task_spec.group_name    -- Mutually exclusive because
     and s.operation_id =  p_task_spec.operation_id  -- of same OpId and GroupName
     and s.task_id      <> p_task_spec.task_id
     and s.state in (sched.gc_state_READY, sched.gc_state_EXECUTING);
  dbms_application_info.set_module(null,null);
  if(v_count>0)then
    return TRUE;
  else
    return FALSE;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return false;
end is_mutual_task_running;


-- Check if any peered tasks are running
function is_peer_task_running(p_task_spec in out sched.t_schedule_rec) return boolean
is
  c_proc_name           constant varchar2(100)  := pc_schema||'.'||pc_package||'.is_peer_task_running';
  v_count pls_integer;
begin
  select count(*)
    into v_count
    from schedules s    
   where s.task_id in (
           select tp1.task_peer1
             from task_peers tp1
            where tp1.task_peer2 = p_task_spec.task_id
            union 
           select tp2.task_peer2
             from task_peers tp2
            where tp2.task_peer1 = p_task_spec.task_id
         ) 
     and s.state in (
           sched.gc_state_READY, 
           sched.gc_state_EXECUTING,
           sched.gc_state_DONE
         );
  if(v_count>0)then
    return TRUE;
  else
    return FALSE;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return false;
end is_peer_task_running;

-- Check if this task in a peered group that is already running
function is_peer_group_running(p_task_spec in out sched.t_schedule_rec) return boolean
is
  c_proc_name           constant varchar2(100)  := pc_schema||'.'||pc_package||'.is_peer_group_running';
  v_count pls_integer;
begin
  select count(*)
    into v_count
    from schedules s
   where s.group_name in (
           select gp1.group_peer1
             from task_group_peers gp1
            where gp1.group_peer2 = p_task_spec.group_name
            union
           select gp2.group_peer2
             from task_group_peers gp2
            where gp2.group_peer1 = p_task_spec.group_name
         )
     and s.task_id <> p_task_spec.task_id
     and s.state in (
           sched.gc_state_READY, 
           sched.gc_state_EXECUTING,
           sched.gc_state_DONE
         );
  if(v_count>0)then
    return TRUE;
  else
    return FALSE;
  end if;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return false;
end is_peer_group_running;

-- Validates the task's dependencies
function validate_dependencies(p_task_spec in out sched.t_schedule_rec) return UTL.global.t_error_code
is
  pragma autonomous_transaction;
  c_proc_name   constant varchar2(100)  := pc_schema||'.'||pc_package||'.validate_dependencies';
  v_retcode     utl.global.t_error_code := utl.pkg_exceptions.gc_success;
  v_tmp_task_id scheduler.schedules.task_id%type;
begin
  dbms_application_info.set_module(c_proc_name,null);
  -- Check for circular references
  -- Some smoke-and-mirrors:
  -- Need to insert into table before this test can be performed
  -- Get next task Id into a *temp variable*
  select sq_schedule_id.nextval
    into v_tmp_task_id
    from dual;
  insert into schedules(
         task_id,
         task_type,
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
         effective_date_offset)
   values (
         v_tmp_task_id,
         p_task_spec.task_type,
         p_task_spec.group_name,
         p_task_spec.operation_id,
         p_task_spec.dependencies,
         p_task_spec.max_waittime,
         p_task_spec.command,
         p_task_spec.command_type,
         p_task_spec.max_runtime,
         p_task_spec.process_id,
         p_task_spec.state,
         sysdate,
         p_task_spec.prev_state,
         p_task_spec.started_at,
         p_task_spec.finished_at,
         p_task_spec.year,
         p_task_spec.month,
         p_task_spec.day,
         p_task_spec.hour,
         p_task_spec.minute,
         p_task_spec.weekdays,
         p_task_spec.special_days,
         p_task_spec.next_due_date,
         p_task_spec.repeats,
         p_task_spec.repeat_interval,
         p_task_spec.repeat_count,
         p_task_spec.description,
         p_task_spec.effective_date_offset);
  -- Attempt to do a simple heirarchycal list
  declare
    v_list  dbms_sql.varchar2s;
  begin
    select replace(lpad('  ',2*level-2)||'['||sys_connect_by_path(group_name||':'||operation_id,'][')||']','[]')
      bulk collect
      into v_list
      from schedules
     start with (group_name=p_task_spec.group_name and operation_id=p_task_spec.operation_id)
   connect by trim(substr(dependencies,
                          instr(dependencies, prior(group_name||':'||operation_id) )-1,
                          length( prior(group_name||':'||operation_id) )+1
                         )
                  ) = trim( prior(group_name||':'||operation_id) )
     order by level;
  exception
    when others then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Circular references in dependencies based off this task. ['||p_task_spec.dependencies||']');
      v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
  end;
  rollback;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    rollback;      -- rollback at all costs!
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end validate_dependencies;


end scheduler_dep;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
