create or replace trigger scheduler.tr_ar_schedules
  after insert or update or delete on scheduler.schedules
  referencing new as new old as old
  for each row
------------------------------------------------------------------------------
-- Audit trail for indicated table
------------------------------------------------------------------------------
declare
  gc_object_name constant varchar2(100) := 'schedules';
  gc_owner_name  constant varchar2(10)  := 'sched';
begin 
  if inserting then
    if(:new.task_type<>sched.gc_type_VOLATILE)then
      utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_insert,
        gc_object_name,
        gc_owner_name,
        :new.change_reason,
        null,  -- no old value
        'SubmittedBy:        '||:new.submitted_by||utl.pkg_string.gc_nl||
        'TaskType:           '||:new.task_type||utl.pkg_string.gc_nl||
  --    'TaskPriority:       '||:new.task_priority||utl.pkg_string.gc_nl||
        'GroupName:          '||:new.group_name||utl.pkg_string.gc_nl||
        'OperationId:        '||:new.operation_id||utl.pkg_string.gc_nl||
        'Command:            '||:new.command||utl.pkg_string.gc_nl||
        'CommandType:        '||:new.command_type||utl.pkg_string.gc_nl||
        'Description:        '||:new.description||utl.pkg_string.gc_nl||
        'Dependencies:       '||:new.dependencies||utl.pkg_string.gc_nl||
        'MaxWaittime:        '||:new.max_waittime||utl.pkg_string.gc_nl||
        'MaxRuntime:         '||:new.max_runtime||utl.pkg_string.gc_nl||
        'QueueId:            '||:new.queue_id||utl.pkg_string.gc_nl||
        'ProcessId:          '||:new.process_id||utl.pkg_string.gc_nl||
        'ReturnCode:         '||:new.return_code||utl.pkg_string.gc_nl||
        'State:              '||:new.state||utl.pkg_string.gc_nl||
        'StateTmstmp:        '||:new.state_tmstmp||utl.pkg_string.gc_nl||
        'PrevState:          '||:new.prev_state||utl.pkg_string.gc_nl||
        'StartedAt:          '||:new.started_at||utl.pkg_string.gc_nl||
        'FinishedAt:         '||:new.finished_at||utl.pkg_string.gc_nl||
        'Year:               '||:new.year||utl.pkg_string.gc_nl||
        'Month:              '||:new.month||utl.pkg_string.gc_nl||
        'Day:                '||:new.day||utl.pkg_string.gc_nl||
        'Hour:               '||:new.hour||utl.pkg_string.gc_nl||
        'Minute:             '||:new.minute||utl.pkg_string.gc_nl||
        'Weekdays:           '||:new.weekdays||utl.pkg_string.gc_nl||
        'SpecialDays:        '||:new.special_days||utl.pkg_string.gc_nl||
        'NextDueDate:        '||:new.next_due_date||utl.pkg_string.gc_nl||
        'Repeats:            '||:new.repeats||utl.pkg_string.gc_nl||
        'RepeatInterval:     '||:new.repeat_interval||utl.pkg_string.gc_nl||
        'RepeatCount:        '||:new.repeat_count||utl.pkg_string.gc_nl||
        'RepeatPeriodic:     '||:new.repeat_periodic||utl.pkg_string.gc_nl||
        'EffectiveDateOffset:'||:new.effective_date_offset||utl.pkg_string.gc_nl||
        'Modal:              '||:new.modal||utl.pkg_string.gc_nl||
        'IgnoreError:        '||:new.ignore_error||utl.pkg_string.gc_nl||
        'DependencySQL:      '||:new.dependency_sql||utl.pkg_string.gc_nl||
        'RepeatPeriodic:     '||:new.repeat_periodic||utl.pkg_string.gc_nl
        );
    elsif deleting then
      if(:old.task_type<>sched.gc_type_VOLATILE)then
        utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_delete,
          gc_object_name,
          gc_owner_name,
          :old.change_reason,
          'SubmittedBy:        '||:old.submitted_by||utl.pkg_string.gc_nl||
          'TaskType:           '||:old.task_type||utl.pkg_string.gc_nl||
    --      'TaskPriority:       '||:old.task_priority||utl.pkg_string.gc_nl||
          'GroupName:          '||:old.group_name||utl.pkg_string.gc_nl||
          'OperationId:        '||:old.operation_id||utl.pkg_string.gc_nl||
          'Command:            '||:old.command||utl.pkg_string.gc_nl||
          'CommandType:        '||:old.command_type||utl.pkg_string.gc_nl||
          'Description:        '||:old.description||utl.pkg_string.gc_nl||
          'Dependencies:       '||:old.dependencies||utl.pkg_string.gc_nl||
          'MaxWaittime:        '||:old.max_waittime||utl.pkg_string.gc_nl||
          'MaxRuntime:         '||:old.max_runtime||utl.pkg_string.gc_nl||
          'QueueId:            '||:old.queue_id||utl.pkg_string.gc_nl||
          'ProcessId:          '||:old.process_id||utl.pkg_string.gc_nl||
          'ReturnCode:         '||:old.return_code||utl.pkg_string.gc_nl||
          'State:              '||:old.state||utl.pkg_string.gc_nl||
          'StateTmstmp:        '||:old.state_tmstmp||utl.pkg_string.gc_nl||
          'PrevState:          '||:old.prev_state||utl.pkg_string.gc_nl||
          'StartedAt:          '||:old.started_at||utl.pkg_string.gc_nl||
          'FinishedAt:         '||:old.finished_at||utl.pkg_string.gc_nl||
          'Year:               '||:old.year||utl.pkg_string.gc_nl||
          'Month:              '||:old.month||utl.pkg_string.gc_nl||
          'Day:                '||:old.day||utl.pkg_string.gc_nl||
          'Hour:               '||:old.hour||utl.pkg_string.gc_nl||
          'Minute:             '||:old.minute||utl.pkg_string.gc_nl||
          'Weekdays:           '||:old.weekdays||utl.pkg_string.gc_nl||
          'SpecialDays:        '||:old.special_days||utl.pkg_string.gc_nl||
          'NextDueDate:        '||:old.next_due_date||utl.pkg_string.gc_nl||
          'Repeats:            '||:old.repeats||utl.pkg_string.gc_nl||
          'RepeatInterval:     '||:old.repeat_interval||utl.pkg_string.gc_nl||
          'RepeatCount:        '||:old.repeat_count||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:old.repeat_periodic||utl.pkg_string.gc_nl||
          'EffectiveDateOffset:'||:old.effective_date_offset||utl.pkg_string.gc_nl||
          'Modal:              '||:old.modal||utl.pkg_string.gc_nl||
          'IgnoreError:        '||:old.ignore_error||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:old.repeat_periodic||utl.pkg_string.gc_nl,
          null);  -- new new value
      end if;                
    end if;
  elsif updating then
    if(:new.task_type<>sched.gc_type_VOLATILE)then
      if( :new.submitted_by       <> :old.submitted_by      or
          :new.task_type          <> :old.task_type         or
  --      :new.task_priority      <> :old.task_priority     or
          :new.group_name         <> :old.group_name        or 
          :new.operation_id       <> :old.operation_id      or
          :new.command            <> :old.command           or
          :new.command_type       <> :old.command_type      or
          :new.description        <> :old.description       or
          :new.dependencies       <> :old.dependencies      or
          :new.max_waittime       <> :old.max_waittime      or
          :new.max_runtime        <> :old.max_runtime       or
      --  :new.queue_id           <> :old.queue_id          or Don't want to audit this - we do this somewhere else
      --  :new.process_id         <> :old.process_id        or
      --  :new.return_code        <> :old.return_code       or
      --  :new.state              <> :old.state             or 
      --  :new.state_tmstmp       <> :old.state_tmstmp      or
      --  :new.prev_state         <> :old.prev_state        or
      --  :new.started_at         <> :old.started_at        or
      --  :new.finished_at        <> :old.finished_at       or
          :new.year               <> :old.year              or
          :new.month              <> :old.month             or
          :new.day                <> :old.day               or
          :new.hour               <> :old.hour              or
          :new.minute             <> :old.minute            or
          :new.weekdays           <> :old.weekdays          or
          :new.special_days       <> :old.special_days      or
      --  :new.next_due_date      <> :old.next_due_date     or Don't want to audit this - we do this somewhere else
          :new.repeats            <> :old.repeats           or
          :new.repeat_interval    <> :old.repeat_interval   or
      --   :new.repeat_count       <> :old.repeat_count      or Don't want to audit this - we do this somewhere else
          :new.repeat_periodic    <> :old.repeat_periodic   or
          :new.effective_date_offset <> :old.effective_date_offset or
          :new.modal              <> :old.modal             or
          :new.ignore_error       <> :old.ignore_error      or
       -- :new.dependency_sql     <> :old.dependency_sql    or Don't want to audit this - we do this somewhere else
          :new.repeat_periodic    <> :old.repeat_periodic)
      then
        utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_update,
          gc_object_name,
          gc_owner_name,
          :new.change_reason,   
          'SubmittedBy:        '||:old.submitted_by||utl.pkg_string.gc_nl||
          'TaskType:           '||:old.task_type||utl.pkg_string.gc_nl||
  --      'TaskPriority:       '||:old.task_priority||utl.pkg_string.gc_nl||
          'GroupName:          '||:old.group_name||utl.pkg_string.gc_nl||
          'OperationId:        '||:old.operation_id||utl.pkg_string.gc_nl||
          'Command:            '||:old.command||utl.pkg_string.gc_nl||
          'CommandType:        '||:old.command_type||utl.pkg_string.gc_nl||
          'Description:        '||:old.description||utl.pkg_string.gc_nl||
          'Dependencies:       '||:old.dependencies||utl.pkg_string.gc_nl||
          'MaxWaittime:        '||:old.max_waittime||utl.pkg_string.gc_nl||
          'MaxRuntime:         '||:old.max_runtime||utl.pkg_string.gc_nl||
          'QueueId:            '||:old.queue_id||utl.pkg_string.gc_nl||
          'ProcessId:          '||:old.process_id||utl.pkg_string.gc_nl||
          'ReturnCode:         '||:old.return_code||utl.pkg_string.gc_nl||
          'State:              '||:old.state||utl.pkg_string.gc_nl||
          'StateTmstmp:        '||:old.state_tmstmp||utl.pkg_string.gc_nl||
          'PrevState:          '||:old.prev_state||utl.pkg_string.gc_nl||
          'StartedAt:          '||:old.started_at||utl.pkg_string.gc_nl||
          'FinishedAt:         '||:old.finished_at||utl.pkg_string.gc_nl||
          'Year:               '||:old.year||utl.pkg_string.gc_nl||
          'Month:              '||:old.month||utl.pkg_string.gc_nl||
          'Day:                '||:old.day||utl.pkg_string.gc_nl||
          'Hour:               '||:old.hour||utl.pkg_string.gc_nl||
          'Minute:             '||:old.minute||utl.pkg_string.gc_nl||
          'Weekdays:           '||:old.weekdays||utl.pkg_string.gc_nl||
          'SpecialDays:        '||:old.special_days||utl.pkg_string.gc_nl||
          'NextDueDate:        '||:old.next_due_date||utl.pkg_string.gc_nl||
          'Repeats:            '||:old.repeats||utl.pkg_string.gc_nl||
          'RepeatInterval:     '||:old.repeat_interval||utl.pkg_string.gc_nl||
          'RepeatCount:        '||:old.repeat_count||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:old.repeat_periodic||utl.pkg_string.gc_nl||
          'EffectiveDateOffset:'||:old.effective_date_offset||utl.pkg_string.gc_nl||
          'Modal:              '||:old.modal||utl.pkg_string.gc_nl||
          'IgnoreError:        '||:old.ignore_error||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:old.repeat_periodic||utl.pkg_string.gc_nl,             
          'SubmittedBy:        '||:new.submitted_by||utl.pkg_string.gc_nl||
          'TaskType:           '||:new.task_type||utl.pkg_string.gc_nl||
  --      'TaskPriority:       '||:new.task_priority||utl.pkg_string.gc_nl||
          'GroupName:          '||:new.group_name||utl.pkg_string.gc_nl||
          'OperationId:        '||:new.operation_id||utl.pkg_string.gc_nl||
          'Command:            '||:new.command||utl.pkg_string.gc_nl||
          'CommandType:        '||:new.command_type||utl.pkg_string.gc_nl||
          'Description:        '||:new.description||utl.pkg_string.gc_nl||
          'Dependencies:       '||:new.dependencies||utl.pkg_string.gc_nl||
          'MaxWaittime:        '||:new.max_waittime||utl.pkg_string.gc_nl||
          'MaxRuntime:         '||:new.max_runtime||utl.pkg_string.gc_nl||
          'QueueId:            '||:new.queue_id||utl.pkg_string.gc_nl||
          'ProcessId:          '||:new.process_id||utl.pkg_string.gc_nl||
          'ReturnCode:         '||:new.return_code||utl.pkg_string.gc_nl||
          'State:              '||:new.state||utl.pkg_string.gc_nl||
          'StateTmstmp:        '||:new.state_tmstmp||utl.pkg_string.gc_nl||
          'PrevState:          '||:new.prev_state||utl.pkg_string.gc_nl||
          'StartedAt:          '||:new.started_at||utl.pkg_string.gc_nl||
          'FinishedAt:         '||:new.finished_at||utl.pkg_string.gc_nl||
          'Year:               '||:new.year||utl.pkg_string.gc_nl||
          'Month:              '||:new.month||utl.pkg_string.gc_nl||
          'Day:                '||:new.day||utl.pkg_string.gc_nl||
          'Hour:               '||:new.hour||utl.pkg_string.gc_nl||
          'Minute:             '||:new.minute||utl.pkg_string.gc_nl||
          'Weekdays:           '||:new.weekdays||utl.pkg_string.gc_nl||
          'SpecialDays:        '||:new.special_days||utl.pkg_string.gc_nl||
          'NextDueDate:        '||:new.next_due_date||utl.pkg_string.gc_nl||
          'Repeats:            '||:new.repeats||utl.pkg_string.gc_nl||
          'RepeatInterval:     '||:new.repeat_interval||utl.pkg_string.gc_nl||
          'RepeatCount:        '||:new.repeat_count||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:new.repeat_periodic||utl.pkg_string.gc_nl||
          'EffectiveDateOffset:'||:new.effective_date_offset||utl.pkg_string.gc_nl||
          'Modal:              '||:new.modal||utl.pkg_string.gc_nl||
          'IgnoreError:        '||:new.ignore_error||utl.pkg_string.gc_nl||
          'RepeatPeriodic:     '||:new.repeat_periodic||utl.pkg_string.gc_nl
        );      
      end if;
    end if;
  end if;
exception 
  when others then
    utl.pkg_errorhandler.handle;    
    raise;  
end tr_ar_schedules;
/
