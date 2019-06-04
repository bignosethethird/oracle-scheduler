create or replace trigger scheduler.tr_ar_task_groups
  after insert or update or delete on scheduler.task_groups
  referencing new as new old as old
  for each row
------------------------------------------------------------------------------
-- Audit trail for indicated table
------------------------------------------------------------------------------
declare
  gc_object_name constant varchar2(100) := 'task_groups';
  gc_owner_name  constant varchar2(10)  := 'sched';
begin 
  if inserting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_insert,
      gc_object_name,
      gc_owner_name,
      :new.change_reason,
      null,  -- no old value
      'GroupName           '||:new.group_name||utl.pkg_string.gc_nl||
      'GroupPriority       '||:new.group_priority||utl.pkg_string.gc_nl
      );
  elsif deleting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_delete,
      gc_object_name,
      gc_owner_name,
      :old.change_reason,
      'GroupName           '||:old.group_name||utl.pkg_string.gc_nl||
      'GroupPriority       '||:old.group_priority||utl.pkg_string.gc_nl,
      null);  -- new new value
  elsif updating then
    if( :new.group_name     <> :old.group_name  or
        :new.group_priority <> :old.group_priority)
    then
      utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_update,
        gc_object_name,
        gc_owner_name,
        :new.change_reason,    
        'GroupName           '||:old.group_name||utl.pkg_string.gc_nl||
        'GroupPriority       '||:old.group_priority||utl.pkg_string.gc_nl,
        'GroupName           '||:new.group_name||utl.pkg_string.gc_nl||
        'GroupPriority       '||:new.group_priority||utl.pkg_string.gc_nl           
      );      
    end if;
  end if;
exception 
  when others then
    utl.pkg_errorhandler.handle;    
    raise;  
end tr_ar_schedules;
/
