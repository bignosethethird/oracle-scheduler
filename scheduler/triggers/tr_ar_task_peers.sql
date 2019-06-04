create or replace trigger scheduler.tr_ar_task_peers
  after insert or update or delete on scheduler.task_peers
  referencing new as new old as old
  for each row
------------------------------------------------------------------------------
-- Audit trail for indicated table
------------------------------------------------------------------------------
declare
  gc_object_name constant varchar2(100) := 'task_peers';
  gc_owner_name  constant varchar2(10)  := 'sched';
begin 
  if inserting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_insert,
      gc_object_name,
      gc_owner_name,
      :new.change_reason,
      null,  -- no old value
      'TaskPeer1           '||:new.task_peer1||utl.pkg_string.gc_nl||
      'TaskPeer2           '||:new.task_peer2||utl.pkg_string.gc_nl
      );
  elsif deleting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_delete,
      gc_object_name,
      gc_owner_name,
      :old.change_reason,
      'TaskPeer1           '||:old.task_peer1||utl.pkg_string.gc_nl||
      'TaskPeer2           '||:old.task_peer2||utl.pkg_string.gc_nl,
      null);  -- new new value
  elsif updating then
    if( :new.task_peer1 <> :old.task_peer1  or
        :new.task_peer2 <> :old.task_peer2)
    then
      utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_update,
        gc_object_name,
        gc_owner_name,
        :new.change_reason,    
        'TaskPeer1           '||:old.task_peer1||utl.pkg_string.gc_nl||
        'TaskPeer2           '||:old.task_peer2||utl.pkg_string.gc_nl,        
        'TaskPeer1           '||:new.task_peer1||utl.pkg_string.gc_nl||
        'TaskPeer2           '||:new.task_peer2||utl.pkg_string.gc_nl   
      );      
    end if;
  end if;
exception 
  when others then
    utl.pkg_errorhandler.handle;    
    raise;  
end tr_ar_schedules;
/
