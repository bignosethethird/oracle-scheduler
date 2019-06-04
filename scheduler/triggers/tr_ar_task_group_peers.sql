create or replace trigger scheduler.tr_ar_task_group_peers
  after insert or update or delete on scheduler.task_group_peers
  referencing new as new old as old
  for each row
------------------------------------------------------------------------------
-- Audit trail for indicated table
------------------------------------------------------------------------------
declare
  gc_object_name constant varchar2(100) := 'task_group_peers';
  gc_owner_name  constant varchar2(10)  := 'sched';
begin 
  if inserting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_insert,
      gc_object_name,
      gc_owner_name,
      :new.change_reason,
      null,  -- no old value
      'GroupPeer1          '||:new.group_peer1||utl.pkg_string.gc_nl||
      'GroupPeer2          '||:new.group_peer2||utl.pkg_string.gc_nl
      );
  elsif deleting then
    utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_delete,
      gc_object_name,
      gc_owner_name,
      :old.change_reason,
      'GroupPeer1          '||:old.group_peer1||utl.pkg_string.gc_nl||
      'GroupPeer1          '||:old.group_peer2||utl.pkg_string.gc_nl,
      null);  -- new new value
  elsif updating then
    if( :new.group_peer1 <> :old.group_peer1  or
        :new.group_peer2 <> :old.group_peer2)
    then
      utl.pkg_audit_trail_mod.insert_entry(utl.pkg_audit_trail.gc_action_update,
        gc_object_name,
        gc_owner_name,
        :new.change_reason,    
        'GroupPeer1          '||:old.group_peer1||utl.pkg_string.gc_nl||
        'GroupPeer2          '||:old.group_peer2||utl.pkg_string.gc_nl,        
        'GroupPeer1          '||:new.group_peer1||utl.pkg_string.gc_nl||        
        'GroupPeer2          '||:new.group_peer2||utl.pkg_string.gc_nl   
      );      
    end if;
  end if;
exception 
  when others then
    utl.pkg_errorhandler.handle;    
    raise;  
end tr_ar_schedules;
/
