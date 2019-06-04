PL/SQL Developer Test script 3.0
35
--  File name:                  $Workfile:  $
--  Source Control version:     $Revision: 1.1 $
--  Last modified by:           $Author: apenney $
--  Source Control location:    $Archive:  $
declare
  v_start   number;
  v_end     number;
  v_retcode integer;
  i         integer;
begin
  dbms_output.put_line('Start ');
  v_start := dbms_utility.get_time;

  -- Test statements
  util.scheduler.quiesce();

  -- Test analysis
  if(util.scheduler.get_mode()=util.scheduler.gc_mode_QUIESCING)then
    v_retcode:=0;
  else
    v_retcode:=-1;
  end if;

  -- Test Summary
  if(v_retcode=0) then
    dbms_output.put_line('Success.');
  else
    dbms_output.put_line('Failure. Error Code: '||v_retcode||'.');
  end if;
  v_end := dbms_utility.get_time;
  dbms_output.put_line('Time taken : '||(v_end-v_start)/100||' seconds');
exception
  when others then
    dbms_output.put_line('* Exception: Error Code: '||to_char(sqlcode)||' in $Workfile: $. Message: '||sqlerrm||'.');
end;
0
