PL/SQL Developer Test script 3.0
28
--  File name:                  $Workfile:  $
--  Source Control version:     $Revision: 1.1 $
--  Last modified by:           $Author: apenney $
--  Source Control location:    $Archive:  $
declare
  v_start   number;
  v_end     number;
  v_retcode integer:=0;
  i         integer;
begin
  dbms_output.put_line('Start ');
  v_start := dbms_utility.get_time;

  -- Test statements
  scheduler.sched.fsm();

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
0
