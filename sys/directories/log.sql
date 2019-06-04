------------------------------------------------------------------------------
-- LOG Directory objects
--
-- NOTE:
-- %..% variables are substituted with the correct values during installation.
-- Refer to the install script for details on these installation variables.
-- Directory objects belong to SYS, regardless of which user creates them.
--
------------------------------------------------------------------------------
set serveroutput on size 1000000
set verify off
set feedback off
set scan off
whenever SQLERROR exit failure
whenever OSERROR exit failure

prompt VCRLOG Directory object for %TARGET_HOME%/log path
host [[ ! -d %TARGET_HOME%/log ]] && mkdir -p %TARGET_HOME%/log
host [[ ! -d %TARGET_HOME%/log ]] && echo "* Could not create directory %TARGET_HOME%/log."
host [[   -d %TARGET_HOME%/log ]] && chmod 775 %TARGET_HOME%/log
create or replace directory LOG as '%TARGET_HOME%/log';
grant read  on directory LOG to utl;
grant write on directory LOG to utl;
grant read  on directory LOG to public;
grant write on directory LOG to public;
grant read  on directory LOG to scheduler;
grant write on directory LOG to scheduler;


------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------
