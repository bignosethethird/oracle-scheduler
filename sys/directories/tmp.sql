------------------------------------------------------------------------------
-- TEMPORARY WORKING directory objects
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

prompt VCRTMP Directory object for %TARGET_HOME%/tmp path
host [[ ! -d %TARGET_HOME%/tmp ]] && mkdir -p %TARGET_HOME%/tmp
host [[ ! -d %TARGET_HOME%/tmp ]] && echo "* Could not create directory %TARGET_HOME%/tmp."
host [[   -d %TARGET_HOME%/tmp ]] && chmod 775 %TARGET_HOME%/tmp
create or replace directory TMP as '%TARGET_HOME%/tmp';
grant read  on directory TMP to utl;
grant write on directory TMP to utl;
grant read  on directory TMP to public;
grant write on directory TMP to public;

------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------
