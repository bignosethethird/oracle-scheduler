------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Data population script for table utl.schedules.
-- WARNING: *** This script overwrites the entire table! ***
--          *** Save important content before running.   ***
-- To run this script from the command line:
--   $ sqlplus "scheduler/[password]@[instance]" @schedules.sql
-- This file was generated from database instance ABC.
--   Database Time    : 28FEB2018 17:04:52
--   IP address       : 192.5.20.64
--   Database Language: AMERICAN_AMERICA.WE8ISO8859P1
--   Client Machine   : ahl64
--   O/S user         : vcr
------------------------------------------------------------------------------
set feedback off;
prompt Populating 42 records into table scheduler.schedules.

-- Truncate the table:
delete from scheduler.schedules;

------------------------------------------------------------------------------
-- Populating the table:
------------------------------------------------------------------------------
------------------------------------------------------------------------------
-- Daily Housekeeping
------------------------------------------------------------------------------
begin
  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  MODAL )
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  10,
  'app.pkg_housekeeping.maintain_date_partitions(to_date(''<EffectiveDate>'',''YYYYMMDD''),''source_position'')',
  'PROCEDURE',
  'add and drop partitions from source_position',
  '21',
  '00',
  '12345',
  -1,
  'Y');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  20,
  'utl.pkg_logger.purge',
  'PROCEDURE',
  'purge old log messages',
  '21',
  '00',
  '12345');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  IGNORE_ERROR)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  30,
  '/usr/bin/gzip -f $APP_HOME/archive/*.dat',
  'SHELL',
  'compress archived data files',
  '21',
  '00',
  '12345',
  'Y');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  40,
  'app.pkg_housekeeping.gather_table_stats',
  'PROCEDURE',
  'gather statistics on non-partitioned tables in vcr schema',
  '21',
  '00',
  '12345');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  50,
  'utl.pkg_system.analyze_schemas(''UTL'')',
  'PROCEDURE',
  'gather statistics on tables in utl schema',
  'DAILYHOUSEKEEPING:20',
  '21',
  '00',
  '12345');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYHOUSEKEEPING',
  60,
  'utl.pkg_system.analyze_schemas(''SCHEDULER'')',
  'PROCEDURE',
  'gather statistics on tables in scheduler schema',
  '21',
  '00',
  '12345');
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin
  ------------------------------------------------------------------------------
  -- BOB Tplus1
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'BOBTPLUSONE',
  10,
  '$APP_HOME/bin/Feed2Oracle --notify -s bob -b tplus1 -a <EffectiveDate>',
  'SHELL',
  'Stage BOB Tplus1 file',
  null,
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'BOBTPLUSONE',
  20,
  'app.pkg_loader.load(''BOB'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load BOB Tplus1 data from staging area',
  'BOBTPLUSONE:10',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'BOBTPLUSONE',
  30,
  'app.pkg_dim_creater.create_dims(''BOB'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for BOB Tplus1',
  'BOBTPLUSONE:20',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'BOBTPLUSONE',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''BOB'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark BOB Tplus1 data as complete',
  'BOBTPLUSONE:30',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin
  ------------------------------------------------------------------------------
  -- BOB Tclean
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'BOBTCLEAN',
  10,
  '$APP_HOME/bin/Feed2Oracle -s bob -b tclean -a <EffectiveDate>',
  'SHELL',
  'Stage BOB Tclean file',
  '12',
  '00',
  '12345',
  -18,
  27,
  1440);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'BOBTCLEAN',
  20,
  'app.pkg_loader.load(''BOB'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load BOB Tclean data from staging area',
  'BOBTCLEAN:10',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'BOBTCLEAN',
  30,
  'app.pkg_dim_creater.create_dims(''BOB'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for BOB Tclean',
  'BOBTCLEAN:20',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'BOBTCLEAN',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''BOB'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark BOB Tclean data as complete',
  'BOBTCLEAN:30',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin
  ------------------------------------------------------------------------------
  -- Daily tplus1 status update
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'DAILYTPLUSONECHK',
  10,
  'app.pkg_source_load_run.send_status_update(''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Send daily tplus1 status update',
  '10',
  '00',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin
  ------------------------------------------------------------------------------
  -- Monthly BOB Tclean completion check
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DAY,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'MONTHLYBOBTCLEANCHK',
  10,
  'app.pkg_source_load_run.check_period_complete(''BOB'',''TCLEAN'',utl.pkg_date.first_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)),utl.pkg_date.last_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)))',
  'PROCEDURE',
  'Monthly check to determine whether BOB tclean is complete',
  '-2',
  '18',
  '00',
  '12345',
  0);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- Monthly SSC Tclean completion check
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DAY,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'MONTHLYSSCTCLEANCHK',
  10,
  'app.pkg_source_load_run.check_period_complete(''SSC'',''TCLEAN'',utl.pkg_date.first_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)),utl.pkg_date.last_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)))',
  'PROCEDURE',
  'Monthly check to determine whether SSC tclean is complete',
  '-2',
  '18',
  '00',
  '12345',
  0);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- Monthly GFS Tclean completion check
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DAY,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'MONTHLYGFSTCLEANCHK',
  10,
  'app.pkg_source_load_run.check_period_complete(''GFS'',''TCLEAN'',utl.pkg_date.first_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)),utl.pkg_date.last_second_month(add_months(to_date(''<EffectiveDate>'',''YYYYMMDD''),-1)))',
  'PROCEDURE',
  'Monthly check to determine whether GFS tclean is complete',
  '-2',
  '18',
  '00',
  '12345',
  0);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- IFS Tplus1
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'IFSTPLUSONE',
  10,
  '$APP_HOME/bin/Feed2Oracle --notify -s ifs -b tplus1 -a <EffectiveDate>',
  'SHELL',
  'Stage IFS Tplus1 file',
  null,
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'IFSTPLUSONE',
  20,
  'app.pkg_loader.load(''IFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load IFS Tplus1 data from staging area',
  'IFSTPLUSONE:10',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'IFSTPLUSONE',
  30,
  'app.pkg_dim_creater.create_dims(''IFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for IFS Tplus1',
  'IFSTPLUSONE:20',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'IFSTPLUSONE',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''IFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark IFS Tplus1 data as complete',
  'IFSTPLUSONE:30',
  '21',
  '30',
  '12345',
  -1);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- SSC Tplus1
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'SSCTPLUSONE',
  10,
  '$APP_HOME/bin/Feed2Oracle --notify -s ssc -b tplus1 -a <EffectiveDate>',
  'SHELL',
  'Stage SSC Tplus1 file',
  null,
  '00',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'SSCTPLUSONE',
  20,
  'app.pkg_loader.load(''SSC'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load SSC Tplus1 data from staging area',
  'SSCTPLUSONE:10',
  '00',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'SSCTPLUSONE',
  30,
  'app.pkg_dim_creater.create_dims(''SSC'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for SSC Tplus1',
  'SSCTPLUSONE:20',
  '00',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'SSCTPLUSONE',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''SSC'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark SSC Tplus1 data as complete',
  'SSCTPLUSONE:30',
  '00',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- SSC Tclean
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'SSCTCLEAN',
  10,
  '$APP_HOME/bin/Feed2Oracle -s ssc -b tclean -a <EffectiveDate>',
  'SHELL',
  'Stage SSC Tclean file',
  '12',
  '00',
  '12345',
  -18,
  27,
  1440);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'SSCTCLEAN',
  20,
  'app.pkg_loader.load(''SSC'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load SSC Tclean data from staging area',
  'SSCTCLEAN:10',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'SSCTCLEAN',
  30,
  'app.pkg_dim_creater.create_dims(''SSC'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for SSC Tclean',
  'SSCTCLEAN:20',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'SSCTCLEAN',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''SSC'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark SSC Tclean data as complete',
  'SSCTCLEAN:30',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- Tykhe Tplus1
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'TYKTPLUSONE',
  10,
  '$APP_HOME/bin/Feed2Oracle --notify -s tyk -b tplus1 -a <EffectiveDate>',
  'SHELL',
  'Stage Tykhe Tplus1 file',
  null,
  '14',
  '00',
  '12345',
  -3);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'TYKTPLUSONE',
  20,
  'app.pkg_loader.load(''TYK'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load Tykhe Tplus1 data from staging area',
  'TYKTPLUSONE:10',
  '14',
  '00',
  '12345',
  -3);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'TYKTPLUSONE',
  30,
  'app.pkg_dim_creater.create_dims(''TYK'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for Tykhe Tplus1',
  'TYKTPLUSONE:20',
  '14',
  '00',
  '12345',
  -3);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'TYKTPLUSONE',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''TYK'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark Tykhe Tplus1 data as complete',
  'TYKTPLUSONE:30',
  '14',
  '00',
  '12345',
  -3);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  ------------------------------------------------------------------------------
  -- GFS Tplus1
  ------------------------------------------------------------------------------


  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'GFSTPLUSONE',
  10,
  '$APP_HOME/bin/Feed2Oracle --notify -s gfs -b tplus1 -a <EffectiveDate>',
  'SHELL',
  'Stage GFS Tplus1 file',
  null,
  '03',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'GFSTPLUSONE',
  20,
  'app.pkg_loader.load(''GFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load GFS Tplus1 data from staging area',
  'GFSTPLUSONE:10',
  '03',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'GFSTPLUSONE',
  30,
  'app.pkg_dim_creater.create_dims(''GFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for GFS Tplus1',
  'GFSTPLUSONE:20',
  '03',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'DURABLE',
  'GFSTPLUSONE',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''GFS'',''TPLUS1'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark GFS Tplus1 data as complete',
  'GFSTPLUSONE:30',
  '03',
  '30',
  '12345',
  -2);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin
  ------------------------------------------------------------------------------
  -- GFS Tclean
  ------------------------------------------------------------------------------

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'GFSTCLEAN',
  10,
  '$APP_HOME/bin/Feed2Oracle -s gfs -b tclean -a <EffectiveDate>',
  'SHELL',
  'Stage GFS Tclean file',
  '12',
  '00',
  '12345',
  -18,
  27,
  1440);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'GFSTCLEAN',
  20,
  'app.pkg_loader.load(''GFS'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Load GFS Tclean data from staging area',
  'GFSTCLEAN:10',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'GFSTCLEAN',
  30,
  'app.pkg_dim_creater.create_dims(''GFS'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD''))',
  'PROCEDURE',
  'Create dimensions for GFS Tclean',
  'GFSTCLEAN:20',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/

begin

  INSERT INTO SCHEDULER.SCHEDULES
  (
  TASK_ID,
  TASK_TYPE,
  GROUP_NAME,
  OPERATION_ID,
  COMMAND,
  COMMAND_TYPE,
  DESCRIPTION,
  DEPENDENCIES,
  HOUR,
  MINUTE,
  WEEKDAYS,
  EFFECTIVE_DATE_OFFSET,
  REPEATS,
  REPEAT_INTERVAL)
  VALUES (
  scheduler.sq_schedule_id.nextval,
  'PERSISTENT',
  'GFSTCLEAN',
  40,
  'app.pkg_source_load_run_mod.set_complete(vcr.pkg_source_load_run.get_load_run(''GFS'',''TCLEAN'',to_date(''<EffectiveDate>'',''YYYYMMDD'')))',
  'PROCEDURE',
  'Mark GFS Tclean data as complete',
  'GFSTCLEAN:30',
  '12',
  '00',
  '12345',
  -18,
  3,
  60);
exception
  when dup_val_on_index then
    dbms_output.put_line('Value already exists');
end;
/



UPDATE SCHEDULER.SCHEDULES S SET S.STATE = 'DISABLED';

commit;
set feedback on;
------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------

