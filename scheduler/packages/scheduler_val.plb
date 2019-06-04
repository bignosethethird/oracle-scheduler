create or replace package body scheduler.scheduler_val as
------------------------------------------------------------------------
------------------------------------------------------------------------
-- Task Validation Mechanism
------------------------------------------------------------------------

--===========================================================================--
-- PRIVATE FUNCTIONS
--===========================================================================--


--===========================================================================--
-- PUBLIC FUNCTIONS FUNCTIONS
--===========================================================================--

-- Validate date
function validate_date(p_task_spec in sched.t_schedule_rec)
return UTL.global.t_error_code
is
  c_proc_name         constant varchar2(100)  := pc_schema||'.'||pc_package||'.validate_date';
  v_minute            varchar2(2);
  v_hour              varchar2(2);
  v_day               varchar2(2);
  v_month             varchar2(2);
  v_year              varchar2(4);
  v_weekdays          varchar2(7);
  e_bad_times         exception;
begin
  dbms_application_info.set_module(c_proc_name,null);
  v_minute            :=lpad(p_task_spec.minute,2,'0');
  v_hour              :=lpad(p_task_spec.hour,2,'0');
  v_day               :=lpad(abs(p_task_spec.day),2,'0'); -- DAY can be a negative value
  v_month             :=lpad(p_task_spec.month,2,'0');
  v_year              :=p_task_spec.year;
  v_weekdays          :=p_task_spec.weekdays;

  -- Range check
  if(nvl(to_number(v_year),2002) not between 2000 and 3000)then
    raise e_bad_times;
  end if;
  if(nvl(to_number(v_month),1) not between 1 and 12)then
    raise e_bad_times;
  end if;
  if(nvl(to_number(v_day),1) not between -28 and 31)then
    raise e_bad_times;
  end if;
  if(nvl(to_number(v_hour),0) not between 0 and 23)then
    raise e_bad_times;
  end if;
  if(nvl(to_number(v_minute),0) not between 0 and 59)then
    raise e_bad_times;
  end if;
  if(nvl(to_number(v_minute),0) not between 0 and 59)then
    raise e_bad_times;
  end if;
  if(instr(nvl(v_weekdays,'1'),'8')>0 or instr(nvl(v_weekdays,'1'),'9')>0)then
    raise e_bad_times;
  end if;
  if(p_task_spec.day < -28)then
    raise e_bad_times;
  end if;
  dbms_application_info.set_module(null,null);
  return utl.pkg_exceptions.gc_success;
exception
  when others then
    return utl.pkg_exceptions.gc_scheduler_task_spec;
end validate_date;

-- Prettify by adding periods in where days are missing
-- 2346 ---> .234.6.
function prettify_weekdays(p_weekdays in varchar2)
return varchar2 
is
  s scheduler.schedules.weekdays%type;  
  j pls_integer;
  c varchar2(1);
begin
  if(p_weekdays is null)then
    return null;
  end if;
  j:=1;
  c:=substr(p_weekdays,j,1);
  for i in 1..7 loop
    if(c=to_char(i))then
      s:=s||i;
      j:=j+1;
      c:=substr(p_weekdays,j,1);
    else
      s:=s||'.';
    end if;          
  end loop;  
  return s;
end prettify_weekdays;

------------------------------------------------------------------------------
-- Validate task specification 
-- Corrects default values
-- Logs all the problems with the task
-- Returns gc_success of the task is accepted.
function validate_task(p_task_spec in out sched.t_schedule_rec) return UTL.global.t_error_code
is
  c_proc_name     constant varchar2(100)  := pc_schema||'.'||pc_package||'.validate_task';
  v_retcode       utl.global.t_error_code := utl.pkg_exceptions.gc_success;
  v_task_spec     sched.t_schedule_rec:=p_task_spec;
  v_len           pls_integer;
begin
  dbms_application_info.set_module(c_proc_name,null);

  -- Basic sanity checking
  if(p_task_spec.year<2000 or p_task_spec.year>3000)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Nonsense year value supplied: '||p_task_spec.year||'. Overriding year to null');
    p_task_spec.year:=null;
  end if;
  if(to_number(p_task_spec.month)<1 or to_number(p_task_spec.month)>12)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Nonsense month value supplied: '||p_task_spec.month||'. Overriding month to null');
    p_task_spec.month:=null;
  end if;
  if(to_number(p_task_spec.day)<-28 or to_number(p_task_spec.day)>31 or to_number(p_task_spec.day)=0)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Nonsense day value supplied: '||p_task_spec.day||'. Overriding day to null');
    p_task_spec.day:=null;
  end if;
  if(to_number(p_task_spec.hour)<-23 or to_number(p_task_spec.hour)>23)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Nonsense hour value supplied: '||p_task_spec.hour||'. Overriding hour to 00');
    p_task_spec.hour:='00';
  end if;
  if(to_number(p_task_spec.minute)<-59 or to_number(p_task_spec.minute)>59)then
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Nonsense minute value supplied: '||p_task_spec.minute||'. Overriding minute to 00');
    p_task_spec.minute:='00';
  end if;

  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    -- Remove higher-order 0's
    if(p_task_spec.year is null)then
      if(to_number(p_task_spec.month)=0)then
        p_task_spec.month:=null;
      elsif(p_task_spec.month is null)then
        if(to_number(p_task_spec.day)=0)then
          p_task_spec.day:=null;
        /*
        elsif(p_task_spec.day is null)then
          if(to_number(p_task_spec.hour)=0)then
            p_task_spec.hour:=null;
          elsif(p_task_spec.hour is null)then
            if(to_number(p_task_spec.minute)=0)then
              p_task_spec.minute:=null;
            end if;
          end if;                
        */
        end if;        
      end if;
    end if;
    
    -- Clean up parameters and pad out --------- WHY?
    --p_task_spec.minute    :=lpad(p_task_spec.minute,2,'0');
    --p_task_spec.hour      :=lpad(p_task_spec.hour,  2,'0');
    --p_task_spec.day       :=lpad(p_task_spec.day,   2,'0');
    --p_task_spec.month     :=lpad(p_task_spec.month, 2,'0');
    
    -- Make defaults for missing lower date granularities
    if(p_task_spec.year is not null)then
      p_task_spec.month   :=nvl(p_task_spec.month, '01');
      p_task_spec.day     :=nvl(p_task_spec.day,   '01');
      p_task_spec.hour    :=nvl(p_task_spec.hour,  '00');
      p_task_spec.minute  :=nvl(p_task_spec.minute,'00');
    elsif(p_task_spec.month is not null)then
      if(p_task_spec.day is null)then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'The month '||p_task_spec.month||' but no day was specified. Setting day to 01');
        p_task_spec.day:='01';
      end if;
      p_task_spec.hour    :=nvl(p_task_spec.hour,  '00');
      p_task_spec.minute  :=nvl(p_task_spec.minute,'00');
    elsif(p_task_spec.day is not null)then
      p_task_spec.hour    :=nvl(p_task_spec.hour, '00');
      p_task_spec.minute  :=nvl(p_task_spec.minute,'00');
    elsif(p_task_spec.hour is not null)then
      p_task_spec.minute  :=nvl(p_task_spec.minute,'00');
    end if;    

    -- Days of week: Make Day 0 Day 7, drop larger numbers, dedupe and put pretty dots in in the gaps
    p_task_spec.weekdays:=utl.pkg_string.clean4numbers(p_task_spec.weekdays);
    p_task_spec.weekdays:=translate(p_task_spec.weekdays,'0123456789.,+-','71234567');
    p_task_spec.weekdays:=utl.pkg_string.dedupe_string(p_task_spec.weekdays); -- dedupe and order ascending    
    p_task_spec.weekdays:=prettify_weekdays(p_task_spec.weekdays);  -- Prettify by adding periods in where days are missing
  end if;
  
  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    -- Default OR Clean up GROUP_NAME - only alphabetical chars allowed for easy dependency parsing
    if(p_task_spec.group_name is null)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Group name defaults to '||sched.gc_default_group_name);
      p_task_spec.group_name:=sched.gc_default_group_name;
    else
      v_task_spec.group_name := utl.pkg_string.clean4alpha(p_task_spec.group_name);
      if(length(trim(v_task_spec.group_name))<>length(p_task_spec.group_name))then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Group name ['||trim(v_task_spec.group_name)||'] is cleaned up and is now ['||p_task_spec.group_name||']');
        p_task_spec.group_name:=v_task_spec.group_name;
      end if;
    end if;
  
    -- Set default values
    if( p_task_spec.task_type is null)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Task Type defaults to '||sched.gc_type_DURABLE);
      p_task_spec.task_type  := sched.gc_type_DURABLE;
    end if;    
    p_task_spec.state      := nvl(p_task_spec.state,sched.gc_state_INITIAL);
  
    -- Pick next Operation ID if not defined. 
    if(p_task_spec.operation_id is null)then
      select round(nvl(max(s.operation_id),0),-1)+10
        into p_task_spec.operation_id
        from schedules s
       where s.group_name = p_task_spec.group_name;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_info,'Operation Id ['||p_task_spec.operation_id||'] is allocated to this task');
    end if;
  end if;
  
  -- Pattern Checking
  -- Check date validity:
  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    if(p_task_spec.repeats is not null and p_task_spec.repeats < 1)then
      -- Check repeat validity
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Invalid number of task repeats specified: ['||p_task_spec.repeats||']');
      v_retcode:= utl.pkg_exceptions.gc_scheduler_task_spec;
    elsif(p_task_spec.repeat_interval is not null and nvl(p_task_spec.repeat_interval,0) < 1)then
      -- Check repeat interval
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Invalid repeat interval specified: ['||p_task_spec.repeat_interval||']');
      v_retcode:= utl.pkg_exceptions.gc_scheduler_task_spec;
      -- Related exclusions
      if(p_task_spec.max_runtime is not null)then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Cannot specify a maximum run-time value when this is a repeating task');
        v_retcode:= utl.pkg_exceptions.gc_scheduler_task_spec;
      end if;
    end if;
  end if;
  
  -- Syntax Error checking in DEPENDENCIES
  -- Rules: No funny chars
  --        No digits in group names - this really screws things up!
  if(v_retcode=utl.pkg_exceptions.gc_success)then    
    p_task_spec.dependencies:=trim(p_task_spec.dependencies);
    if(p_task_spec.dependencies is not null)then
      -- Look for invalid chars
      -- TODO: Use Oracle 10g++ RegEx parser
      if(instr(p_task_spec.dependencies,',')>0 or    
         instr(p_task_spec.dependencies,'.')>0 or
         instr(p_task_spec.dependencies,'\')>0 or
         instr(p_task_spec.dependencies,chr(38))>0 or
         instr(p_task_spec.dependencies,chr(47))>0 or
         instr(p_task_spec.dependencies,'<')>0 or
         instr(p_task_spec.dependencies,'>')>0)
      then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Invalid Characters in dependecy logic: ['||p_task_spec.dependencies||']');
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
      else
        -- Tidy up and spread for easier parsing
        -- TODO: Use Oracle 10g++ RegEx parser
        p_task_spec.dependencies:=replace(p_task_spec.dependencies,': ',':');
        p_task_spec.dependencies:=replace(p_task_spec.dependencies,' :',':');
        p_task_spec.dependencies:=replace(p_task_spec.dependencies,'(',' ( ');
        p_task_spec.dependencies:=replace(p_task_spec.dependencies,')',' ) ');
        v_len:=0;        
        while(v_len<>length(p_task_spec.dependencies))loop
          v_len:=length(p_task_spec.dependencies);
          p_task_spec.dependencies:=replace(p_task_spec.dependencies,'  ',' ');                
        end loop;
        -- Count parenthesis
        if(utl.pkg_string.substr_count(p_task_spec.dependencies,'(')<>utl.pkg_string.substr_count(p_task_spec.dependencies,')'))then
          utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Mismatch in parenthesis in dependecy logic: ['||p_task_spec.dependencies||']');
          v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
        else
          -- Group Name
          declare
            v_pos           pls_integer:=1;
            v_operation_id  schedules.operation_id%type;
            v_num_len       pls_integer;
            v_group_name    schedules.group_name%type;
            v_group_name_pos  pls_integer;
          begin
            while(utl.pkg_string.parse_number(p_task_spec.dependencies,v_pos,v_operation_id,v_num_len))loop
              -- Extract group name, if any, from before the operation [GROUP_NAME]:[OPERATION_ID]
              if(substr(p_task_spec.dependencies,v_pos-v_num_len-1,1)=':')then
                -- Found ':' preceeding operation Id
                v_group_name_pos:=instr(substr(p_task_spec.dependencies,1,v_pos-v_num_len-1),' ',-1)+1;
                v_group_name:=trim(substr(p_task_spec.dependencies,v_group_name_pos,v_pos-v_group_name_pos-v_num_len-1));
                if(v_group_name is null)then
                  utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Group name not found before the '':''-delimiter in dependecy expression: ['||p_task_spec.dependencies||']');
                  v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
                  raise utl.pkg_exceptions.e_scheduler_task_spec;
                end if;
              else
                -- Group name not defined - add insert default group name
                p_task_spec.dependencies:=substr(p_task_spec.dependencies,1,v_pos-v_num_len-1)||
                                          sched.gc_default_group_name||':'||
                                          substr(p_task_spec.dependencies,v_pos-v_num_len);
                v_pos:=v_pos+length(sched.gc_default_group_name)+1;
              end if;
            end loop;
          exception
            when others then
              utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'Could not parse group names from dependency logic: ['||p_task_spec.dependencies||']');
              v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end;
        end if;
      end if;
    end if;
  end if;

  -- Special Days
  if(v_retcode=utl.pkg_exceptions.gc_success)then
    if(p_task_spec.special_days is not null)then
      if(p_task_spec.special_days not in (sched.gc_special_day_INCLUDE,
                                          sched.gc_special_day_EXCLUDE,
                                          sched.gc_special_day_ONLY, 
                                          sched.gc_special_day_AFTER,   
                                          sched.gc_special_day_BEFORE))
      then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,'An invalid special-day operator "'||p_task_spec.special_days||'" was specified.');
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;        
      else        
        if(p_task_spec.special_days=sched.gc_special_day_ONLY)then
          if(p_task_spec.year     is not null 
          or p_task_spec.month    is not null 
          or p_task_spec.day      is not null 
          or p_task_spec.weekdays is not null)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
              'An exact date cannot be specified when the the task is supposed to ONLY execute on a '||sched.gc_special_day_name);
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;
        elsif(p_task_spec.special_days=sched.gc_special_day_BEFORE)then
          if( p_task_spec.day      is null
          and p_task_spec.hour     is null
          and p_task_spec.minute   is null)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
              'A task can only be specified to execute a number of days BEFORE each '||sched.gc_special_day_name||
              ' if only the DAY parameter, and optionally the HOUR and MINUTE parameters are specified.');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;          
          if(p_task_spec.day<1
          or p_task_spec.hour<0
          or p_task_spec.minute<0)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error, 
              'Can''t specify negative values in the pattern');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;                    
          if(abs(p_task_spec.day) > 28)then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error, 
              'A task cannot be specified to execute more that 28 days BEFORE a '||sched.gc_special_day_name||'.');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;          
        elsif(p_task_spec.special_days=sched.gc_special_day_AFTER)then
          if( p_task_spec.day      is null
          and p_task_spec.hour     is null
          and p_task_spec.minute   is null)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
              'A task can only be specified to execute a number of days AFTER each '||sched.gc_special_day_name||
              ' if only the DAY parameter, and optionally the HOUR and MINUTE parameters are specified.');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;
          if(p_task_spec.day<1
          or p_task_spec.hour<0
          or p_task_spec.minute<0)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error, 
              'Can''t specify negative values in the pattern');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;                    
          if(abs(p_task_spec.day) > 28)then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error, 
              'A task cannot be specified to execute more that 28 days AFTER a '||sched.gc_special_day_name||'.');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;
        elsif(p_task_spec.special_days=sched.gc_special_day_INCLUDE)then
          if( p_task_spec.day      is null 
          and p_task_spec.weekdays is null)
          then
            utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
              'A task that should in addition to its recurrance pattern execute on a '||sched.gc_special_day_name||
              ' and should have at least the DAY or WEEKDAYS value specified.');
            v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
          end if;
        end if;        
      end if;
    end if;
  end if;      

  -- Task type checking:
  if(v_retcode=utl.pkg_exceptions.gc_success)then
    if(p_task_spec.task_type=sched.gc_type_TIMECRITICAL and p_task_spec.dependencies is not null)then
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
        'Can''t specify depencies in a task that is of type '||sched.gc_type_TIMECRITICAL);
      v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
    end if;
    if(p_task_spec.task_type=sched.gc_type_PERSISTENT)then
      if(p_task_spec.repeat_interval is null)then
        utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
          'A '||sched.gc_type_PERSISTENT||' task need to have a retry interval specified');
        v_retcode:=utl.pkg_exceptions.gc_scheduler_task_spec;
      end if;
    end if;
  end if;
  
  -- Command content checking
  -- Commands passed from the console have ampersants represented as chr(38)'s. 
  -- Make them real ampersants
  if(v_retcode=utl.pkg_exceptions.gc_success)then  
    p_task_spec.command:=replace(p_task_spec.command,'chr(38)',chr(38));
  end if;

  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    raise;
end validate_task;

end scheduler_val;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
