create or replace package body scheduler.scheduler_due
as
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Due Date calculations for the scheduler
--
-- Package Architecture:
--
--  +---------------+       +---------------+        +---------------+
--  | SCHEDULER_FSM +---+---+ SCHEDULER_MOD +---+----+ SCHEDULER_GUI |
--  +---------------+   |   +---------------+   |    +---------------+
--                      |                       |
--                      |   +---------------+   |    +---------------+
--                      +---+ SCHEDULER_REP |   +----+ SCHEDULER_CON |
--                      |   +---------------+        +---------------+
--                      |
--                      |   +---------------+
--                      +---+ SCHEDULER_DEP |
--                      |   +---------------+
--                      |
--                      |   +---------------+
--                      +---+ SCHEDULER_DUE |
--                      |   +---------------+
--                      |
--                      |   +---------------+
--                      +---+ SCHEDULER_VAL |
--                          +---------------+
--
-------------------------------------------------------------------------------
-- DO NOT EVEN THINK OF RUNNING A SO_CALLED CODE-BEAUTIFYER ON THIS CODE!
-------------------------------------------------------------------------------

--===========================================================================--
-- PRIVATE FUNCTIONS
--===========================================================================--

-- Calculate the closest date to the reference date for a given granularity.
-- The degree of granularisation is determined by the smallest-size item in 
-- the recurring pattern.
-- The date elements are zero-padded strings.
-- All values need to be zero-padded to 2 or 4 characters.
function granularize_date(p_ref_date in date, 
                          p_year  in varchar2,
                          p_month in varchar2,
                          p_day   in varchar2,
                          p_hour  in varchar2,
                          p_min   in varchar2)
return date
is      
 c_proc_name            constant varchar2(100) := pc_schema||'.'||pc_package||'.granularize_date';
  v_ref_date            date := nvl(p_ref_date,sysdate);                  
  v_granularized_date   date := p_ref_date;
  v_leap_year           boolean := false;
  e_date_not_valid      exception;
  pragma exception_init(e_date_not_valid,-1839);
begin 
  dbms_application_info.set_module(c_proc_name,null);
  -- Catch a leap year: This is the only irregularity that should be anticipated.
  if(p_month='02' and p_day='29')then
    v_leap_year:=true;
  end if;

  -- Round date to granularity specified by by the smallest-size item in 
  -- the recurring pattern.starting at:
  -- Year (need to consider leap year)
  if(p_year is null)then  
    if(p_month is not null)then
      begin
        if(p_day is null or p_hour is null or p_min is null)then
          raise e_date_not_valid;
        end if;
        v_granularized_date:=to_date(to_char(v_ref_date,'YYYY')||p_month||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
      exception 
        when e_date_not_valid then
          if(not v_leap_year)then
            raise;
          end if;        
          -- Find next matching leap year
          begin
            v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
            v_granularized_date:=to_date(to_char(v_ref_date,'YYYY')||p_month||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
          exception 
            when e_date_not_valid then
              begin
                v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                v_granularized_date:=to_date(to_char(v_ref_date,'YYYY')||p_month||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
              exception 
                when e_date_not_valid then
                  v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                  v_granularized_date:=to_date(to_char(v_ref_date,'YYYY')||p_month||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
              end;
          end;
      end;      
    end if;
    -- Month (need to consider leap year)
    if(p_month is null)then      
      if(p_day is not null)then
        begin
          if(p_hour is null or p_min is null)then
            raise e_date_not_valid;
          end if;
          v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMM')||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
        exception 
          when e_date_not_valid then
            if(not v_leap_year)then
              raise;
            end if;
            -- Find next matching leap year
            begin
              v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
              v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMM')||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
            exception 
              when e_date_not_valid then
                begin
                  v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                  v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMM')||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
                exception 
                  when e_date_not_valid then
                    v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                    v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMM')||p_day||p_hour||p_min,'YYYYMMDDHH24MI');
                end;
            end;
        end;              
      end if;      
      --  Day
      if(p_day is null)then
        if(p_hour is not null)then
          begin
            if(p_min is null)then
              raise e_date_not_valid;
            end if;
            v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDD')||p_hour||p_min,'YYYYMMDDHH24MI');
          exception 
            when e_date_not_valid then
              if(not v_leap_year)then
                raise;
              end if;
              -- Find next matching leap year
              begin
                v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDD')||p_hour||p_min,'YYYYMMDDHH24MI');
              exception 
                when e_date_not_valid then
                  begin
                    v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                    v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDD')||p_hour||p_min,'YYYYMMDDHH24MI');
                  exception 
                    when e_date_not_valid then
                      v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                      v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDD')||p_hour||p_min,'YYYYMMDDHH24MI');
                  end;
              end;
          end;              
        end if;
        -- Hour
        if(p_hour is null)then      
          if(p_min is not null)then
            begin
              v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24')||p_min,'YYYYMMDDHH24MI');
            exception 
              when e_date_not_valid then
                if(not v_leap_year)then
                  raise;
                end if;
                -- Find next matching leap year
                begin
                  v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                  v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24')||p_min,'YYYYMMDDHH24MI');
                exception 
                  when e_date_not_valid then
                    begin
                      v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                      v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24')||p_min,'YYYYMMDDHH24MI');
                    exception 
                      when e_date_not_valid then
                        v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                        v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24')||p_min,'YYYYMMDDHH24MI');
                    end;
                end;
            end;              
          end if;
          -- Minute
          if(p_min is null)then
            v_granularized_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24MI'),'YYYYMMDDHH24MI');
          end if;
        end if;
      end if;
    end if;
  end if;            
  
  return v_granularized_date;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    return null;
end granularize_date;

--===========================================================================--
-- PUBLIC FUNCTIONS
--===========================================================================--


-- Calculate and return the next date that the task is due using repeat pattern
-- based on the given reference date
--
-- Note:
--   If no date parameters are specified, then it will increment to the next minute.
--   unless the task has depencies, which case the task should execute as soon as the 
--   dependencies are satisfied. In this case, this function return NULL 
--   so that the NEXT_DUE_DATE should be set to NULL.
--
-- Returns:  next due date if it could be calculated with success code
--           null if not possible to calculate next date (e.g. one-off tasks)
--           no date has been specified and thetask has dependencies with success code
function calc_next_due_date(p_task_spec in sched.t_schedule_rec, 
                            p_ref_date in date, 
                            p_due_date out date) return UTL.global.t_error_code
is
  c_proc_name         constant varchar2(100) := pc_schema||'.'||pc_package||'.calc_next_due_date';
  v_ref_date          date:=p_ref_date;
  v_proposed_date     date;
  v_minutes_overdue   number;
  v_minute            varchar2(3);
  v_hour              varchar2(3);
  v_day               varchar2(3);
  v_month             varchar2(2);
  v_year              varchar2(4);
  v_weekdays          varchar2(7);
  v_loop_count        pls_integer:=0;
  v_days_in_unit      number;
  v_task_hierarchy    varchar2(20);   -- notional position in the dependency net
  v_retcode           utl.global.t_error_code:=utl.pkg_exceptions.gc_success;
  
  -- Count number of days back from end of the month referred to in the reference date
  -- if the day is a negative value.
  -- If an invalid day results due to this backwards count, the v_day remains untouched
  procedure days_back_from_end_of_month is
    v_num_day pls_integer:=p_task_spec.day;
    v_last_day_of_month date;
  begin
    if(nvl(v_num_day,0)<0)then
      -- Negative day - count days from end of month referred to in reference date
      if(p_task_spec.month is not null)then
        -- Count days back from specified month  
        if(p_task_spec.year is not null)then
          v_last_day_of_month:=last_day(to_date(v_year||v_month||'01','YYYYMMDD'));        
        else
          v_last_day_of_month:=last_day(to_date(to_char(v_ref_date,'YYYY')||v_month||'01','YYYYMMDD'));        
        end if;
      else
        -- Count days back from scheduler ref date
        if(p_task_spec.year is not null)then
          v_last_day_of_month:=last_day(to_date(v_year||v_month||'01','YYYYMMDD'));        
        else
          v_last_day_of_month:=last_day(v_ref_date);        
        end if;
      end if;
      v_day:= to_char(v_last_day_of_month+v_num_day,'DD');
    end if;
  exception
    when others then
      utl.pkg_errorhandler.handle;
      utl.pkg_logger.log(utl.pkg_logger.gc_log_message_error,
                     'Backward counting of days resulted in going into a previous month, and a valid date could not be constructed',
                     'Task Id: '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_inv_date);
      raise;
  end days_back_from_end_of_month;
  
  -- Count number of hours back from end of the day referred to in the reference date
  -- if the hour is a negative value.
  procedure hours_back_from_end_of_day is
    v_num_hour pls_integer:=p_task_spec.hour;
  begin
    if(abs(v_num_hour)>=24)then
      v_num_hour:=0;
    end if;
    if(nvl(v_num_hour,0)<0)then
      v_hour:=lpad(24+v_num_hour,2,'0');
    end if;
  end hours_back_from_end_of_day;  

  -- Count number of hours back from end of the day referred to in the reference date
  -- if the hour is a negative value.
  procedure minutes_back_from_end_of_hour is
    v_num_min pls_integer:=p_task_spec.minute;
  begin
    if(abs(v_num_min)>=60)then
      v_num_min:=0;
    end if;
    if(nvl(v_num_min,0)<0)then
      v_minute:=lpad(60+v_num_min,2,'0');
    end if;
  end minutes_back_from_end_of_hour;  
  
  -- Process any special days need to be included or excluded for the 
  -- reference date and temporarely override the repeat pattern 
  -- so that calc_next_due_date cause the desired effect...
  function special_days_pre_process(p_ref_date in date)
  return utl.global.t_error_code 
  is    
    v_special_day       date;
    v_override_day      date;
  begin
    if(p_task_spec.special_days is null)then
      -- No special day processing to do
      return utl.pkg_exceptions.gc_success;
    end if;
    
    if(p_task_spec.special_days = sched.gc_special_day_ONLY) then
      -- ONLY considers all days, including when they are successive.
      -- If the next special day following ref. day exists, force next due date 
      -- for this day regardless of pattern (which there should be only hours and minutes)
      select min(sd.day)
        into v_special_day
        from special_days sd
       where to_char(sd.day,'YYYYMMDD')>=to_char(p_ref_date,'YYYYMMDD');
      if(v_special_day is null)then
        return utl.pkg_exceptions.gc_scheduler_special_day;                           
      end if;       
      if(v_special_day is not null)then
        v_day  :=to_char(v_special_day,'DD');
        v_month:=to_char(v_special_day,'MM');
        v_year :=to_char(v_special_day,'YYYY');
      else
        -- No next special day is found - need to ensure that this task is not called every day
        -- since it may have hours and minutes set
        return utl.pkg_exceptions.gc_scheduler_special_day;        
      end if;
    elsif(p_task_spec.special_days = sched.gc_special_day_BEFORE)then
      -- Need count backwards the DAY, HOUR and MINUTE values
      -- The first of a set of successive days is considered.    
      -- Get the best previous special day
      -- Ensure that result and refdate are not the same
      select min(s1.day)
        into v_special_day
        from special_days s1
       where p_ref_date+1/1440
          <= s1.day-nvl(p_task_spec.day,0)
                   -nvl(p_task_spec.hour/24,0)
                   -nvl(p_task_spec.minute/1440,0)
         and s1.day    not in (select s2.day
                                from special_days s2
                               where p_ref_date+1/1400
                                  <= s2.day+nvl(p_task_spec.day,0)
                                           +nvl(p_task_spec.hour/24,0)
                                           +nvl(p_task_spec.minute/1440,0)
                                 and level=2 -- sufficient
                             connect by s2.day -1 = prior s2.day                             
                              );

      if(v_special_day is null)then
        return utl.pkg_exceptions.gc_scheduler_special_day;                           
      end if;
      -- Override pattern
      v_override_day:=v_special_day-nvl(p_task_spec.day,0)
                                   -nvl(p_task_spec.hour/24,0)
                                   -nvl(p_task_spec.minute/1440,0);
      v_minute :=to_char(v_override_day,'MI'); 
      v_hour   :=to_char(v_override_day,'HH24');
      v_day    :=to_char(v_override_day,'DD');
      v_month  :=to_char(v_override_day,'MM');
      v_year   :=to_char(v_override_day,'YYYY');
    elsif(p_task_spec.special_days = sched.gc_special_day_AFTER)then
      -- Get the next best special day
      -- Ensure that result and refdate are not the same by adding 1 minute to comparison
      -- The last of a set of successive days is considered.
      select min(s1.day)
        into v_special_day
        from special_days s1
       where p_ref_date+1/1400
          <= s1.day+nvl(p_task_spec.day,0)
                   +nvl(p_task_spec.hour/24,0)
                   +nvl(p_task_spec.minute/1440,0)
         and s1.day+1 not in (select s2.day
                                from special_days s2
                               where p_ref_date+1/1400
                                  <= s2.day+nvl(p_task_spec.day,0)
                                           +nvl(p_task_spec.hour/24,0)
                                           +nvl(p_task_spec.minute/1440,0)
                                 and level=2 -- sufficient
                             connect by s2.day -1 = prior s2.day                             
                              );

      if(v_special_day is null)then
        return utl.pkg_exceptions.gc_scheduler_special_day;                           
      end if;
      -- Override pattern
      v_override_day:=v_special_day+nvl(p_task_spec.day,0)
                                   +nvl(p_task_spec.hour,0)/24
                                   +nvl(p_task_spec.minute,0)/1440;
      -- Hours and minutes remain the same, change D,M,Y                                   
      v_day    :=to_char(v_override_day,'DD');
      v_month  :=to_char(v_override_day,'MM');
      v_year   :=to_char(v_override_day,'YYYY');
    end if;
    return utl.pkg_exceptions.gc_success;
  exception
    when others then
      return utl.pkg_exceptions.gc_scheduler_special_day;
  end special_days_pre_process;

  -- After the next-due-date is calculated based in the pattern, 
  -- amend the result to suit the excludions of special days
  procedure special_days_post_process(p_due_date in out date) 
  is
    v_special_day special_days.day%type;
  begin  
    if(p_task_spec.special_days = sched.gc_special_day_INCLUDE)then
      -- If ref day is a special day, then force it to be included regardless of pattern
      -- Get next special day starting from the reference date
      select min(s1.day)
        into v_special_day
        from special_days s1
       where to_char(s1.day,'YYYYMMDD') >= to_char(p_ref_date,'YYYYMMDD');              
      -- If the special date come before the date resulting from the pattern 
      -- then bring the due_date forward
      if(p_due_date>v_special_day)then
        p_due_date:=to_date(to_char(v_special_day,'YYYYMMDD')||nvl(v_hour,'00')||nvl(v_minute,'00'),'YYYYMMDDHH24MI');
      end if;
    elsif(p_task_spec.special_days = sched.gc_special_day_EXCLUDE)then
      -- Assumption: The are no more than 30 contiguous days in SPECIAL_DAYS
      select min(due_date)
        into v_special_day
        from (
              select counter + p_due_date -1 due_date
                from (select level counter 
                        from dual 
                     connect by level<=30
                     )         
             )             
       where to_date(to_char(due_date,'YYYYMMDD'),'YYYYMMDD') not in 
             (
               select s2.day
                 from special_days s2 
             );
      p_due_date:=to_date(to_char(v_special_day,'YYYYMMDD')||nvl(v_hour,'00')||nvl(v_minute,'00'),'YYYYMMDDHH24MI');
    end if;
  end special_days_post_process;
    
  -- In case of bad task editing, fill in missing lesser date element values
  procedure default_date_elements is
  begin 
    if(  v_minute is null
    and (v_hour is not null or v_day is not null or v_month is not null or v_year is not null) )
    then 
      v_minute:='00';      
    end if;
    
    if(  v_hour is null
    and (v_day is not null or v_month is not null or v_year is not null) )
    then 
      v_hour:='00';      
    end if;
  
    if(  v_day is null
    and (v_month is not null or v_year is not null) )
    then 
      v_day:='00';      
    end if;
  
    if(  v_month is null
    and  v_year is not null )
    then 
      v_month:='00';      
    end if;
  end default_date_elements;
  
-----------------------'ere we go, 'ere we go---------------------------------
begin
  dbms_application_info.set_module(c_proc_name,null);

  -- Special case: The task has just been reset and is in the INITIAL state.
  -- Resolution:   If the NEXT_DATE_DUE is already set (because it has run before),
  --               we leave it and exit.
  if(p_task_spec.next_due_date is not null and 
     p_task_spec.state=sched.gc_state_INITIAL)
  then
    p_due_date:=p_task_spec.next_due_date;   
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,
      'The task has run before and has recently been reset to the INITIAL state. Keep the current NEXT_DUE_DATE as is.',null,p_task_spec.task_id);
  else
  
    -- Check input parameters
    if(p_ref_date is null)then
      utl.pkg_logger.log(null,'The task''s reference date (usually the value in the NEXT_DUE_DATE field) was not provided. '||
        c_proc_name||' cannot proceed.',
        'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
      v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
      p_due_date:=null;
    else 
      -- Make strings for to_date construction  
      v_minute            :=lpad(abs(p_task_spec.minute),2,'0');
      v_hour              :=lpad(abs(p_task_spec.hour),2,'0');
      v_day               :=lpad(abs(p_task_spec.day),2,'0');
      v_month             :=lpad(abs(p_task_spec.month),2,'0');
      v_year              :=lpad(abs(p_task_spec.year),4,'0');
      v_weekdays          :=utl.pkg_string.clean4numbers(p_task_spec.weekdays);
    
      -- In case of bad task editing, fill in missing lesser values
      default_date_elements;   
  
      -- Determine which date to use as reference    
      v_task_hierarchy:=scheduler_dep.get_hierarchy(p_task_spec.task_id);
      if(v_task_hierarchy=scheduler_dep.gc_parent or 
         v_task_hierarchy=scheduler_dep.gc_independent)
      then
        --   Base next due date on previous 'next-due-date'.                
        --   If this is a new task i.e. next_due_date is null, 
        --   then we use the current reference date instead
        --   to 'seed' the sequence.
        v_ref_date:=nvl(p_task_spec.next_due_date,v_ref_date);
      else
        -- Either a child or a middle-of-the-net task:
        -- Base next due date on the current FSM time.
        -- Note that a child task with no pattern has no next_due_date
        if(p_task_spec.next_due_date is not null)then
          v_ref_date:=nvl(p_task_spec.next_due_date,v_ref_date);
        end if;    
      end if;    
    
      if(p_task_spec.special_days is null)then
        days_back_from_end_of_month;
        hours_back_from_end_of_day;
        minutes_back_from_end_of_hour;
      else
        -- Deal with special days that may temporarily modify the pattern
        v_retcode:=special_days_pre_process(v_ref_date);    
        if(v_retcode <> utl.pkg_exceptions.gc_success)then
          return v_retcode;
        end if;
        -- Need to add default date elements since we may have 
        default_date_elements;
      end if;
    
      if( v_minute is not null)then
        if(v_hour   is not null)then
          if(v_day    is not null)then
            if(v_month  is not null)then
              if(v_year  is not null)then
                -- Run this job once only on a specific day and time
                p_due_date:=to_date(v_year||v_month||v_day||v_hour||v_minute,'YYYYMMDDHH24MI');                
                -- If the reference date is the same or greater than this date then and
                -- the task has not recently been created, then no next date should arrise.
                if( p_ref_date>=p_due_date
                and p_task_spec.state<>sched.gc_state_INITIAL
                and p_task_spec.state<>sched.gc_state_EDIT_LOCK)
                then
                  p_due_date:=null;
                  v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                end if;
              else
                -- The following cases also need to be constrained by days-of-week:
                -- Run this job annually
                v_days_in_unit:=365;
    
                -- If we have already run, ensure that it is not in the same year by incrementing the reference date
                -- If this is the first time, ensure that we are not running for a date in the past.
                if( p_task_spec.state=sched.gc_state_DONE 
                or  p_task_spec.state=sched.gc_state_EDIT_LOCK
                or (    p_task_spec.state=sched.gc_state_INITIAL 
                    and granularize_date(v_ref_date,null,v_month,v_day,v_hour,v_minute)<v_ref_date) )
                then
                  v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                end if;
                
                while(true)loop
                  p_due_date:=granularize_date(v_ref_date,null,v_month,v_day,v_hour,v_minute);                
                  if(to_char(p_due_date,'YYYY')<to_char(v_ref_date,'YYYY')
                  or instr(nvl(v_weekdays,'1234567'),utl.pkg_date.day_of_week(p_due_date))=0)
                  then
                    -- Add year
                    v_ref_date:=utl.pkg_date.add_years_shrink(v_ref_date,1);
                    v_loop_count:=v_loop_count+1;
                    if(v_loop_count>(365*5/v_days_in_unit))then
                      -- We are not going to go further than 5 years out
                      utl.pkg_logger.log(null,null,'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
                      v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                      p_due_date:=null;
                      exit;
                    end if;        
                  else
                    exit;
                  end if;
                end loop;               
              end if;
            else
              -- Run this job monthly
              v_days_in_unit:=31;
              
              -- If we have already run, ensure that it is not in the same month by incrementing the reference date
              -- If this is the first time, ensure that we are not running for a datre in the past.
              if( p_task_spec.state=sched.gc_state_DONE 
              or  p_task_spec.state=sched.gc_state_EDIT_LOCK
              or (p_task_spec.state=sched.gc_state_INITIAL 
                  and granularize_date(v_ref_date,null,null,v_day,v_hour,v_minute)<v_ref_date) )
              then
                v_ref_date:=utl.pkg_date.add_months_shrink(v_ref_date,1);
              end if;
              -- In case the date resolves to an impossible date
              while(true) loop
                begin                
                  p_due_date:=granularize_date(v_ref_date,null,null,v_day,v_hour,v_minute);                
                  if(to_char(p_due_date,'YYYYMM')<to_char(v_ref_date,'YYYYMM') 
                  or instr(nvl(v_weekdays,'1234567'),utl.pkg_date.day_of_week(p_due_date))=0)
                  then                  
                    v_ref_date:=add_months(v_ref_date,1); -- increment the reference date and recalculate
                    -- Deal with the odd behaviour of add_months (e.g. 28FEB2004+1month)
                    if(to_char(v_ref_date,'DD')>to_char(p_due_date,'DD'))then
                      v_ref_date:=to_date(to_char(v_ref_date,'YYYYMMHH24MI')||to_char(p_due_date,'DD'),'YYYYMMHH24MIDD');
                    end if;
                    v_loop_count:=v_loop_count+1;
                    if(v_loop_count>(365*5/v_days_in_unit))then
                      -- We are not going to go further than 5 years out
                      utl.pkg_logger.log(null,null,'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
                      v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                      p_due_date:=null;
                      exit;
                    end if;        
                  else
                    exit;
                  end if;                  
                exception
                  when others then
                    -- Impossible date
                    v_ref_date:=add_months(v_ref_date,1);
                end;
              end loop;
            end if;
          else
            -- Run this job daily
            v_days_in_unit:=1;
            
            -- If we have already run, ensure that it is not in the same day by incrementing the reference date
            -- If this is the first time, ensure that we are not running for a datre in the past.
            if( p_task_spec.state=sched.gc_state_DONE 
            or  p_task_spec.state=sched.gc_state_EDIT_LOCK        
            or (p_task_spec.state=sched.gc_state_INITIAL 
                and granularize_date(v_ref_date,null,null,null,v_hour,v_minute)<v_ref_date) )
            then
              v_ref_date:=v_ref_date+1;
            end if;
            
            while(true) loop
              p_due_date:=granularize_date(v_ref_date,null,null,null,v_hour,v_minute);                
              if(to_char(p_due_date,'YYYYMMDD')<to_char(v_ref_date,'YYYYMMDD') 
              or instr(nvl(v_weekdays,'1234567'),utl.pkg_date.day_of_week(p_due_date))=0)
              then
                -- Add day
                v_ref_date:=v_ref_date+1;
                v_loop_count:=v_loop_count+1;
                if(v_loop_count>(365*5))then
                  -- We are not going to go further than 5 years out
                  utl.pkg_logger.log(null,null,'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
                  v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                  p_due_date:=null;
                  exit;
                end if;        
              else
                exit;
              end if;
            end loop;
          end if;
        else
          -- Run this task hourly
          v_days_in_unit:=utl.pkg_date.gc_hours_per_day;
          v_proposed_date:=granularize_date(v_ref_date,null,null,null,null,v_minute);
          
          -- If we have already run, ensure that it is not in the same hour by incrementing the reference date
          -- If this is the first time, ensure that we are not running for a datre in the past.
          if( p_task_spec.state=sched.gc_state_DONE 
          or  p_task_spec.state=sched.gc_state_EDIT_LOCK      
          or (p_task_spec.state=sched.gc_state_INITIAL 
              and v_proposed_date<v_ref_date) )
          then
            v_ref_date:=v_proposed_date+1/24; -- add to make next hour
          end if;        
          
          while(true) loop
            p_due_date:=granularize_date(v_ref_date,null,null,null,null,v_minute);                
            if(to_char(p_due_date,'YYYYMMDDHH24')<to_char(v_ref_date,'YYYYMMDDHH24') 
            or instr(nvl(v_weekdays,'1234567'),utl.pkg_date.day_of_week(p_due_date))=0)
            then
              -- Add hour
              v_ref_date  := v_ref_date+1/24; -- add to make next hour
              v_loop_count:= v_loop_count+1;
              if(v_loop_count>(365*5/v_days_in_unit))then
                -- We are not going to go further than 5 years out
                utl.pkg_logger.log(null,null,'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
                v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                p_due_date:=null;
                exit;
              end if;
            else
              exit;
            end if;
          end loop;
        end if;
      else
        -- Special case: Are dependencies specified?
        if(p_task_spec.dependencies is not null)then
          p_due_date:=null;
        else
          -- Run this task every minute
          v_days_in_unit:=1/utl.pkg_date.gc_mins_per_day;
          
          -- If we have already run, ensure that it is not in the same minute by incrementing the reference date
          -- If this is the first time, ensure that we are not running for a datre in the past.
          v_ref_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24MI'),'YYYYMMDDHH24MI');
          if( p_task_spec.state=sched.gc_state_DONE 
          or  p_task_spec.state=sched.gc_state_EDIT_LOCK      
          or  p_task_spec.state=sched.gc_state_INITIAL)
          then
            v_ref_date:=utl.pkg_date.add_minutes_shrink(v_ref_date,1);
          end if;        
          
          while(true) loop
            p_due_date:=to_date(to_char(v_ref_date,'YYYYMMDDHH24MI'),'YYYYMMDDHH24MI');
            --v_due_date:=utl.pkg_date.add_minutes_shrink(v_ref_date,1);  -- Different since we are dealing with the smallest unit
            if(to_char(p_due_date,'YYYYMMDDHH24MI')<to_char(v_ref_date,'YYYYMMDDHH24MI') 
            or instr(nvl(v_weekdays,'1234567'),utl.pkg_date.day_of_week(p_due_date))=0)
            then
              -- Add minute
              v_ref_date:=utl.pkg_date.add_minutes_shrink(v_ref_date,1);
              v_loop_count:=v_loop_count+1;
              if(v_loop_count>(365*5/v_days_in_unit))then
                -- We are not going to go further than 5 years out
                utl.pkg_logger.log(null,null,'Task Id '||p_task_spec.task_id,null,utl.pkg_exceptions.gc_scheduler_next_due_date);
                v_retcode:=utl.pkg_exceptions.gc_scheduler_next_due_date;
                p_due_date:=null;
                exit;
              end if;        
            else
              exit;
            end if;
          end loop;
        end if;
      end if;
  
      -- Special days post-processing
      if(p_task_spec.special_days is not null)then
        special_days_post_process(p_due_date);
      end if;
      
      -- This is only likely to occur in a test environment
      if(p_due_date is not null)then
        v_minutes_overdue:=trunc((p_ref_date-p_due_date)*utl.pkg_date.gc_mins_per_day);
        if(v_minutes_overdue > 1)then
          utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,'Task Id '||p_task_spec.task_id||' is '||trunc(v_minutes_overdue)||' minutes overdue');
        end if;
      end if;
      
    end if; -- ...check input parameters
  end if;
  dbms_application_info.set_module(null,null);
  return v_retcode;
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    utl.pkg_logger.log(utl.pkg_logger.gc_log_message_debug,scheduler_rep.task_dump(p_task_spec),'Task '||p_task_spec.task_id,p_task_spec.task_id,sqlcode,c_proc_name);
    return sqlcode;
end calc_next_due_date;

-------------------------------------------------------------------------------
-- Calculates the effective as-of-date from the given task's effective_date_offset
-- based on the most recent next_date_due value (or FSM date is not yet available)
-- We consider days-of-week if defined and walk forward  (offset is a positive value)
--                                      or walk backward (offset is a negative value)
--                                     until a matching weekday is reached.
-- Returns the calculated date.
-- If effective_date_offset is not defined or 0, return a NULL date
function calc_dynamic_effective_date(p_task_spec in sched.t_schedule_rec, p_fsm_date in date)
return date
is
  c_proc_name       constant varchar2(100) := pc_schema||'.'||pc_package||'.calc_dynamic_effective_date';
  v_effective_date  date :=nvl(p_task_spec.next_due_date,p_fsm_date);
  v_days            pls_integer:=0;
  v_op_days         pls_integer:=0;
  v_days_of_week    schedules.weekdays%type:=trim(replace(p_task_spec.weekdays,'.'));
  v_offset          pls_integer:=nvl(p_task_spec.effective_date_offset,0);
begin
  dbms_application_info.set_module(c_proc_name,null);
  if(v_offset<>0)then        
    if(v_days_of_week is not null and length(v_days_of_week)<>7)then
      -- Get the number of days including the non-operational days specified in the weekdays field
      -- We do this the crude way by looping until we get the right number of days          
      while(v_op_days<>v_offset)loop
        v_effective_date:=v_effective_date+sign(v_offset);
        if(instr(v_days_of_week,utl.pkg_date.day_of_week(v_effective_date))<>0)then
          -- This Day 
          v_op_days:=v_op_days+sign(v_offset);  
        end if;
        v_days:=v_days+sign(v_offset);
        if(abs(v_days)>28)then
          raise utl.pkg_exceptions.e_scheduler_task_spec;
        end if;
      end loop;
    else
      -- No day exclusion - simply add the offset
      v_effective_date:=v_effective_date+v_offset;
    end if;
  end if;    
  dbms_application_info.set_module(null,null);
  return v_effective_date;
exception
  when others then
    raise utl.pkg_exceptions.e_scheduler_inv_date;
end calc_dynamic_effective_date;

-- Calculate dynamic values for previously agreed identifyers and implementations
-- Procedures like this should ideally be pluggable into this scheduler package
-- as more types of dynamic variables are added to the system, but this is not
-- supported by PL/SQL. So we follow the recipe outlined below:
procedure calc_dynamic_values(p_task_spec in out sched.t_schedule_rec, p_fsm_date in date := null)
is
  c_proc_name       constant varchar2(100) := pc_schema||'.'||pc_package||'.calc_dynamic_values';
  v_task_spec       sched.t_schedule_rec:=p_task_spec;
begin
  dbms_application_info.set_module(c_proc_name,null);
  
  -- Recipe:  
  p_task_spec.command:=replace(p_task_spec.command,sched.gc_dyn_parm_effective_date,
                               to_char(calc_dynamic_effective_date(p_task_spec,nvl(p_fsm_date,sysdate)),
                                       sched.gc_dyn_form_effective_date));
  -- Do the same for any other dynamic parameters....

  -- The resulting thing should not ever be updated to the schedules table!
  dbms_application_info.set_module(null,null);
exception
  when others then
    utl.pkg_errorhandler.handle;
    utl.pkg_logger.log;
    -- Restore to original
    p_task_spec:=v_task_spec;
end calc_dynamic_values;


end scheduler_due;
-------------------------------------------------------------------------------
-- end of file
-------------------------------------------------------------------------------
/
