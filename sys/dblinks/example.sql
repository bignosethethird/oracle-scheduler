------------------------------------------------------------------------------
------------------------------------------------------------------------------
--  Webmark database link
--
------------------------------------------------------------------------------
prompt Database link to example

-- Drop link if it already exists
declare 
  v_count integer:=0;
begin
  select count(*)
    into v_count
    from sys.all_db_links
    where owner = 'sys'
    and	  db_link = 'example';
  if(v_count>0)then
    execute immediate 'drop database link sys.example';
  end if;
end;

create database link sys.example connect to example_mgr identified by somepassword using 'fully.qualified.tnsname';


------------------------------------------------------------------------------
-- end of file
------------------------------------------------------------------------------
