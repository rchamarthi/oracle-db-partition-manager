create or replace package pkg_partition_manager
as
 procedure p_create_missing_partitions(
    /* add this procedure to the partitions/maint/utilities package */
    p_owner      in partition_metadata.owner%type,
    p_table_name in partition_metadata.table_name%type
  );
end;
/

create or replace package body pkg_partition_manager
as

 procedure p_create_missing_partitions(
    /* add this procedure to the partitions/maint/utilities package */
    p_owner      in partition_metadata.owner%type,
    p_table_name in partition_metadata.table_name%type
  )
 is
  /*
  ToDo:
  1) change code to be modular based on partition type (daily, monthly, yearly)
  2) error handling and custom error messages
  4) rebuild any global indexes that are defined - performance issues on billion row tables
  */
  l_partition_name_format  partition_metadata.partition_name_format%type;
  l_first_partition_name   partition_metadata.first_partition_name%type;
  l_partition_frequency    partition_metadata.partition_frequency%type;
  l_num_advance_partitions partition_metadata.num_advance_partitions%type;
  l_maxval_partition_name  partition_metadata.maxval_partition_name%type;
  i            number;
  /* format : Y{yyyy}M{mm}, expression/value : Y2014M01 */
  l_format_start_pos number;
  l_format_end_pos   number;
  l_expr_start_pos   number;
  l_expr_end_pos     number;
  l_format           varchar2(20); --{yyyy} in iteration 1,  {mm} in iteration 2
  l_expr             varchar2(20);  --2014   in iteration 1,   01  in iteration 2            
  l_format_complete  varchar2(20);
  l_expr_complete    varchar2(20);
  l_num_partitions_query varchar2(200);
  l_partition_maxval_query varchar2(200);
  /* array to hold which patterns are replaced by which expressions.. */
  type t_map_tab  is table of varchar2(20) index by varchar2(20); --eg. map_tab('{yyyy}')='2014'
  map_tab t_map_tab;
  l_idx varchar2(20);
  l_first_date date;
  l_partitions_query varchar2(4000);
  l_tabbed_new_line varchar2(2) := chr(10) || chr(13);
  l_ref_cursor sys_refcursor;
  l_alter_statement varchar2(400);
begin
  
  select partition_frequency, 
         partition_name_format, 
         first_partition_name, 
         num_advance_partitions,
         maxval_partition_name
  into   l_partition_frequency, 
         l_partition_name_format, 
         l_first_partition_name, 
         l_num_advance_partitions,
         l_maxval_partition_name
  from   partition_metadata
  where  owner = p_owner
    and  table_name = p_table_name;

  if l_partition_frequency not in ('DAILY','MONTHLY') then
      raise_application_error(-20001,'Only partition frequencies of daily and monthly are supported');
  end if;
  
  i := 1; --occurance
  l_format_start_pos := instr(l_partition_name_format,'{',1,i);
  while l_format_start_pos <> 0 loop
    l_format_end_pos := instr(l_partition_name_format,'}',l_format_start_pos+1,1);
    l_format := substr(l_partition_name_format,l_format_start_pos, l_format_end_pos-l_format_start_pos+1);
    l_format_complete := l_format_complete || regexp_replace(l_format,'{|}','');

    -- actual expression
    l_expr_start_pos := l_format_start_pos  - (i-1)*2;
    l_expr_end_pos   := l_format_end_pos    - (i)*2;
    l_expr           := substr(l_first_partition_name, l_expr_start_pos, l_expr_end_pos - l_expr_start_pos + 1);
    l_expr_complete     := l_expr_complete || l_expr;
    
    --store the expression and format key value pairs
    map_tab(l_format) := l_expr;
    
    -- next occurance
    i := i+1;
    l_format_start_pos := instr(l_partition_name_format,'{',1,i);

  end loop;

  -- get the first date 
  -- 20140101 could mean the first daily partition in jan 2014, first monthly partition in 2014, or the 2014 yearly partition
  -- Todo : raise exceptions if partition type is not date.. (or should this be in the insert trigger)?

  select to_date(l_expr_complete, l_format_complete) into l_first_date from dual;

if map_tab.count > 0 then
    l_idx := map_tab.first;
    
    if l_partition_frequency = 'DAILY' then
        l_partitions_query := 'replace(''' || l_partition_name_format || ''',''' || l_idx || ''',' ||  
                              'to_char(to_date(''' || l_first_date || ''')+level,''' ||
                              regexp_replace(l_idx,'{|}','') || '''))';
    elsif l_partition_frequency = 'MONTHLY' then
        l_partitions_query := 'replace(''' || l_partition_name_format || ''',''' || l_idx || ''',' ||  
                              'to_char(add_months(to_date(''' || l_first_date || '''),level),''' || 
                              regexp_replace(l_idx,'{|}','') || '''))';
    end if;
    
    l_idx := map_tab.next(l_idx);
    while (l_idx is not null) loop
        if l_partition_frequency = 'DAILY' then
          l_partitions_query := 'replace(' || l_partitions_query || ',''' || l_idx || ''',' ||  
                                'to_char(to_date(''' || l_first_date || ''')+level,''' || 
                                regexp_replace(l_idx,'{|}','') || ''')) ';
        elsif l_partition_frequency = 'MONTHLY' then
          l_partitions_query := 'replace(' || l_partitions_query || ',''' || l_idx || ''',' ||  
                                'to_char(add_months(to_date(''' || l_first_date || '''),level),''' || 
                                regexp_replace(l_idx,'{|}','') || ''')) ';
        end if;
        l_idx := map_tab.next(l_idx);
    end loop;
--todo
--else raise_application_error?
end if;

-- number of partitions to create
if l_partition_frequency = 'DAILY' then
   l_num_partitions_query := ' trunc(sysdate -  to_date(''' || l_first_date || ''')) ';
elsif l_partition_frequency = 'MONTHLY' then
   l_num_partitions_query := ' trunc(months_between(sysdate,to_date(''' || l_first_date || '''))) ';
end if;

-- partition level maxval (eg 2014-01-02 for p20140101 partition)
if l_partition_frequency = 'DAILY' then
   l_partition_maxval_query := ' to_date(''' || l_first_date || ''') + level + 1 ';
elsif l_partition_frequency = 'MONTHLY' then
   l_partition_maxval_query := ' add_months(to_date(''' || l_first_date || '''),level) ';
end if;

--using left outer join instead of minus to get the partition maxval and the name in one query
--doing minus with all_tab_partitions is complicated becuase maxval is of long datatype.
-- chr(10) for new line, so that printed query is easier to debug

l_partitions_query := 'with all_partitions as (select ' || l_partitions_query || ' partition_name, ' 	|| chr(10) ||  
                            l_partition_maxval_query || ' partition_maxval, ' 							|| chr(10) || 
                            ' level partition_position ' 												|| chr(10) || 
                            ' from dual'             													|| chr(10) || 
                      ' connect by level < ' || l_num_partitions_query || ' + ' ||  l_num_advance_partitions || chr(10) ||
                      '), current_partitions as (' 														|| chr(10) ||
                      ' select partition_name from all_tab_partitions ' 								|| chr(10) ||
                      ' where table_owner = ''' || p_owner || '''' 										|| chr(10) ||
                      '   and table_name = ''' || p_table_name || ''')' 								|| chr(10) ||
                      'select   '                                                                       || chr(10) ||
                      '       ''alter table ' || p_owner || '.' || p_table_name || ' split partition '  || l_maxval_partition_name || 
                      ' at ( to_date('''''' || ap.partition_maxval || '''''')) into ' ||  
                      '         (partition '' || ap.partition_name  || '' ,partition ' || l_maxval_partition_name || ') update global indexes''' || chr(10) ||
                      'from   current_partitions cp, all_partitions ap' 								|| chr(10) ||
                      'where  ap.partition_name = cp.partition_name(+)' 								|| chr(10) ||
                      '  and  cp.partition_name is null' 												|| chr(10) ||
                      ' order by partition_position asc';
                       
-- add split partitions 
--dbms_output.put_line(l_partitions_query);
-- execute immediate l_partitions_query;
  open l_ref_cursor for l_partitions_query;
  loop
    fetch l_ref_cursor into l_alter_statement;
    exit when l_ref_cursor%notfound;
--    dbms_output.put_line('inside for loop : ' || l_alter_statement);
    execute immediate l_alter_statement;
  end loop;

end p_create_missing_partitions;

end pkg_partition_manager;
/
