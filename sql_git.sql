with logs_time as (
  select user_id, event, event_time, value, case
                                               when event_lag_time is NULL then 1
                                               when dif_min > 5 then 1
                                               else 0
                                            end as num_ses
  from (select *, extract(epoch from(event_time::timestamp - event_lag_time::timestamp))/60 as dif_min
        from (select *, lag(event_time) over(partition by user_id order by event_time) as event_lag_time
              from logs))) as tbl
number_sessions as (
  select user_id, event_time, event, value, sum(num_ses) over(partition by user_id order by event_time rows between unbounded preceding and current row) as session_number 
  from logs_time)
select value, count(*) as value_count
from (select user_id, session_number, value, count(*) as count_val_session_seq
      from (select *, val_num - row_number() over(partition by user_id, session_number order by event_time) as sequence 
            from (select *, row_number() over(partition by user_id, session_number, value order by event_time) as val_num 
                  from number_sessions
                  where event = 'template_selected') as tbl1)
      group by user_id, session_number, value, sequence)
where count_val_session_seq > 2
group by value
order by count(*) desc
limit 5






