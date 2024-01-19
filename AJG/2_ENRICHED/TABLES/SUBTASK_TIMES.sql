create or replace table "2_ENRICHED".subtask_times
as
select 
    task
    , subtask_id
    , location
    , created_datetime
    , dayname(created_datetime) as created_day
    , case
            when dayname(created_datetime) = 'Sat' 
            then dateadd('HOUR', 9, dateadd('DAY', 2, date_trunc('DAY', created_datetime)))
            when dayname(created_datetime) = 'Sun' 
            then dateadd('HOUR', 9, dateadd('DAY', 1, date_trunc('DAY', created_datetime)))
            when extract(hour from created_datetime) >= 17
            then dateadd('HOUR', 9, dateadd('DAY', 1, date_trunc('DAY', created_datetime)))
            when extract(hour from created_datetime) between 00 and 09
            then dateadd('HOUR', 9, date_trunc('DAY', created_datetime))
        else created_datetime
    end as business_created_datetime
    , completed_datetime
    , dayname(completed_datetime::datetime) as completed_day
    , case
            -- If completed on Saturday, finish at 17:00 on previous Friday
            when dayname(completed_datetime) = 'Sat' 
            then dateadd('HOUR', 17, dateadd('DAY', -1, date_trunc('DAY', completed_datetime::datetime)))
            -- If completed on Sunday, finish at 17:00 on previous Friday
            when dayname(completed_datetime) = 'Sun' 
            then dateadd('HOUR', 17, dateadd('DAY', -2, date_trunc('DAY', completed_datetime::datetime))) 
            -- If completed before working day starts, finish at 17:00 previous day
            when extract(hour from completed_datetime::datetime) between 00 and 09
            then dateadd('HOUR', 17, dateadd('DAY', -1, date_trunc('DAY', completed_datetime::datetime)))
            -- If completed after working day ends finish at 17:00 on same day
            when extract(hour from completed_datetime::datetime) >= 17
            then dateadd('HOUR', 17, date_trunc('DAY', completed_datetime::datetime))
        else completed_datetime
    end as business_completed_datetime
    -- ***********************************************************************************
    ,"1_RAW".buss_hours(business_created_datetime,business_completed_datetime) as total_business_hours
    -- ***********************************************************************************
from "2_ENRICHED".completed_subtasks_enriched
order by workflow_id,subtask_id;