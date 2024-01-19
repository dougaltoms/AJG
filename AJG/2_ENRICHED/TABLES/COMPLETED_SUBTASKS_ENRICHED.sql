create or replace table "2_ENRICHED".completed_subtasks_enriched
as
select w.workflow_id
    , w.workflow_type
    , (distinct s.subtask_id)
    , s.task
    , (w.created_date || ' ' || w.create_time)::datetime as created_datetime
    , w.urgency
    , case 
        when w.urgency = 'Urgent' then sla.urgent_sla
        when w.urgency = 'Standard' then sla.standard_sla
      end as sla
    , ah.assigned_to
    , u.*
    , s.completed_by
    , try_to_timestamp((s.completed_date || ' ' || s.completed_time),'DD/MM/YYYY HH24:MI:SS') as completed_datetime
from "1_RAW".workflows as w
left join "1_RAW".subtasks as s
on w.workflow_id = s.workflow_id
left join "1_RAW".users as u
on s.completed_by = u.user
left join "1_RAW".assignment_history as ah
on s.subtask_id = left(ah.assignment_id, len(ah.assignment_id)-2)
left join "2_ENRICHED".sla as sla
on sla.workflow_type = s.task
and lower(sla.location) = lower(u.location)
where completed_by is not null
order by workflow_id, subtask_id;