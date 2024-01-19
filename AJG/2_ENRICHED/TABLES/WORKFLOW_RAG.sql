with rag_cte as (
select sb.workflow_id
    , case 
        when sb.urgency = 'Urgent'
        then sla.urgent_sla
        when sb.urgency = 'Standard'
        then sla.standard_sla
      end as sla
    , sbt.total_business_hours
    , case
        when total_business_hours <= sla
        then 'GREEN'
        when total_business_hours between sla*1 and sla*1.25
        then 'AMBER'
        when total_business_hours > sla*1.25
        then 'RED'
      end as RAG
from "2_ENRICHED".completed_subtasks_enriched as sb
left join "2_ENRICHED".sla as sla
on sb.task = sla.workflow_type
and lower(sb.location) = lower(sla.location)
left join "2_ENRICHED".subtask_times as sbt
on sb.subtask_id = sbt.subtask_id
order by 1, 2)

select workflow_id
    , sum(sla) as sla
    , sum(total_business_hours) as total_business_hours
    , case
        when sum(total_business_hours) <= sum(sla)
        then 'GREEN'
        when sum(total_business_hours) between sum(sla)*1 and sum(sla)*1.25
        then 'AMBER'
        when sum(total_business_hours) > sum(sla)*1.25
        then 'RED'
      end as RAG
from rag_cte
group by 1;