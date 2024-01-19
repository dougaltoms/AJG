create or replace view urgent_sla
as
select round(avg(total_business_hours),0) as hours
, round(avg(sla),0) as sla 
from "2_ENRICHED".subtask_rag
where urgency = 'Urgent';