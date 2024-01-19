create or replace view location_sla
as
select distinct location
    , urgency
    , round(avg(sla),0) as avg_sla
    , round(avg(total_business_hours),0) as avg_business_hours
from "2_ENRICHED".subtask_rag
group by 1,2;