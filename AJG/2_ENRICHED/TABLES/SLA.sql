create or replace table "2_ENRICHED".sla
as
with sla_urgent as(
select * exclude workflow_type
    ,case when workflow_type = 'Update Clients' -- Mismatch between Urgent/Standard SLA
    then 'Update Client'
    else workflow_type
    end as workflow_type
from "1_RAW".sla_urgent
UNPIVOT (sla FOR location IN (LONDON, SWANSEA, EDINBURGH))
)
select stn.workflow_type
    , urg.location
    , stn.sla as standard_sla
    , urg.sla as urgent_sla
from "1_RAW".sla_standard as stn
left join sla_urgent as urg
on stn.workflow_type = urg.workflow_type
and lower(stn.location) = lower(urg.location);