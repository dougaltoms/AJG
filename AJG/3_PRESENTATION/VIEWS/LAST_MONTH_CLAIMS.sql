create or replace view last_month_claims
as
select date_trunc('MONTH', created_datetime) as month_start
    , count(distinct workflow_id) as "COUNT"
    from "2_ENRICHED".all_subtasks_enriched
    where created_datetime >= date_trunc('MONTH', current_date()) - interval '2 MONTH'
    and created_datetime < date_trunc('MONTH', current_date())
    group by date_trunc('MONTH', created_datetime)
;