# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session


st.title("Claims Processing Dashboard")
st.subheader("AJ Gallagher UK (Speciality)")
st.write(
    """Create a dashboard in a tool of your choice (e.g. Power BI/Tableau). 1 page that covers
the most important insights for a claims manager, with a minimum of 6 key insights. (if
you don’t have access to a tool, please create a dashboard like experience in Excel)
    """)

###################
### Get Session ###
###################

session = get_active_session()

###################
### Key Metrics ###
###################

col1, col2, col3 = st.columns(3)

# Percentage of standard claims meeting SLA
std = session.sql('''select count(*) as total from "2_ENRICHED".subtask_rag where urgency = 'Standard' and total_business_hours <= sla;''').to_pandas()
col1.metric("Standard Claims Meeting SLA", f"{round(std['TOTAL'][0]/7080*100,2)} %")

# Total claims this month compared to last
total_claims = session.sql('''select month("MONTH_START"), count from AJG."3_PRESENTATION".last_month_claims;''').to_pandas()
col2.metric("Claims This Month", f"{total_claims['COUNT'][1]}", f"{total_claims['COUNT'][1]-total_claims['COUNT'][0]}")

# Percentage of urgent claims meeting SLA
std = session.sql('''select count(*) as total from "2_ENRICHED".subtask_rag where urgency = 'Urgent' and total_business_hours <= sla;''').to_pandas()
col3.metric("Urgent Claims Meeting SLA", f"{round(std['TOTAL'][0]/1310*100,2)} %")

######################
### Monthly Claims ###
######################

monthly_claims = session.sql('''select * from "3_PRESENTATION".MONTHLY_CLAIMS;''').to_pandas()
st.bar_chart(monthly_claims, x="MONTH_NUMBER", y="NUMBER_CLAIMS")