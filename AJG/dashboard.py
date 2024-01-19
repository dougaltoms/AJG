# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import matplotlib.pyplot as plt

st.title("Claims Processing Dashboard")
st.subheader("AJ Gallagher UK (Speciality)")
st.write(
    """Create a dashboard in a tool of your choice (e.g. Power BI/Tableau). 1 page that covers
the most important insights for a claims manager, with a minimum of 6 key insights. (if
you don’t have access to a tool, please create a dashboard like experience in Excel)
    """)

st.markdown('---')

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

##################
### RAG Charts ###
##################
st.subheader('RAG status for Urgent/Standard Subtasks Compared to SLA')

rag_col1, rag_col2 = st.columns(2)

#Urgent RAG pie chart
with rag_col1:
    st.write('Urgent Subtasks')
    urgent_rag = session.sql('''select rag, urgency, count(task) as cnt from AJG."2_ENRICHED".SUBTASK_RAG group by rag, urgency having urgency='Urgent';''').to_pandas()
    sizes = [urgent_rag['CNT'][0],urgent_rag['CNT'][1],urgent_rag['CNT'][2]]
    colors = {'ORANGE': 'orange',
              'GREEN': 'green',
              'RED': 'red'}
    
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, autopct='%1.1f%%', startangle=0, colors=colors)
    ax1.axis('equal') 
    st.pyplot(fig1)



#Standard RAG pie chart
with rag_col2:
    st.write('Standard Subtasks')
    standard_rag = session.sql('''select rag, urgency, count(task) as cnt from AJG."2_ENRICHED".SUBTASK_RAG group by rag, urgency having urgency='Standard';''').to_pandas()
    sizes = [standard_rag['CNT'][0],standard_rag['CNT'][1],standard_rag['CNT'][2]]
    colors = {'RED': 'red',
              'ORANGE': 'orange',
              'GREEN': 'green'}
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, autopct='%1.1f%%', startangle=90, colors=colors)
    ax1.axis('equal') 
    st.pyplot(fig1)