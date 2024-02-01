# Import python packages
import streamlit as st
import matplotlib.pyplot as plt
from snowflake.snowpark import Session
import openai
import numpy as np


col1, col2, = st.columns([1,3])
with col1:
    st.image('''https://www.princess.co.uk/wp-content/uploads/2020/10/Gallagher-Logo.png''')
with col2:    
    st.header("AJ Gallagher UK (Speciality)")
st.header("Claims Processing Dashboard")


# Themed Divider
st.markdown('<hr style="border:.5px solid #1669c9">', unsafe_allow_html=True)

###################
### Get Session ###
###################

connection_parameters = {
    "account": "fb59538.eu-west-2.aws",
    "user": "DOUGALTOMS",
    "password": "Drimlee99",
    "role": "ACCOUNTADMIN", 
    "warehouse": "COMPUTE_WH",
    "database": "AJG"
  }  

# connection_parameters = st.secrets['connection_parameters']

tab1, tab2, tab3 = st.tabs(["Key Metrics", "RAG Status", "AI SQL Assisstant"])

session = Session.builder.configs(connection_parameters).create()

with tab1:

    st.subheader('Key Metrics')
    ###################
    ### Key Metrics ###
    ###################

    open_tasks = session.sql('''SELECT count(*) as cnt FROM AJG."2_ENRICHED".COMBINED where completed_by = 'Open';''').to_pandas()
    open_tasks = open_tasks['CNT'][0]
    st.info(f"Action required: {open_tasks} open subtasks", icon='⚠️')


    col1, col2, col3 = st.columns(3)

    # Percentage of standard claims meeting SLA
    if 'std' not in st.session_state:
        st.session_state['std'] = session.sql('''select count(*) as total from AJG."2_ENRICHED".SUBTASK_RAG where urgency = 'Standard' and total_business_hours <= sla;''').to_pandas()

    col1.metric("Standard Claims Meeting SLA", f"{round(st.session_state['std']['TOTAL'][0]/7080*100,2)} %",f"-{80- round(st.session_state['std']['TOTAL'][0]/7080*100,2)}%")

    # Total claims this month compared to last
    if 'total_claims' not in st.session_state:
        st.session_state['total_claims'] = session.sql('''select month("MONTH_START"), count from AJG."3_PRESENTATION".last_month_claims;''').to_pandas()
    st.dataframe(st.session_state['total_claims'])
    #col2.metric("Claims This Month", f"{st.session_state['total_claims']['COUNT'][1]}", f"{st.session_state['total_claims']['COUNT'][1]-st.session_state['total_claims']['COUNT'][0]}")

    # Percentage of urgent claims meeting SLA
    if 'std2' not in st.session_state:
        st.session_state['std2'] = session.sql('''select count(*) as total from "2_ENRICHED".subtask_rag where urgency = 'Urgent' and total_business_hours <= sla;''').to_pandas()
    col3.metric("Urgent Claims Meeting SLA", f"{round(st.session_state['std2']['TOTAL'][0]/1310*100,2)} %", f"-{90- round(st.session_state['std2']['TOTAL'][0]/1310*100,2)}%")


    ######################
    ### Monthly Claims ###
    ######################

    key1, key2 = st.columns(2)

    with key1:
        st.write('**Total Claims per Month**')
        st.caption('Larger Volume of Claims in Q2-3')

        if 'monthly_claims' not in st.session_state:
            st.session_state['monthly_claims'] = session.sql('''select * from "3_PRESENTATION".MONTHLY_CLAIMS;''').to_pandas()

        st.bar_chart(st.session_state['monthly_claims'], x="MONTH_NUMBER", y=["STANDARD_CLAIMS", "URGENT_CLAIMS"], use_container_width=True)
        
    with key2:
        st.write('**Task Types by Average Hours**')
        st.caption('Look to Improve Time to Notify Insurer')
        if 'avg_hours' not in st.session_state:
            st.session_state['avg_hours'] = session.sql('''select task, avg(round(avg_total_business_hours,0))::int as avg_hours from AJG."3_PRESENTATION".task_times group by 1 order by 2 desc;''').to_pandas()
        st.bar_chart(st.session_state['avg_hours'], y="AVG_HOURS", x= "TASK", use_container_width=True)

    ##################
    ### Area Chart ###
    ##################
    st.write('**Cumulative Count of Subtasks by Team**')
    st.caption('Large disparity between most busy (Energy) and least busy (Financial Risks) teams')
    if 'df' not in st.session_state:
        st.session_state['df'] = session.sql('''select * from AJG."3_PRESENTATION".cumulative_subtasks_by_team where team = 'Energy' or team = 'Financial Risks' ''').to_pandas()

    st.area_chart(st.session_state['df'], x="WEEK",y="CUMULATIVE_COUNT", color="TEAM")


with tab2:
    ######################
    ### Create Filters ###
    ######################

    st.subheader('Filter RAG Charts By Location / Team')

    # Get Location info for filters
    if 'info' not in st.session_state:
        st.session_state['info'] = session.sql('''select distinct location, team from AJG."2_ENRICHED".SUBTASK_RAG where location is not null or team is not null order by 1, 2;''').to_pandas()

    locations = st.session_state['info']["LOCATION"].unique()
    location_list = []

    # Normalise location names
    for location in locations:
        location = location.capitalize()
        location_list.append(location)


    filters = st.toggle('Show Available Filters')
    col1, col2 = st.columns(2)

    ###########################
    ### Filtered RAG Charts ###
    ###########################

    # If filter toggle is chosen then display RAG pie charts based on user input (location/team)
    if filters:

        with col1:
            location_selection = st.selectbox('Location',location_list)  

            if location_selection:
                st.session_state['location_selection'] = location_selection

                with col2:
                    with st.spinner(f'Loading Teams from {location_selection}'):
                        # Get Location info for Teams
                        teams = st.session_state['info'][st.session_state['info']["LOCATION"] == location_selection]
                        teams = teams["TEAM"].unique()
                        team_selection = st.selectbox('Team', teams)

        # Urgent RAG pie chart
        with col1:
            st.write(f'**Urgent Subtasks ({location_selection}, {team_selection})**')
            urgent_rag = session.sql(f'''select rag, location, urgency, count(task) as COUNT from AJG."2_ENRICHED".SUBTASK_RAG group by rag, location, urgency having urgency='Urgent' and location = '{location_selection}' order by rag;''').to_pandas()
            sizes = [urgent_rag['COUNT'][0],urgent_rag['COUNT'][1],urgent_rag['COUNT'][2]]
            colors = { 'ORANGE': 'orange',
                    'GREEN': 'green',
                    'RED': 'red'
                        }
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, startangle=90, colors=colors)#,autopct='%1.1f%%')
            ax1.axis('equal') 
            st.pyplot(fig1)

        # Standard RAG pie chart
        with col2:
            st.write(f'**Standard Subtasks ({location_selection}, {team_selection})**')
            standard_rag = session.sql(f'''select rag, urgency, count(task) as cnt from AJG."2_ENRICHED".SUBTASK_RAG group by rag, location, urgency having urgency='Standard' and location = '{location_selection}' order by rag;''').to_pandas()
            sizes = [standard_rag['CNT'][0],standard_rag['CNT'][1],standard_rag['CNT'][2]]
            colors = {'ORANGE': 'orange',
                    'GREEN': 'green',
                    'RED': 'red'}
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, startangle=90, colors=colors) #,autopct='%1.1f%%')
            ax1.axis('equal') 
            st.pyplot(fig1)

        st.caption('Few Subtasks (Urgent & Standard) Meet Their Target SLA')

    ##########################
    ### Overall RAG Charts ###
    ##########################

    # Else display RAG pie charts based on overall performance
    else:

        # Urgent RAG pie chart
        with col1:
            st.write(f'**Urgent Subtasks (Overall)**')
            urgent_rag = session.sql(f'''select rag, urgency, count(task) as COUNT from AJG."2_ENRICHED".SUBTASK_RAG group by rag, urgency having urgency='Urgent' order by rag;''').to_pandas()
            sizes = [urgent_rag['COUNT'][0],urgent_rag['COUNT'][1],urgent_rag['COUNT'][2]]
            colors = {'ORANGE': 'orange',
                    'GREEN': 'green',
                    'RED': 'red'}
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, startangle=90, colors=colors)#,autopct='%1.1f%%')
            ax1.axis('equal') 
            st.pyplot(fig1)

        # Standard RAG pie chart
        with col2:
            st.write(f'**Standard Subtasks (Overall)**')
            standard_rag = session.sql(f'''select rag, urgency, count(task) as cnt from AJG."2_ENRICHED".SUBTASK_RAG group by rag, urgency having urgency='Standard' order by rag;''').to_pandas()
            sizes = [standard_rag['CNT'][0],standard_rag['CNT'][1],standard_rag['CNT'][2]]
            colors = {'ORANGE': 'orange',
                    'GREEN': 'green',
                    'RED': 'red'}
            fig1, ax1 = plt.subplots()
            ax1.pie(sizes, startangle=90, colors=colors) #,autopct='%1.1f%%')
            ax1.axis('equal') 
            st.pyplot(fig1)

        st.caption('Few Subtasks (Urgent & Standard) Meet Their Target SLA')



    ##################################
    ### Combined Data For Download ###
    ##################################

    with st.expander('Show Combined Data Set'):

        count = session.sql('''select count(*) as "COUNT" from AJG."2_ENRICHED".COMBINED_WITH_TIMES''').to_pandas()
        st.write(f'{count["COUNT"][0]} records (reassigned subtasks removed)')
        df = session.sql('''select * exclude total_business_hours from AJG."2_ENRICHED".COMBINED_WITH_TIMES''').collect()
        st.dataframe(df)

with tab3:
    ################
    ### Chat Bot ###
    ################

    st.subheader('Query Data Using Chatbot')
    st.write('''LLM-powered chat bots can be used to generate queries against data in Snowflake, enabling data exploration and analytics for non-technical business users
    \nExample question: 'What is the most common workflow type when processing claims?' ''')


    openai.api_key= st.secrets["api_key"] 
    
    prompt = """
        You will be acting as an AI Snowflake SQL Expert for an insurance company.
        Your goal is to give correct, executable sql query to users.
        You will be replying to users who will be confused if you don't respond in the character of the AI Snowflake SQL Expert.
        You are given one table. This table has various metrics for insurance policy claims. Each claim triggers a workflow which can be comprised 
        of 1 or more subtasks. Depending on the urgency of a subtask ('Urgent' or 'Standard') and the location of the
        office, a corresponding SLA is assigned.
        
        The table name is: AJG."2_ENRICHED".COMBINED_WITH_TIMES
        
        The columns are: 
        WORKFLOW_TYPE
        TOTAL_BUSINESS_HOURS
        TASK
        WORKFLOW_ID
        SUBTASK_ID
        CREATED_DATETIME
        COMPLETED_BY
        COMPLETED_DATETIME
        CREATED_DAY
        LOCATION
        COMPLETED_DAY
        URGENCY
        SLA
        ASSIGNED_TO
        TEAM

        The user will ask questions, for each question you should respond and include a sql query based on the question and the above table. 


        Here are 6 critical rules for the interaction you must abide:
        1. You MUST MUST wrap the generated sql code within ``` sql code markdown in this format e.g
        ```sql
        (select 1) union (select 2)
        ```
        2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
        3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
        4. Make sure to generate a single snowflake sql code, not multiple. 
        5. You should only use the table columns mentioned above, and the table name is always 'AJG."2_ENRICHED".ALL_SUBTASKS_ENRICHED', you MUST NOT hallucinate about the table names
        6. DO NOT put numerical at the very front of sql variable.
        
        Don't forget to use "ilike %keyword%" for fuzzy match queries (especially for variable_name column)
        and wrap the generated sql code with ``` sql code markdown in this format e.g:
        ```sql
        (select 1) union (select 2)
        ```

        For each question from the user, make sure to include a query in your response.

        The following is the question from the user:

    """
    with st.expander('🤖 Generate SQL Query using AI bot'):
        user_prompt = st.text_input('Hi, I\'m your AI SQL bot 👋')

        prompt += '\n\n'
        prompt += user_prompt


        response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                timeout=60)

        if response:
            st.markdown(response["choices"][0]["message"]["content"])
