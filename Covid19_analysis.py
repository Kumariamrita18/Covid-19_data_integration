#!/usr/bin/env python
# coding: utf-8

# In[1]:


import snowflake.connector as sf
import pandas as pd


# In[2]:


# make changes as per your credentials
user='KUMARIAMRITA'
password = '*********'
account='*********'
database='COVID19_EPIDEMIOLOGICAL_DATA'
warehouse='COMPUTE_WH'
schema='PUBLIC'
role='ACCOUNTADMIN'


# In[3]:


conn = sf.connect(user = user,
           password = password,
           account = account,
    )


# In[4]:


def run_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


# In[5]:


try:
    warehouse_sql = 'use warehouse {}'.format(warehouse)
    run_query(conn, warehouse_sql)
    
    try:
        sql = 'alter warehouse {} resume'.format(warehouse)
        run_query(conn, sql)
    except:
        pass
    
    sql = 'use database {}'.format(database)
    run_query(conn, sql)
    
    sql = 'use role {}'.format(role)
    run_query(conn, sql)
    
    sql = f'use schema {schema}'
    run_query(conn, sql)

except Exception as e:
    print(e)


# In[6]:


sql = 'select count(*) from CT_US_COVID_TESTS'
pd.read_sql(sql, conn)


# In[7]:


sql = 'select count(*) from CDC_REPORTED_PATIENT_IMPACT'
pd.read_sql(sql, conn)


# In[8]:


sql = "select * from CDC_REPORTED_PATIENT_IMPACT"
df = pd.read_sql(sql, conn)


# In[9]:


import warnings
warnings.filterwarnings("ignore")
sql = "select * from CT_US_COVID_TESTS"
df1 = pd.read_sql(sql, conn)


# In[10]:


df.head()


# In[11]:


df1.head()


# In[12]:


# Change the name of the 'county' column to 'state'
df1.rename(columns={'ISO3166_2': 'STATE'}, inplace=True)


# In[14]:


df1.head(STATE =='AL')


# In[15]:


df1[df1['STATE'] == 'AL'].head()


# In[16]:


# Merge based on 'state' and 'date'
# Convert 'date' column to datetime64[ns] type
df['DATE'] = pd.to_datetime(df['DATE'])
df1['DATE'] = pd.to_datetime(df1['DATE'])
merged_df = pd.merge(df, df1, on=['STATE', 'DATE'], how='inner')


# In[17]:


merged_df.head()


# In[18]:


merged_df.shape


# In[19]:


new_df = merged_df[['STATE', 'CRITICAL_STAFFING_SHORTAGE_TODAY_YES', 'CRITICAL_STAFFING_SHORTAGE_TODAY_NO', 'CRITICAL_STAFFING_SHORTAGE_TODAY_NOT_REPORTED', 'CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_YES', 'CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_NO', 'CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_NOT_REPORTED', 'HOSPITAL_ONSET_COVID', 'HOSPITAL_ONSET_COVID_COVERAGE', 'INPATIENT_BEDS', 'INPATIENT_BEDS_USED', 'INPATIENT_BEDS_USED_COVID', 'PREVIOUS_DAY_ADMISSION_ADULT_COVID_CONFIRMED', 'PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_CONFIRMED', 'STAFFED_ADULT_ICU_BED_OCCUPANCY', 'STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_AND_SUSPECTED_COVID', 'STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID', 'STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID_COVERAGE', 'TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID', 'TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_COVID', 'TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID', 'TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID_COVERAGE', 'TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_COVID', 'TOTAL_STAFFED_ADULT_ICU_BEDS', 'INPATIENT_BEDS_UTILIZATION', 'INPATIENT_BEDS_UTILIZATION_COVERAGE', 'INPATIENT_BEDS_UTILIZATION_NUMERATOR', 'INPATIENT_BEDS_UTILIZATION_DENOMINATOR', 'PERCENT_OF_INPATIENTS_WITH_COVID', 'PERCENT_OF_INPATIENTS_WITH_COVID_NUMERATOR', 'PERCENT_OF_INPATIENTS_WITH_COVID_DENOMINATOR', 'INPATIENT_BED_COVID_UTILIZATION', 'INPATIENT_BED_COVID_UTILIZATION_NUMERATOR', 'INPATIENT_BED_COVID_UTILIZATION_DENOMINATOR', 'ADULT_ICU_BED_COVID_UTILIZATION', 'ADULT_ICU_BED_COVID_UTILIZATION_NUMERATOR', 'ADULT_ICU_BED_COVID_UTILIZATION_DENOMINATOR', 'ADULT_ICU_BED_UTILIZATION', 'ADULT_ICU_BED_UTILIZATION_NUMERATOR', 'ADULT_ICU_BED_UTILIZATION_DENOMINATOR', 'DATE','POSITIVE', 'NEGATIVE', 'PENDING', 'DEATH', 'HOSPITALIZED', 'HOSPITALIZEDCURRENTLY', 'HOSPITALIZEDCUMULATIVE', 'INICUCURRENTLY', 'INICUCUMULATIVE', 'ONVENTILATORCURRENTLY', 'ONVENTILATORCUMULATIVE'
]]


# In[20]:


new_df.head()


# In[21]:


new_df.info()


# In[22]:


columns_to_drop = ['INICUCUMULATIVE', 'ONVENTILATORCURRENTLY', 'ONVENTILATORCUMULATIVE','PENDING','INPATIENT_BEDS_UTILIZATION_NUMERATOR','INPATIENT_BEDS_UTILIZATION_DENOMINATOR','PERCENT_OF_INPATIENTS_WITH_COVID_NUMERATOR',
'PERCENT_OF_INPATIENTS_WITH_COVID_DENOMINATOR','INPATIENT_BED_COVID_UTILIZATION_NUMERATOR','INPATIENT_BED_COVID_UTILIZATION_DENOMINATOR','ADULT_ICU_BED_COVID_UTILIZATION_NUMERATOR',
'ADULT_ICU_BED_COVID_UTILIZATION_DENOMINATOR','ADULT_ICU_BED_UTILIZATION_NUMERATOR','ADULT_ICU_BED_UTILIZATION_DENOMINATOR']

# Drop the specified columns from the DataFrame
new_df.drop(columns=columns_to_drop, inplace=True)


# In[23]:


new_df.info()


# In[24]:


new_df['HOSPITAL_ONSET_COVID'].fillna(0, inplace=True)


# In[25]:


new_df = new_df[new_df['INPATIENT_BEDS'] != 6.0]


# In[26]:


new_df.dropna(subset=['INPATIENT_BEDS'], inplace=True)


# In[27]:


pd.set_option('display.max_rows', 1000)  
pd.set_option('display.max_columns', None)


# In[28]:


new_df.dropna(subset=['INPATIENT_BEDS_USED'], inplace=True)


# In[29]:


new_df.dropna(subset=['INPATIENT_BEDS_USED_COVID'], inplace=True)


# In[30]:


columns_to_drop = ['PREVIOUS_DAY_ADMISSION_ADULT_COVID_CONFIRMED','PREVIOUS_DAY_ADMISSION_PEDIATRIC_COVID_CONFIRMED']

# Drop the specified columns from the DataFrame
new_df.drop(columns=columns_to_drop, inplace=True)


# In[31]:


new_df.info()


# In[32]:


new_df.head()


# In[33]:


df_max = new_df.groupby(['DATE', 'STATE']).max().reset_index()


# In[34]:


df_max.head(1000)


# In[35]:


df_max.info()


# In[36]:


df_max['DEATH'].fillna(0, inplace=True)


# In[37]:


df_max.dropna(subset=['STAFFED_ADULT_ICU_BED_OCCUPANCY'], inplace=True)


# In[38]:


df_max.dropna(subset=['ADULT_ICU_BED_COVID_UTILIZATION'], inplace=True)


# In[39]:


df_max['POSITIVE'].fillna(0, inplace=True)


# In[40]:


df_max['NEGATIVE'].fillna(0, inplace=True)


# In[41]:


df_max[['HOSPITALIZED', 'HOSPITALIZEDCURRENTLY','HOSPITALIZEDCUMULATIVE']] = df_max[['HOSPITALIZED', 'HOSPITALIZEDCURRENTLY','HOSPITALIZEDCUMULATIVE']].fillna(0)


# In[42]:


df_max['INICUCURRENTLY'].fillna(0, inplace=True)


# In[43]:


df_max.describe()


# In[44]:


import matplotlib.pyplot as plt
import seaborn as sns


# In[45]:


# function to create labeled barplots


def labeled_barplot(data, feature, perc=False, n=None):
    """
    Barplot with percentage at the top

    data: dataframe
    feature: dataframe column
    perc: whether to display percentages instead of count (default is False)
    n: displays the top n category levels (default is None, i.e., display all levels)
    """

    total = len(data[feature])  # length of the column
    count = data[feature].nunique()
    if n is None:
        plt.figure(figsize=(count + 1, 5))
    else:
        plt.figure(figsize=(n + 1, 5))

    plt.xticks(rotation=90, fontsize=15)
    ax = sns.countplot(
        data=data,
        x=feature,
        palette="Paired",
        order=data[feature].value_counts().index[:n].sort_values(),
    )

    for p in ax.patches:
        if perc == True:
            label = "{:.1f}%".format(
                100 * p.get_height() / total
            )  # percentage of each class of the category
        else:
            label = p.get_height()  # count of each level of the category

        x = p.get_x() + p.get_width() / 2  # width of the plot
        y = p.get_height()  # height of the plot

        ax.annotate(
            label,
            (x, y),
            ha="center",
            va="center",
            size=12,
            xytext=(0, 5),
            textcoords="offset points",
        )  # annotate the percentage

    plt.show()  # show the plot


# In[46]:


labeled_barplot(df_max, "STATE", perc=True, n=10)


# In[47]:


dropping_columns = ['CRITICAL_STAFFING_SHORTAGE_ANTICIPATED_WITHIN_WEEK_NO','CRITICAL_STAFFING_SHORTAGE_TODAY_NO','INPATIENT_BEDS_USED',
'STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID','TOTAL_PEDIATRIC_PATIENTS_HOSPITALIZED_CONFIRMED_AND_SUSPECTED_COVID_COVERAGE','TOTAL_STAFFED_ADULT_ICU_BEDS']

# Drop the specified columns from the DataFrame
df_max.drop(columns=dropping_columns, inplace=True)


# In[48]:


dropping_columns = ['STAFFED_ICU_ADULT_PATIENTS_CONFIRMED_COVID_COVERAGE','HOSPITAL_ONSET_COVID_COVERAGE']

# Drop the specified columns from the DataFrame
df_max.drop(columns=dropping_columns, inplace=True)


# In[49]:


df_max.drop(columns='TOTAL_ADULT_PATIENTS_HOSPITALIZED_CONFIRMED_COVID', inplace=True)


# In[50]:


plt.figure(figsize=(15, 7))
sns.heatmap(df_max.corr(), annot=True, vmin=-1, vmax=1, fmt=".2f", cmap="Spectral")
plt.show()


# In[51]:


correlation_matrix = df_max.corr()

# Print the correlation matrix
print(correlation_matrix)


# In[52]:


correlation_matrix_by_state = df_max.groupby('STATE').corr()
print(correlation_matrix_by_state)


# In[53]:


def plot_line_graphs(df_max, state, columns):
    state_data = df_max[df_max['STATE'] == state]

    # Plot line graphs for each specified column
    plt.figure(figsize=(10, 5))
    for column in columns:
        plt.plot(state_data['DATE'], state_data[column], label=column, marker='o')

    plt.title(f'Line Graphs for {state}')
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.show()


# In[54]:


columns_to_plot = ['DEATH', 'HOSPITALIZED', 'INPATIENT_BEDS']
plot_line_graphs(df_max, 'AL', columns_to_plot)


# In[55]:


def plot_line_graphs_all_states(df_max, states, columns):
    # Plot line graphs for each specified column and state
    plt.figure(figsize=(12, 6))
    
    for state in states:
        state_data = df_max[df_max['STATE'] == state]
        for column in columns:
            plt.plot(state_data['DATE'], state_data[column], label=f'{state} - {column}', marker='o')

    plt.title('Line Graphs for All States')
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.show()


# In[56]:


columns_to_plot = ['DEATH']
all_states = df_max['STATE'].unique()
plot_line_graphs_all_states(df_max, all_states, columns_to_plot)


# In[57]:


import plotly.express as px

def plot_line_graphs_all_states_interactive(df_max, states, columns):
    # Use plotly express for interactive plotting
    fig = px.line(df_max, x='DATE', y=columns, color='STATE', markers=True, line_dash='STATE',
                  title='Line Graphs for All States', labels={'value': 'Values'},
                  hover_data={'STATE': True, 'DATE': '|%B %d, %Y'})
    
    fig.update_layout(xaxis_title='Date', yaxis_title='Values')

    fig.show()


# In[58]:


columns_to_plot = ['DEATH']
all_states = df_max['STATE'].unique()
plot_line_graphs_all_states_interactive(df_max, all_states, columns_to_plot)


# In[59]:


df_max.corr()


# In[ ]:




