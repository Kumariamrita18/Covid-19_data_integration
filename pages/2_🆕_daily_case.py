import time
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from utils.constants import *

st.set_page_config(page_title="Cases Around The World", page_icon="🌍")

@st.cache_data
def add_comment(data_id, user, content):
    response = requests.post("http://localhost:8000/comments", json={"data_id": data_id, "user": user, "content": content})
    return response.json()

st.sidebar.markdown("# Add Comment")
user = st.sidebar.text_input("User")
content = st.sidebar.text_area("Comment")

def get_daily_cases(country):
    response = requests.get(f"http://localhost:8000/daily-cases?country={country}")
    return response.json()

def get_comments():
    response = requests.get(f"http://localhost:8000/comments?data_id=8")
    return response.json()

st.markdown("# Daily Cases")
country = st.selectbox("Country", LIST_OF_COUNTRIES, index=LIST_OF_COUNTRIES.index("New Zealand")) # default to New Zealand

json_data = get_daily_cases(country)
df = pd.DataFrame(json_data)
fig = px.bar(df, x="date", y="difference", color="case_type", barmode="group")
st.plotly_chart(fig)

    
if st.sidebar.button("Submit"):
    response = add_comment(8, user, content)
    st.sidebar.markdown(f"### {response['message']}")
    
st.markdown("## Comments")
comments = get_comments()
for comment in comments:
    st.markdown(f"### {comment['user']}")
    st.markdown(f"{comment['content']}")