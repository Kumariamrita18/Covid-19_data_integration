import streamlit as st
import time
import numpy as np
import pandas as pd
import plotly.express as px
import requests

st.set_page_config(page_title="Cases Around The World", page_icon="🌍")

@st.cache_data
def add_comment(data_id, user, content):
    response = requests.post("http://localhost:8000/comments", json={"data_id": data_id, "user": user, "content": content})
    return response.json()

st.sidebar.markdown("# Add Comment")
user = st.sidebar.text_input("User")
content = st.sidebar.text_area("Comment")

@st.cache_data
def get_cases(date):
    response = requests.get(f"http://localhost:8000/cases-lat-long?date={date}")
    return response.json()

def get_comments():
    response = requests.get(f"http://localhost:8000/comments?data_id=7")
    return response.json()

st.markdown("# Cases Around The World")

date = st.date_input("Date", value=pd.to_datetime("2022-12-15"), min_value=pd.to_datetime("2020-01-22"), max_value=pd.to_datetime("2023-5-15")) 

json_data = get_cases(date)
df = pd.DataFrame(json_data)

fig = px.scatter_geo(df, lat="lat", lon="long", color="country", size="cases", projection="natural earth")
fig.update_layout(showlegend=False)  
st.plotly_chart(fig)
    
st.markdown("## Comments")
comments = get_comments()
for comment in comments:
    st.markdown(f"### {comment['user']}")
    st.markdown(f"{comment['content']}")


if st.sidebar.button("Submit"):
    response = add_comment(7, user, content)
    st.sidebar.markdown(f"### {response['message']}")

    