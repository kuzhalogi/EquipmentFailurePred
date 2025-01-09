# from io import StringIO
import streamlit as st
import requests
from datetime import datetime, date, time 
from helper import *


def past_prediction():
    st.header("Fetch Data")
    
    # Input for From Date & Time
    from_date = st.date_input("From Date", value=date.today())
    from_time = st.time_input("From Time", value=datetime.now().time())
    from_datetime = datetime.combine(from_date, from_time).strftime("%Y-%m-%d %H:%M:%S")
    
    
    # Input for To Date & Time
    to_date = st.date_input("To Date", value=date.today())
    to_time = st.time_input("To Time", value=datetime.now().time())
    to_datetime = datetime.combine(to_date, to_time).strftime("%Y-%m-%d %H:%M:%S")
    
    source_fetch = st.selectbox(
        "Select source for fetching data", ["webapp", "scheduler","all"], 
        key="source_fetch"
        )
    
    if st.button("Fetch Data"):
        
        pay_load = { "from_date":from_datetime,
                     "to_date":to_datetime  ,
                     "source":source_fetch }
        
        response = requests.post(DB_API_URL,json=pay_load)
        
        if response.status_code == 200:
            result = response.json()['data']
            st.success("Done!")
            pred_df = to_df(result)
            st.write(pred_df)
        else:
            st.error("Failed to get predictions")
