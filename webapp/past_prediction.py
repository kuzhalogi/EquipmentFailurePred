# from io import StringIO
import streamlit as st
import requests
from datetime import datetime, date, time 
from helper import *
from webapp_config import DB_API_ENDPOINT


def past_prediction():
    st.header("Fetch Data")
    
    # Input for From Date & Time
    from_date = st.date_input("From Date")
    from_time = st.time_input("From Time")
    from_datetime = datetime.combine(from_date, from_time).strftime("%Y-%m-%d %H:%M:%S")
    st.write(from_datetime)
    
    # Input for To Date & Time
    to_date = st.date_input("To Date")
    to_time = st.time_input("To Time")
    to_datetime = datetime.combine(to_date, to_time).strftime("%Y-%m-%d %H:%M:%S")
    st.write(to_datetime)
    
    source_fetch = st.selectbox(
        "Select source for fetching data", ["webapp", "scheduler","all"], 
        key="source_fetch"
        )
    
    if st.button("Fetch Data"):
        st.write(source_fetch) 
        pay_load = { "from_datetime":from_datetime,
                     "to_datetime":to_datetime  ,
                     "source":source_fetch }
        
        response = requests.post(DB_API_ENDPOINT,json=pay_load)
        
        if response.status_code == 200:
            result = response.json()['data']
            st.success("Done!")
            pred_df = to_df(result)
            st.write("Check the prediction Column: 0 means machine is good, 1 means machine Failure")
            st.write(pred_df)
        else:
            st.error("Failed to get predictions")
