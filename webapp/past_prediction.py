# from io import StringIO
import streamlit as st
import requests
from datetime import date
from helper import *


def past_prediction():
    st.header("Fetch Data")
    from_date = st.date_input("From Date", value=date.today())
    to_date = st.date_input("To Date", value=date.today())
    from_date = from_date.strftime("%Y-%m-%d")
    to_date = to_date.strftime("%Y-%m-%d")
    source_fetch = st.selectbox("Select source for fetching data", ["webapp", "scheduler","all"], key="source_fetch")
    if st.button("Fetch Data"):
        response = requests.post(DB_API_URL,json={"from_date":from_date,"to_date":to_date,"source":source_fetch})
        if response.status_code == 200:
            result = response.json()['data']
            st.success("Done!")
            pred_df = to_df(result)
            st.write(pred_df)
        else:
            st.error("Failed to get predictions")
