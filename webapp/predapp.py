import streamlit as st
from make_pred import manual_pred
from past_prediction import past_prediction


def main():
    st.set_page_config(page_title="Equipment failure predictor")
    page = st.sidebar.radio("Select a page", ["Predict manually","Get Predictions"])
    if page == "Predict manually":
        manual_pred()
    elif page == "Get Predictions":
        past_prediction()


if __name__ == "__main__":
    main()
