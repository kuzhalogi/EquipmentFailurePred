import streamlit as st
import pandas as pd
from helper import *

def single_prediction():
    st.title("Prediction Page")
    st.subheader("Single Sample Prediction")
    product_id = st.text_input("Product ID",placeholder='H12345')
    type_value = st.selectbox("Type", options=["L", "M", "H"])
    air_temp = st.number_input("Air temperature [K]", value=0.0)
    process_temp = st.number_input("Process temperature [K]", value=0.0)
    rotational_speed = st.number_input("Rotational speed [rpm]", value=0)
    torque = st.number_input("Torque [Nm]", value=0.0)
    tool_wear = st.number_input("Tool wear [min]", value=0)
    
    data = [
        str(product_id),
        float(air_temp),
        float(process_temp),
        int(rotational_speed),
        float(torque),
        int(tool_wear),
        type_value]
    
    df = pd.DataFrame([data],columns=COLNAME)
    st.write(df)
    if st.button("Predict Single Sample"):
        result = callmodel(df)
        final_df = add_prediction_column_to_df(df,result)
        st.write(final_df)
            

def multi_predictions():
    st.title('Predict tool failure')
    st.header('Sample Prediction')
    df = pd.DataFrame()    
    uploaded_file = st.file_uploader('Upload CSV file')

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write('Shape of the .CSV file',df.shape)
        st.write('data:',df)
        
    if st.button('Predict'):
        if uploaded_file is not None:
            result = callmodel(df)
            final_df = add_prediction_column_to_df(df,result)
            st.write('Shape of the results',final_df.shape)
            st.write(final_df)
        else:
            st.error('File not detected, Please upload a valid .csv file')


def manual_pred():
    userpick=st.radio(label='Pick', options=['Single Prediction','CSV file'])
    if userpick == 'Single Prediction':
        single_prediction()
    else:
        multi_predictions()
        
        