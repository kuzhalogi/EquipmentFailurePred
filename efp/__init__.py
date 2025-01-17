NUMERICAL = ['Air temperature [K]', 
             'Process temperature [K]', 
             'Rotational speed [rpm]', 
             'Torque [Nm]', 
             'Tool wear [min]']
ORDINAL = ['Type']
FEATURES = NUMERICAL + ORDINAL
TARGET = ['Target']

MODEL_PATH = "../models/rfmodel.joblib"
SCALER_PATH = "../models/scaler.joblib"
LENCODER_PATH = "../models/lencoder.joblib"
