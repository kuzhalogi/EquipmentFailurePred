NUMERICAL = ['Air temperature [K]', 
             'Process temperature [K]', 
             'Rotational speed [rpm]', 
             'Torque [Nm]', 
             'Tool wear [min]']
ORDINAL = ['Type']
FEATURES = NUMERICAL + ORDINAL
TARGET = ['Machine failure', 'TWF', 'HDF', 'PWF', 'OSF', 'RNF']

MODEL_PATH = "../models/rfmodel.joblib"
SCALER_PATH = "../models/scaler.joblib"
LENCODER_PATH = "../models/lencoder.joblib"
