import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
from equipfailpred import FEATURES, TARGET, NUMERICAL, ORDINAL
from equipfailpred import SCALER_PATH, MODEL_PATH, LENCODER_PATH

 
def selected_split(data: pd.DataFrame) -> pd.DataFrame:
    X = data[FEATURES]
    y = data[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test


def to_oned(data: pd.DataFrame)-> np.array:
    nparr = np.array(data)
    oneD = nparr.ravel()
    return oneD


def scale(data: pd.DataFrame, to_train: bool) -> np.ndarray:
    if to_train:
        scaler = StandardScaler()
        fitted_scaler = scaler.fit(data[NUMERICAL])
        joblib.dump(fitted_scaler, SCALER_PATH)
        scaled_set = fitted_scaler.transform(data[NUMERICAL])
    else:
        joblib_scaler = joblib.load(SCALER_PATH)
        scaled_set = joblib_scaler.transform(data[NUMERICAL])
    return scaled_set


def lencode(data: pd.DataFrame, to_train: bool) -> np.ndarray:
    flaten_data = to_oned(data[ORDINAL])
    if to_train:
        l_encoder = LabelEncoder()
        fitted_lencoder = l_encoder.fit(flaten_data)
        joblib.dump(fitted_lencoder, LENCODER_PATH)
        encoded_set = fitted_lencoder.transform(flaten_data)
    else:
        joblib_encoder = joblib.load(LENCODER_PATH)
        encoded_set = joblib_encoder.transform(flaten_data)
    return encoded_set


def preprocessor(data: pd.DataFrame, to_train: bool) -> pd.DataFrame:
    numerical_values = scale(data, to_train)
    categorical_values = lencode(data, to_train)
    scaled_df = pd.DataFrame(numerical_values, columns=NUMERICAL)
    labeled_df = pd.DataFrame(categorical_values, columns=ORDINAL)
    processed_data = pd.concat([scaled_df, labeled_df], axis=1)
    return processed_data


def predict(X: pd.DataFrame) -> np.ndarray: 
    model = joblib.load(MODEL_PATH)
    predictions = model.predict(X)
    predictions = np.around(predictions, 3)
    return predictions


def compute_accuracy(y_test: pd.DataFrame, y_pred:pd.DataFrame)-> dict:
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred)
    scores = {'Accuracy':accuracy, 'Precision':precision, 'Recall':recall, 'Auc':auc}
    return scores
