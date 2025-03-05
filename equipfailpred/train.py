from sklearn.ensemble import RandomForestClassifier
import joblib
import pandas as pd
import numpy as np
from equipfailpred import MODEL_PATH
from equipfailpred.preprocess import preprocessor,predict,to_oned
from equipfailpred.preprocess import compute_accuracy,selected_split


def model_fit(X_train: pd.DataFrame, y_train: np.ndarray) -> None:
    model = RandomForestClassifier(random_state=42)
    fitted_model = model.fit(X_train, y_train)
    joblib.dump(fitted_model, MODEL_PATH)
    return None


def model_train(data: pd.DataFrame) -> pd.DataFrame:
    X_train, X_test, y_train, y_test = selected_split(data)
    X_train_processed = preprocessor(X_train, True)
    y_flaten = to_oned(y_train)
    model_fit(X_train_processed, y_flaten)
    return X_test, y_test


def model_eval(X_test: pd.DataFrame, y_test: np.ndarray) -> dict:
    X_test_processed = preprocessor(X_test, False)
    predictions_test = predict(X_test_processed)
    y_flaten = to_oned(y_test)
    scores = compute_accuracy(y_flaten, predictions_test)
    return scores


def build_model(data: pd.DataFrame) -> dict:
    X_test, y_test = model_train(data)
    model_score = model_eval(X_test, y_test)
    return model_score
