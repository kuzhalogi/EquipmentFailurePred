from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier
import joblib
import pandas as pd
import numpy as np
from equipfailpred.model_config import MODEL_PATH
from equipfailpred.preprocess import preprocessor, predict,to_oned
from equipfailpred.preprocess import compute_accuracy, selected_split


def model_fit(X_train: pd.DataFrame, y_train: np.ndarray) -> None:
    """
    Train a RandomForestClassifier wrapped in a MultiOutputClassifier on the provided training data.

    Args:
    X_train: pd.DataFrame
        The features of the training dataset.
    y_train: np.ndarray
        The multi-output target labels of the training dataset.

    Returns:
    None
    """
    model = RandomForestClassifier(random_state=42)
    multi_target_model = MultiOutputClassifier(model)
    fitted_model = multi_target_model.fit(X_train, y_train)
    joblib.dump(fitted_model, MODEL_PATH)
    return None


def model_train(data: pd.DataFrame) -> pd.DataFrame:
    """
    Split the input data into training and testing sets, preprocess the training data, 
    and fit a multi-output model using the training set.

    Args:
    data: pd.DataFrame
        The complete dataset containing both features and target labels.

    Returns:
    X_test: pd.DataFrame
        The features of the test dataset.
    y_test: np.ndarray
        The multi-output target labels of the test dataset.
    """
    X_train, X_test, y_train, y_test = selected_split(data)
    X_train_processed = preprocessor(X_train, True)
    # y_flaten = to_oned(y_train)
    y_train_flattened = y_train.values  
    # model_fit(X_train_processed, y_flaten)
    model_fit(X_train_processed, y_train_flattened)
    return X_test, y_test


def model_eval(X_test: pd.DataFrame, y_test: np.ndarray) -> dict:
    """
    Preprocess the test data, make predictions using the trained model, 
    and compute various classification metrics to evaluate the model.

    Args:
    X_test: pd.DataFrame
        The features of the test dataset.
    y_test: np.ndarray
        The multi-output target labels of the test dataset.

    Returns:
    dict: 
        A dictionary containing accuracy, precision, recall, and AUC scores.
    """
    X_test_processed = preprocessor(X_test, False)
    predictions_test = predict(X_test_processed)
    # y_flaten = to_oned(y_test)
    y_test_flattened = y_test.values  
    # scores = compute_accuracy(y_flaten, predictions_test)
    scores = compute_accuracy(y_test_flattened, predictions_test)
    return scores


def build_model(data: pd.DataFrame) -> dict:
    """
    Train and evaluate the model on the provided dataset.

    Args:
    data: pd.DataFrame
        The complete dataset containing both features and target labels.

    Returns:
    dict: 
        A dictionary containing evaluation metrics such as accuracy, precision, recall, and AUC.
    """
    X_test, y_test = model_train(data)
    model_score = model_eval(X_test, y_test)
    return model_score
