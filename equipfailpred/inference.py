import pandas as pd
import numpy as np
from equipfailpred.preprocess import preprocessor, predict


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    """
    Process input data and make predictions using the trained model.
    """
    processed_data = preprocessor(input_data, False)
    predictions, prediction_probalistics = predict(processed_data)
    return predictions, prediction_probalistics
