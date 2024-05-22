import pandas as pd
import numpy as np
from equipfailpred.preprocess import preprocessor, predict


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    processed_data = preprocessor(input_data, False)
    predictions = predict(processed_data)
    return predictions
