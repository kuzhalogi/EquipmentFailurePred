from equipfailpred.inference import make_predictions
from fastapi import FastAPI, HTTPException
from models import ToPred, FetchPred
from equipfailpred import FEATURES
from utils import *
from dbcon import *


app = FastAPI()


@app.post("/predict")
async def makePredictions(data: ToPred) :
    """Process incoming data, make predictions, and store them in the database."""
    df = to_df(data.df)
    result, prediction_proba = make_predictions(df[FEATURES])
    pred = ar_tostr(result)
    final_df = format_predictions(df, result, prediction_proba, data.source)
    try:
        insert_predictions(final_df)
        failure_mode_df = detect_failure_modes(final_df)
        insert_failure_modes(failure_mode_df)
        message = "Prediction inserted successfully"
        return {"message": message,"pred":pred}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/past-predictions")
async def get_data(data: FetchPred):
    """Get predictions from the database"""
    from_datetime = data.from_datetime
    to_datetime = data.to_datetime
    source = data.source
    try:
        data_from_table = fetch_past_predictions(from_datetime, to_datetime, source)
        data_str = to_str(data_from_table)
        return {"data": data_str}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    