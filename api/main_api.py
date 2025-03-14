from equipfailpred.inference import make_predictions
from fastapi import FastAPI, HTTPException
from models import ToPred, FetchPred
from equipfailpred import FEATURES
from utils import *
from dbcon import *


app = FastAPI()


@app.post("/predict")
async def makePredictions(data: ToPred) :
    """
    Process incoming data, make predictions,
    and store them in the database.
    """
    df = strto_df(data.df)
    result, prediction_proba = make_predictions(df[FEATURES])
    pred_df = pd.DataFrame(result, columns=TARGET)
    pred = dfto_str(pred_df)
    final_df = format_predictions(
                                df, 
                                result, 
                                data.source)
    try:
        insert_predictions(final_df)
        prediction_proba_df = fillter_prediction_proba(prediction_proba)
        insert_probability(prediction_proba_df)
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
        data_from_table = fetch_past_predictions(
                                                from_datetime, 
                                                to_datetime, 
                                                source
                                                )
        data_str = dfto_str(data_from_table)
        return {"data": data_str}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    