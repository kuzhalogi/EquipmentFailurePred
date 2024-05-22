from fastapi import FastAPI, HTTPException
from equipfailpred import FEATURES
from equipfailpred.inference import make_predictions
from utils import *
from models import ToPred, FetchPred
from dbcon import *
import datetime

app = FastAPI()
df = pd.DataFrame


@app.post("/predict")
async def makePredictions(data: ToPred) :
    df = to_df(data.df)
    result = make_predictions(df[FEATURES])
    pred = ar_tostr(result)
    final_df = df[COLM_ORDER].copy()
    final_df['Predictions'] = result        
    current_date = datetime.date.today()
    final_df['date'] = current_date.strftime("%Y-%m-%d")  
    final_df['source'] = data.source
    
    final_df = final_df.rename(columns={
            'Product ID': 'product_id',
            'Air temperature [K]': 'air_temperature_k',
            'Process temperature [K]': 'process_temperature_k',
            'Rotational speed [rpm]': 'rotational_speed_rpm',
            'Torque [Nm]': 'torque_nm',
            'Tool wear [min]': 'tool_wear_min',
            'Type': 'type',
            'Predictions': 'prediction'
        })
    print(final_df)
    try:
        final_df.to_sql(TABLE, engine, if_exists='append', index=False)
        message = "Prediction inserted successfully"
        return {"message": message,"pred":pred}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/past-predictions")
async def get_data(data: FetchPred):
    print(f"recived{data.from_date}|{data.to_date}|{data.source}")
    query=f"""SELECT *
            FROM prediction
            WHERE date >= '{data.from_date}' -- From Date
            AND date <= '{data.to_date}' -- To Date
            AND source = '{data.source}';
            """
    data_from_table = pd.read_sql(query, engine)
    data_str = to_str(data_from_table)
    return {"data":data_str}
        