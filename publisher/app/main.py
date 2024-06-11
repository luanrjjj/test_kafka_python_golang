import time
import json
import requests
import asyncio
from requests.structures import CaseInsensitiveDict

from app.core.gateways.kafka import Kafka
from app.dependencies.kafka import get_kafka_instance
from app.enum import EnvironmentVariables
from app.routers import publisher

from fastapi import BackgroundTasks, FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title='Kafka Publisher API')
kafka_server = Kafka(
    topic=EnvironmentVariables.KAFKA_TOPIC_NAME.get_env(),
    port=EnvironmentVariables.KAFKA_PORT.get_env(),
    servers=EnvironmentVariables.KAFKA_SERVER.get_env(),
)

@app.on_event("startup")
async def startup_event():
    await kafka_server.aioproducer.start()

@app.on_event("shutdown")
async def shutdown_event():
    await kafka_server.aioproducer.stop()

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

@app.get('/')
def get_root():     
    return {'message': 'Financial API is Running...'}

@app.get('/financial_data/{symbol}')
def get_financial_data(symbol: str, background_tasks: BackgroundTasks):
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + symbol + "&apikey=demo"
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    
    response = requests.get(url, headers=headers)

    financial_data = response.json()

    if 'Time Series (Daily)' not in financial_data:
        raise HTTPException(status_code=404, detail="Item not found")

    time_series = financial_data['Time Series (Daily)']
    streaming_data(time_series, background_tasks)

    return financial_data

def streaming_data(data, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_to_kafka, data)
        
async def send_to_kafka(data):
    for each_day in data:
        await asyncio.sleep(1)
        stock_price_by_day = {
            'date': each_day,
            'data': data[each_day]
        }
        print(object)
        print("", data[each_day])
        await kafka_server.aioproducer.send_and_wait(
            topic=EnvironmentVariables.KAFKA_TOPIC_NAME.get_env(),
            value=json.dumps(stock_price_by_day).encode('utf-8')
        )

