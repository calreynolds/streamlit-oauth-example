from dash import Dash, dcc
import dash_mantine_components as dmc
from flask import request, Response, stream_with_context
import dash
import threading


app = Dash(__name__, use_pages=True)
server = app.server

from components import (
    LEFT_SIDEBAR,
    FOOTER,
)  # noqa: E402 isort:skip - must be imported after app is defined

app.layout = dmc.MantineProvider(
    withGlobalStyles=True,
    theme={
        "primaryColor": "dbx-orange",
        "colors": {
            "dbx-orange": [
                "#FFB4AC",
                "#FFB4AC",
                "#FFB4AC",
                "#FFB4AC",
                "#FF9B90",
                "#FF8174",
                "#FF6859",
                "#FF4F3D",
                "#FF3621",
            ]
        },
    },
    children=[
        dmc.Grid(
            m=0,
            children=[
                dmc.Col(
                    LEFT_SIDEBAR,
                    span=2,
                    style={"backgroundColor": "#303F47"},
                    p=0,
                ),
                dmc.Col(
                    dash.page_container,
                    className="page",
                    span=10,
                    p=20,
                ),
            ],
        ),
        FOOTER,
        dcc.Store(
            id="general-store", data={"outputdpdn2": "main.delta_optimizer_mercury"}
        ),
    ],
)


import asyncio
import json
from flask import Response
import websockets

URI = "wss://campaigns-orientation-talk-gasoline.trycloudflare.com/api/v1/stream"


async def run(question):
    # Note: the selected defaults change from time to time.
    prompt = f"""Given the following SQL database metadata, write an SQL query that answers the user's text question.\nSQL database metadata:
- date: dateInt INT, CalendarDate DATE PK, CalendarYear INT, CalendarMonth STRING, MonthOfYear INT, CalendarDay STRING, DayOfWeek INT, DayOfWeekStartMonday INT, IsWeekDay STRING, DayOfMonth INT, IsLastDayOfMonth STRING, DayOfYear INT, WeekOfYearIso INT, QuarterOfYear INT,- paymenttype - payment_type STRING PK, payment_desc STRING
- ratecode - RateCodeID STRING PK, RateCodeDesc STRING
- rawyellowtrips - VendorID STRING, pickupDate DATE (FK date CalendarDate), pickupHour INT, pickup TIMESTAMP, dropoff TIMESTAMP, trip_duration_minutes DECIMAL(27,6), store_and_fwd_flag STRING, RatecodeID STRING (FK ratecode RateCodeID), PULocationID INT, DOLocationID INT, payment_type STRING (FK paymenttype payment_type), passenger_count INT, trip_distance FLOAT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, total_amount FLOAT
- taxizone - LocationID STRING PK, Borough STRING, Zone STRING, service_zone STRING, latitude STRING, longitude STRING, Lat_Long STRING, latitude_dbl STRING, longitude_dbl STRING

User text question:{question}

Response:
```"""
    #     instruction = """- date: dateInt INT, CalendarDate DATE PK, CalendarYear INT, CalendarMonth STRING, MonthOfYear INT, CalendarDay STRING, DayOfWeek INT, DayOfWeekStartMonday INT, IsWeekDay STRING, DayOfMonth INT, IsLastDayOfMonth STRING, DayOfYear INT, WeekOfYearIso INT, QuarterOfYear INT,
    # - paymenttype - payment_type STRING PK, payment_desc STRING
    # - ratecode - RateCodeID STRING PK, RateCodeDesc STRING
    # - rawyellowtrips - VendorID STRING, pickupDate DATE (FK date CalendarDate), pickupHour INT, pickup TIMESTAMP, dropoff TIMESTAMP, trip_duration_minutes DECIMAL(27,6), store_and_fwd_flag STRING, RatecodeID STRING (FK ratecode RateCodeID), PULocationID INT, DOLocationID INT, payment_type STRING (FK paymenttype payment_type), passenger_count INT, trip_distance FLOAT, fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT, total_amount FLOAT
    # - taxizone - LocationID STRING PK, Borough STRING, Zone STRING, service_zone STRING, latitude STRING, longitude STRING, Lat_Long STRING, latitude_dbl STRING, longitude_dbl STRING
    # """
    #     prompt = f"Given the following SQL database metadata, write an SQL query that answers the user's text question.\nSQL database metadata:\n{instruction}\n\nUser text question:\n{question}\n\nResponse:\n```"

    request = {
        "prompt": prompt,
        "max_new_tokens": 250,
        "do_sample": True,
        "temperature": 1.5,
        "top_p": 0.1,
        "typical_p": 1,
        "repetition_penalty": 1,
        "top_k": 40,
        "min_length": 0,
        "no_repeat_ngram_size": 0,
        "num_beams": 1,
        "penalty_alpha": 0,
        "length_penalty": 1,
        "early_stopping": True,
        "seed": 42,
        "add_bos_token": True,
        "truncation_length": 2048,
        "ban_eos_token": False,
        "skip_special_tokens": True,
        "stopping_strings": ["`", "#", "Response", "Note", "</", ";", "; ", "`", "```"],
    }

    async with websockets.connect(URI, ping_interval=None) as websocket:
        await websocket.send(json.dumps(request))

        while True:
            incoming_data = await websocket.recv()
            incoming_data = json.loads(incoming_data)

            if incoming_data["event"] == "text_stream":
                yield incoming_data["text"]
            else:
                return


def thread_run(loop, context):
    async_gen = run(context)
    while True:
        try:
            # use run_coroutine_threadsafe to get a result from
            # an async function from within a new thread
            future = asyncio.run_coroutine_threadsafe(async_gen.__anext__(), loop)
            yield future.result()
        except StopAsyncIteration:
            break


@app.server.route("/dbx-stream", methods=["POST"])
def streaming_chat():
    prompt = request.json["question"]

    # create new event loop in this thread
    new_loop = asyncio.new_event_loop()

    # run new event loop in a separate thread
    threading.Thread(target=new_loop.run_forever).start()

    # return a streaming response
    return Response(
        stream_with_context(thread_run(new_loop, prompt)), mimetype="text/plain"
    )


if __name__ == "__main__":
    app.run(debug=True)
