from dash import Dash, dcc
import dash_mantine_components as dmc
from flask import request, Response, stream_with_context
import dash


app = Dash(__name__, use_pages=True)
server = app.server

from components import (
    LEFT_SIDEBAR,
    # FOOTER,
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
        # FOOTER,
        dcc.Store(
            id="general-store", data={"outputdpdn2": "main.delta_optimizer_mercury"}
        ),
    ],
)


@app.server.route("/dbx-stream", methods=["POST"])
def streaming_chat():
    prompt = request.json["question"]

    import time  # TODO: remove this

    def send_messages(prompt):
        return "hello"  # TODO: add streaming chat from Dolly here
        # return openai.ChatCompletion.create(
        #     model="gpt-3.5-turbo",
        #     messages=prompt,
        #     stream=True,
        #     max_tokens=2024,
        #     temperature=0,
        # )

    def generate_content():
        response = "SELECT payment_desc, COUNT(*) FROM rawyellowtrips JOIN paymenttype ON paymenttype.payment_type = rawyellowtrips.payment_type GROUP BY paymenttype.payment_type, payment_desc"
        for i in response:
            yield i
            time.sleep(0.01)

    def response_stream():
        yield from (
            line.choices[0].delta.get("content", "") for line in send_messages(prompt)
        )

    content_generator = generate_content()
    return Response(stream_with_context(content_generator), mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True)
