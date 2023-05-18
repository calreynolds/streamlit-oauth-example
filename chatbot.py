import dash
from dash import (
    html,
    dcc,
    Input,
    Output,
    State,
    callback,
    clientside_callback,
    ClientsideFunction,
)
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from flask import request, Response, stream_with_context
import dash_ag_grid as dag
import os

import pandas as pd
from databricks import sql


def dbx_SQL_query(query):
    # with sql.connect(
    #     server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    #     http_path=os.getenv("DATABRICKS_HTTP_PATH"),
    #     access_token=os.getenv("DATABRICKS_TOKEN"),
    # ) as connection:
    with sql.connect(
        server_hostname=os.getenv(
            "DATABRICKS_SERVER_HOSTNAME", "plotly-customer-success.cloud.databricks.com"
        ),
        http_path=os.getenv(
            "DATABRICKS_HTTP_PATH",
            "sql/protocolv1/o/4090279629911309/0516-185422-xj7o3il3",
        ),
        access_token=os.getenv(
            "DATABRICKS_TOKEN", "dapi096678ebaf3ce5533425ee71162cc1b3"
        ),
    ) as connection:
        try:
            with connection.cursor() as cursor:
                # Execute the SQL query
                cursor.execute(query)
                result = cursor.fetchall()

                columns = [column[0] for column in cursor.description]
                df = pd.DataFrame(result, columns=columns)

                if df.empty:
                    print(f"No results returned from query")
                    return False
        except:
            print(f"Error executing SQL query")
            return False
    return df


def rcw_message(message, type):
    if type == "rcw":
        return html.Div(
            [
                html.Img(
                    src="https://agao00.github.io/static/media/dblogo.56ac0149.jpg",
                    className="rcw-avatar",
                ),
                html.Div(
                    message,
                    className="rcw-response",
                    id="rcw-response",
                ),
            ],
            className="rcw-message rcw-message-response",
        )
    return html.Div(
        html.Div(
            message,
            className="client-question",
            id="client-question",
        ),
        className="rcw-message",
    )


CHAT_AFFIX = dmc.Affix(
    dmc.Tooltip(
        position="right",
        label="Ask the Databricks AI assistant about your data.",
        withArrow=True,
        children=dmc.ActionIcon(
            size=50,
            variant="filled",
            id="chat-affix-modal",
            n_clicks=0,
            mb=10,
            radius="xl",
            children=DashIconify(
                icon="mdi:chat-plus-outline",
                width=30,
                color="rgb(255 54 33)",
                id="chat-affix-icon",
            ),
        ),
    ),
    position={"bottom": 20, "left": 20},
)


CHAT_MODAL = dmc.Affix(
    position={"bottom": 100, "left": 20},
    id="affix-chat-modal",
    className="hide",
    children=html.Div(
        className="modal-card",
        children=[
            html.Div("Databricks AI Assistant", className="card-title"),
            html.Div(
                className="messages",
                children=[
                    rcw_message(
                        "",
                        "user",
                    ),
                    rcw_message(
                        "Ask me a question!",
                        "rcw",
                    ),
                    dag.AgGrid(  # https://dashaggrid.pythonanywhere.com/getting-started/quickstart
                        id="rcw-output-table",
                        defaultColDef={
                            "resizable": True,
                            "sortable": True,
                            "filter": True,
                        },
                        columnSize="sizeToFit",
                        className="ag-theme-balham hide",
                    ),
                    dmc.Tooltip(
                        position="right",
                        label="Waitin for SQL query to return...",
                        withArrow=True,
                        children=dmc.Loader(
                            id="table-loader",
                            color="orange",
                            size="md",
                            variant="bars",
                            className="hide",
                        ),
                    ),
                ],
            ),
            html.Div(
                className="rcw-sender",
                children=[
                    dcc.Input(
                        placeholder="Question regarding your DBX data...",
                        debounce=True,
                        id="input-question",
                        className="rcw-new-message",
                    ),
                    dmc.ActionIcon(
                        DashIconify(
                            icon="tabler:send",
                            height=20,
                            style={"cursor": "pointer"},
                        ),
                        size="lg",
                        variant="filled",
                        id="rcw-send",
                    ),
                ],
            ),
            html.Div(id="dummy-output", className="hide"),
        ],
    ),
)


def component_chatbot():
    return html.Div(
        children=[
            CHAT_AFFIX,
            CHAT_MODAL,
        ]
    )


from dash_spa import DashSPA, page_container, register_page

app = DashSPA(__name__, pages_folder="")

# app = dash.Dash(__name__)
app.layout = html.Div(children=[component_chatbot()])


@callback(
    Output("affix-chat-modal", "className"),
    Output("chat-affix-icon", "icon"),
    Input("chat-affix-modal", "n_clicks"),
)
def open_chat_modal(n_clicks):
    if n_clicks % 2 == 0:
        return "hide", "mdi:chat-plus-outline"
    else:
        return "show", "mdi:close"


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
    # return Response(response_stream(), mimetype="text/response-stream")


@callback(
    Output("rcw-output-table", "columnDefs"),
    Output("rcw-output-table", "rowData"),
    Output("rcw-output-table", "className", allow_duplicate=True),
    Output("table-loader", "className", allow_duplicate=True),
    Input("dummy-output", "children"),
    prevent_initial_call=True,
)
def update_table(sql_query):
    print(sql_query)
    if sql_query:
        df = dbx_SQL_query(sql_query)
        print(df)
        if df is False:
            return [], [], "ag-theme-balham hide", "hide"
        columnDefs = [{"field": i, "headerName": i} for i in df.columns]
        rowData = df.to_dict("records")
        print(columnDefs, rowData)
        return columnDefs, rowData, "ag-theme-balham show", "hide"
    return [], [], "ag-theme-balham hide", "hide"


clientside_callback(
    ClientsideFunction(
        namespace="clientside", function_name="data_streaming_OpenAI_flask_API"
    ),
    Output("rcw-send", "loading", allow_duplicate=True),
    Output("dummy-output", "children"),
    Output("table-loader", "className"),
    Input("rcw-send", "n_clicks"),
    State("input-question", "value"),
    prevent_initial_call=True,
)


clientside_callback(
    """
    function updateLoadingState(n_clicks) {
        return [ true, "ag-theme-balham hide" ];
    }
    """,
    Output("rcw-send", "loading"),
    Output("rcw-output-table", "className", allow_duplicate=True),
    Input("rcw-send", "n_clicks"),
    prevent_initial_call=True,
)

clientside_callback(
    """
    function updateQuestionFields(n_clicks, question) {
        return ["", question, "client-question blue-bg"];
    }
    """,
    Output("input-question", "value"),
    Output("client-question", "children"),
    Output("client-question", "className"),
    Input("rcw-send", "n_clicks"),
    State("input-question", "value"),
    prevent_initial_call=True,
)

if __name__ == "__main__":
    app.run_server(debug=True)
