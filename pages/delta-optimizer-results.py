import dash
import json
import requests
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/optimizer-results", title="Results")

SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
WAREHOUSE_ID = "f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"


big_engine = create_engine(
    f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
)


tables_stmt = f"SELECT table_catalog, table_schema,table_name, created, created_by, last_altered, last_altered_by FROM {CATALOG}.INFORMATION_SCHEMA.TABLES;"
tables_in_db = pd.read_sql_query(tables_stmt, big_engine)
schemas_init_statment = f"SELECT schema_name FROM {CATALOG}.{SCHEMA}.SCHEMATA ORDER BY created DESC;"  # ORDER BY created DESC
schema_list = pd.read_sql_query(schemas_init_statment, big_engine)
schema_select_data = [
    {"label": c, "value": c} for c in schema_list.schema_name.unique()
]


def layout():
    return html.Div(
        [
            dmc.Select(
                label="Select Output DB",
                placeholder="Select one",
                id="output-db-select",
                data=schema_select_data,
            ),
            html.Div(id="result-page-layout"),
            component_chatbot(),
        ]
    )


@callback(
    Output("result-page-layout", "children"),
    Input("output-db-select", "value"),
    prevent_initial_call=True,
)
def create_dynamic_results_layout(selected_db):
    results_engine = create_engine(
        f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={selected_db}"
    )
    get_optimizer_results = f"Select * FROM {selected_db}.optimizer_results"
    optimizer_results = pd.read_sql_query(get_optimizer_results, results_engine)
    get_results_stats = f"Select * FROM {selected_db}.all_tables_table_stats"
    results_stats = pd.read_sql_query(get_results_stats, results_engine)
    get_cardinality = f"Select * FROM {selected_db}.all_tables_cardinality_stats WHERE IsUsedInReads = 1 OR IsUsedInWrites = 1"
    cardinality_stats = pd.read_sql_query(get_cardinality, results_engine)
    get_raw_queries = f"SELECT * from_unixtime(query_start_time_ms/1000) AS QueryStartTime, from_unixtime(query_end_time_ms/1000) AS QueryEndTime, duration/1000 AS QueryDurationSeconds FROM {selected_db}.raw_query_history_statistics"

    optimizer_results_columnDefs = [
        {
            "headerName": x,
            "field": x,
            "filter": True,
            "floatingFilter": True,
            "filterParams": {"buttons": ["apply", "reset"]},
        }
        for x in optimizer_results.columns
    ]
    optimizer_results_rowData = optimizer_results.to_dict("records")
    sideBar = {
        "toolPanels": [
            {
                "id": "columns",
                "labelDefault": "Columns",
                "labelKey": "columns",
                "iconKey": "columns",
                "toolPanel": "agColumnsToolPanel",
            },
            {
                "id": "filters",
                "labelDefault": "Filters",
                "labelKey": "filters",
                "iconKey": "filter",
                "toolPanel": "agFiltersToolPanel",
            },
            {
                "id": "filters 2",
                "labelKey": "filters",
                "labelDefault": "More Filters",
                "iconKey": "menu",
                "toolPanel": "agFiltersToolPanel",
            },
        ],
        "position": "right",
        "defaultToolPanel": "filters",
    }
    results_stats_columnDefs = [
        {
            "headerName": x,
            "field": x,
            "filter": True,
            "floatingFilter": True,
            "filterParams": {"buttons": ["apply", "reset"]},
        }
        for x in results_stats.columns
    ]
    results_stats_rowData = results_stats.to_dict("records")
    stats_sideBar = {
        "toolPanels": [
            {
                "id": "columns",
                "labelDefault": "Columns",
                "labelKey": "columns",
                "iconKey": "columns",
                "toolPanel": "agColumnsToolPanel",
            },
            {
                "id": "filters",
                "labelDefault": "Filters",
                "labelKey": "filters",
                "iconKey": "filter",
                "toolPanel": "agFiltersToolPanel",
            },
            {
                "id": "filters 2",
                "labelKey": "filters",
                "labelDefault": "More Filters",
                "iconKey": "menu",
                "toolPanel": "agFiltersToolPanel",
            },
        ],
        "position": "right",
        "defaultToolPanel": "filters",
    }
    cardinality_stats_columnDefs = [
        {
            "headerName": x,
            "field": x,
            "filter": True,
            "floatingFilter": True,
            "filterParams": {"buttons": ["apply", "reset"]},
        }
        for x in cardinality_stats.columns
    ]
    cardinality_stats_rowData = cardinality_stats.to_dict("records")
    cardinality_stats_sideBar = {
        "toolPanels": [
            {
                "id": "columns",
                "labelDefault": "Columns",
                "labelKey": "columns",
                "iconKey": "columns",
                "toolPanel": "agColumnsToolPanel",
            },
            {
                "id": "filters",
                "labelDefault": "Filters",
                "labelKey": "filters",
                "iconKey": "filter",
                "toolPanel": "agFiltersToolPanel",
            },
            {
                "id": "filters 2",
                "labelKey": "filters",
                "labelDefault": "More Filters",
                "iconKey": "menu",
                "toolPanel": "agFiltersToolPanel",
            },
        ],
        "position": "right",
        "defaultToolPanel": "filters",
    }
    return html.Div(
        [
            dmc.Card(
                children=[
                    dag.AgGrid(
                        id="optimizer-results-grid",
                        enableEnterpriseModules=True,
                        columnDefs=optimizer_results_columnDefs,
                        rowData=optimizer_results_rowData,
                        columnSize="sizeToFit",
                        style={"height": "550px"},
                        dashGridOptions={
                            "rowSelection": "multiple",
                            "sideBar": sideBar,
                        },
                        defaultColDef=dict(
                            resizable=True,
                            editable=True,
                            sortable=True,
                            autoHeight=True,
                            width=150,
                        ),
                    ),
                ],
            ),
            html.Br(),
            dmc.Card(
                children=[
                    dag.AgGrid(
                        id="stats-results-grid",
                        enableEnterpriseModules=True,
                        columnDefs=results_stats_columnDefs,
                        rowData=results_stats_rowData,
                        columnSize="sizeToFit",
                        style={"height": "550px"},
                        dashGridOptions={
                            "rowSelection": "multiple",
                            "sideBar": stats_sideBar,
                        },
                        defaultColDef=dict(
                            resizable=True,
                            editable=True,
                            sortable=True,
                            autoHeight=True,
                            width=150,
                        ),
                    ),
                ],
            ),
            html.Br(),
            dmc.Card(
                children=[
                    dag.AgGrid(
                        id="cardinality-stats-grid",
                        enableEnterpriseModules=True,
                        columnDefs=cardinality_stats_columnDefs,
                        rowData=cardinality_stats_rowData,
                        columnSize="sizeToFit",
                        style={"height": "550px"},
                        dashGridOptions={
                            "rowSelection": "multiple",
                            "sideBar": cardinality_stats_sideBar,
                        },
                        defaultColDef=dict(
                            resizable=True,
                            editable=True,
                            sortable=True,
                            autoHeight=True,
                            width=150,
                        ),
                    ),
                ],
            ),
        ]
    )


#################################
############ CHATBOT ############
#################################


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


@callback(
    Output("rcw-output-table", "columnDefs"),
    Output("rcw-output-table", "rowData"),
    Output("rcw-output-table", "className", allow_duplicate=True),
    Output("table-loader", "className", allow_duplicate=True),
    Input("dummy-output", "children"),
    prevent_initial_call=True,
)
def update_table(sql_query):
    if sql_query:
        df = dbx_SQL_query(sql_query)
        if df is False:
            return [], [], "ag-theme-balham hide", "hide"
        columnDefs = [{"field": i, "headerName": i} for i in df.columns]
        rowData = df.to_dict("records")
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
