import dash
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
import result_page_table_config as comp
from result_page_table_config import create_accordion_item, create_ag_grid
import json
import requests

dash.register_page(__name__, path="/optimizer-results", title="Results")

SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
WAREHOUSE_ID = "f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"


def layout():
    big_engine = create_engine(
        f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
    )
    tables_stmt = f"SELECT table_catalog, table_schema,table_name, created, created_by, last_altered, last_altered_by FROM {CATALOG}.INFORMATION_SCHEMA.TABLES;"
    schemas_init_statment = f"SELECT schema_name FROM {CATALOG}.{SCHEMA}.SCHEMATA ORDER BY created DESC;"  # ORDER BY created DESC
    schema_list = pd.read_sql_query(schemas_init_statment, big_engine)
    schema_select_data = [
        {"label": c, "value": c} for c in schema_list.schema_name.unique()
    ]

    return html.Div(
        [
            dmc.Select(
                label="Delta Optimizer Instance",
                placeholder="Select one",
                id="output-db-select",
                searchable=True,
                data=schema_select_data,
            ),
            dmc.Space(h=20),
            dmc.Group(
                position="center",
                children=[
                    dmc.Button(
                        "Run Strategy", id="run-strategy-button", variant="outline"
                    ),
                    dmc.Button("Schedule", variant="outline"),
                ],
            ),
            dmc.Space(h=10),
            dmc.Text(id="run-strategy-output", align="center"),
            dmc.Space(h=20),
            dmc.LoadingOverlay(
                overlayOpacity=0.95,
                loaderProps=dict(color="#FF3621", variant="bars"),
                children=html.Div(id="result-page-layout"),
            ),
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
    get_raw_queries = f"""SELECT 
                FROM_UNIXTIME(query_start_time_ms/1000) AS QueryStartTime,
                FROM_UNIXTIME(query_end_time_ms/1000) AS QueryEndTime,
                duration/1000 AS QueryDurationSeconds
              FROM 
                delta_optimizer_mercury.raw_query_history_statistics"""
    raw_queries = pd.read_sql_query(get_raw_queries, results_engine)
    get_most_expensive = f"""SELECT r.query_hash, r.query_text, 
                  SUM(r.duration/1000) AS TotalRuntimeOfQuery, 
                  AVG(r.duration/1000) AS AvgDurationOfQuery, 
                  COUNT(r.query_id) AS TotalRunsOfQuery, 
                  COUNT(r.query_id) / COUNT(DISTINCT date_trunc('day', TO_TIMESTAMP(r.query_start_time_ms/1000))) AS QueriesPerDay, 
                  SUM(r.duration/1000) / COUNT(DISTINCT date_trunc('day', TO_TIMESTAMP(r.query_start_time_ms/1000))) AS TotalRuntimePerDay 
              FROM {selected_db}.raw_query_history_statistics r 
              WHERE r.query_start_time_ms >= UNIX_TIMESTAMP(CURRENT_DATE() - INTERVAL 7 DAY)
              GROUP BY r.query_hash, r.query_text 
              ORDER BY TotalRuntimePerDay DESC"""
    most_expensive = pd.read_sql_query(get_most_expensive, results_engine)
    # get_query_runs = f"""SELECT
    #               date_trunc('minute', QueryStartTime) AS Date,
    #               COUNT(*) AS TotalQueryRuns,
    #               AVG(QueryDurationSeconds) AS AvgQueryDurationSeconds
    #           FROM ({raw_queries}
    #           WHERE QueryStartTime > (current_timestamp() - INTERVAL '12 HOURS')
    #           GROUP BY date_trunc('minute', QueryStartTime)
    #           ORDER BY Date"""
    # query_runs = pd.read_sql_query(get_query_runs, results_engine)
    # get_total_runtime_query = f"""WITH r AS (
    #     SELECT
    #         date_trunc('minute', r.QueryStartTime) AS Date,
    #         r.query_hash,
    #         SUM(r.duration / 1000) AS TotalRuntimeOfQuery,
    #         AVG(r.duration / 1000) AS AvgDurationOfQuery,
    #         COUNT(r.query_id) AS TotalRunsOfQuery
    #     FROM
    #         {raw_queries} r -- Replace with the actual table name
    #     WHERE
    #         QueryStartTime > (current_timestamp() - INTERVAL '12 HOURS')
    #     GROUP BY
    #         date_trunc('minute', r.QueryStartTime),
    #         r.query_hash
    # ),
    # s AS (
    #     SELECT
    #         *,
    #         DENSE_RANK() OVER (PARTITION BY Date ORDER BY TotalRuntimeOfQuery DESC) AS PopularityRank
    #     FROM
    #         r
    # )
    # SELECT
    #     uu.query_text,
    #     s.*
    # FROM
    #     s
    # LEFT JOIN
    #     unique_queries uu ON uu.query_hash = s.query_hash
    # WHERE
    #     PopularityRank <= 10;
    # """
    # total_runtime_query = pd.read_sql_query(get_total_runtime_query, results_engine)
    #     get_longest_query = f"""WITH r AS (
    #     SELECT
    #         date_trunc('minute', r.QueryStartTime) AS Date,
    #         r.query_hash,
    #         SUM(r.duration / 1000) AS TotalRuntimeOfQuery,
    #         AVG(r.duration / 1000) AS AvgDurationOfQuery,
    #         COUNT(r.query_id) AS TotalRunsOfQuery
    #     FROM
    #         {raw_queries} r
    #     WHERE
    #         QueryStartTime > (current_timestamp() - INTERVAL '12 HOURS')
    #     GROUP BY
    #         date_trunc('minute', r.QueryStartTime),
    #         r.query_hash
    # ),
    # s AS (
    #     SELECT
    #         *,
    #         DENSE_RANK() OVER (PARTITION BY Date ORDER BY AvgDurationOfQuery DESC) AS PopularityRank
    #     FROM
    #         r
    # )
    # SELECT
    #     uu.query_text,
    #     s.*
    # FROM
    #     s
    # LEFT JOIN
    #     unique_queries uu ON uu.query_hash = s.query_hash
    # WHERE
    #     PopularityRank <= 10;
    # """
    #     longest_queries = pd.read_sql_query(get_longest_query, results_engine)
    #     get_most_often_query = "WITH r AS (SELECT date_trunc('minute', r.QueryStartTime) AS Date,r.query_hash, SUM(r.duration/1000) AS TotalRuntimeOfQuery,AVG(r.duration/1000) AS AvgDurationOfQuery, COUNT(r.query_id) AS TotalRunsOfQuery FROM raw_queries r WHERE QueryStartTime > (current_timestamp() - INTERVAL 12 HOURS) GROUP BY date_trunc('minute', r.QueryStartTime), r.query_hash),s as (SELECT *,DENSE_RANK() OVER (PARTITION BY Date ORDER BY TotalRunsOfQuery DESC) AS PopularityRank FROM r)SELECT uu.query_text,s.* FROM s LEFT JOIN unique_queries uu ON uu.query_hash = s.query_hash WHERE PopularityRank <= 10"
    #     most_often_query = pd.read_sql_query(get_most_often_query, results_engine)
    get_merge_expense = f"SELECT * FROM {selected_db}.write_statistics_merge_predicate"
    merge_expense = pd.read_sql_query(get_merge_expense, results_engine)
    return dmc.AccordionMultiple(
        children=[
            create_accordion_item(
                "Most Recent Strategy Result",
                [create_ag_grid(optimizer_results)],
            ),
            create_accordion_item(
                "Table Statistics",
                [create_ag_grid(results_stats)],
            ),
            create_accordion_item(
                "Cardinality Sampling Statistics",
                [create_ag_grid(cardinality_stats)],
            ),
            create_accordion_item("Raw Queries", [create_ag_grid(raw_queries)]),
            create_accordion_item(
                "Most Expensive Queries", [create_ag_grid(most_expensive)]
            ),
            # create_accordion_item(
            #     "Queries Over Time - general", [create_ag_grid(query_runs)]
            # ),
            # create_accordion_item(
            #     "Top 10 Queries by Duration", [create_ag_grid(total_runtime_query)]
            # ),
            # create_accordion_item(
            #     "Top 10 Queries by Day", [create_ag_grid(longest_queries)]
            # ),
            # create_accordion_item(
            #     "Most Often Run Queries by Day", [create_ag_grid(most_often_query)]
            # ),
            create_accordion_item(
                "Most Expensive Merge/Delete Operations",
                [create_ag_grid(merge_expense)],
            ),
        ],
    )


@callback(
    Output("run-strategy-output", "children"),
    Input("run-strategy-button", "n_clicks"),
    State("general-store", "data"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(n_clicks, outputdpdn2):
    optimize_job_two = {
        "name": "Delta_Optimizer_Step_2",
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Delta_Optimizer_Step_2",
                "notebook_task": {
                    "notebook_path": "/Repos/sach@streamtostream.com/edw-best-practices/Delta Optimizer/Step 2_ Strategy Runner",
                    "notebook_params": {
                        "Optimizer Output Database:": outputdpdn2["outputdpdn2"],
                        "exclude_list(csv)": "",
                        "include_list(csv)": "",
                        "table_mode": "include_all_tables",
                    },
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": "0510-131932-sflv6c6d",
                "libraries": [
                    {
                        "whl": "dbfs:/FileStore/jars/d7178675_7c86_429a_83f8_d0ed668ef4c5/deltaoptimizer-1.4.0-py3-none-any.whl"
                    }
                ],
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
            }
        ],
        "format": "MULTI_TASK",
    }
    job_json2 = json.dumps(optimize_job_two)
    # Get this from a secret or param
    headers_auth2 = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/create"
    endp_resp2 = requests.post(uri2, data=job_json2, headers=headers_auth2).json()
    # Run Job
    optimize_job_two = endp_resp2["job_id"]
    run_now_uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/run-now"
    job_run_2 = {
        "job_id": 60847542300700,
        "notebook_params": {
            "Optimizer Output Database:": outputdpdn2["outputdpdn2"],
            "exclude_list(csv)": "",
            "include_list(csv)": "",
            "table_mode": "include_all_tables",
        },
    }
    job_run_json2 = json.dumps(job_run_2)
    run_resp2 = requests.post(
        run_now_uri2, data=job_run_json2, headers=headers_auth2
    ).json()
    msg2 = (
        f"Optimizer Ran with Job Id: {endp_resp2['job_id']} \n run message: {run_resp2}"
    )
    return msg2


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
import dash_ag_grid as dag
import os

import pandas as pd
from databricks import sql


def dbx_SQL_query(query):
    try:
        results_engine = create_engine(
            f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog=hive_metastore&schema=default"
        )
        df = pd.read_sql_query(query, results_engine)

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
                        label="Waiting for SQL query to return...",
                        withArrow=True,
                        children=dmc.Loader(
                            id="table-loader",
                            color="#FF3621",
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
