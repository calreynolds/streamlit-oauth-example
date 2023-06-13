import dash
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
import result_page_table_config as comp
from result_page_table_config import (
    create_accordion_item,
    create_ag_grid,
    create_top_ten_figure,
)
import json
import requests
from datetime import datetime, timedelta
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table, create_engine, MetaData

dash.register_page(__name__, path="/optimizer-results", title="Results")

SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
WAREHOUSE_ID = "f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"
SOUND = "dbxdashstudio"

conn_str = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SOUND}"
extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL Example Script",
}
sound_engine = create_engine(
    conn_str,
    connect_args=extra_connect_args,
)


db = SQLAlchemy()

metadata = MetaData()


class RawQueryTempView(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    QueryStartTime = db.Column(db.DateTime)
    QueryEndTime = db.Column(db.DateTime)
    QueryDurationSeconds = db.Column(db.Numeric(precision=10, scale=2))


rawquery_tbl = Table("raw_queries", RawQueryTempView.metadata)


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
            # dmc.Group(
            #     position="center",
            #     children=[
            #         dmc.Button(
            #             "Run Strategy", id="run-strategy-button", variant="outline"
            #         ),
            #         dmc.Button("Schedule", id="checksql", variant="outline"),
            #     ],
            # ),
            # dmc.Space(h=10),
            # dmc.Text(id="run-strategy-output", align="center"),
            # dmc.Space(h=20),
            dmc.LoadingOverlay(
                overlayOpacity=0.95,
                loaderProps=dict(color="#FF3621", variant="bars"),
                children=html.Div(id="result-page-layout"),
            ),
            html.Div(id="sqlalchemycheck"),
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
                duration/1000 AS QueryDurationSeconds,
                query_hash AS query_hash, 
                query_text AS query_text,
                query_id AS query_id

              FROM 
                {selected_db}.raw_query_history_statistics"""
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

    # Convert QueryStartTime column to datetime type

    # Convert QueryStartTime column to datetime type
    raw_queries["QueryStartTime"] = pd.to_datetime(raw_queries["QueryStartTime"])

    # Filter rows based on QueryStartTime
    start_time_threshold = datetime.now() - timedelta(hours=12)
    filtered_queries = raw_queries[raw_queries["QueryStartTime"] > start_time_threshold]

    # Grouping and aggregation to calculate TotalQueryRuns and AvgQueryDurationSeconds
    grouped_queries = (
        filtered_queries.groupby(pd.Grouper(key="QueryStartTime", freq="Min"))
        .agg(
            TotalQueryRuns=("query_id", "count"),
            AvgQueryDurationSeconds=("QueryDurationSeconds", "mean"),
        )
        .reset_index()
    )

    # Sort by Date
    grouped_queries.sort_values(by="QueryStartTime", inplace=True)

    # Print the resulting DataFrame
    print(grouped_queries)

    # GET TOP 10 QUERIES WITH MOST TOTAL RUNTIME

    # Grouping and aggregation to calculate TotalRuntimeOfQuery and TotalRunsOfQuery
    grouped_data = (
        raw_queries.groupby(["QueryStartTime", "query_hash"])
        .agg(
            QueryDurationSeconds=("QueryDurationSeconds", lambda x: x.sum() / 1000),
            query_id=("query_id", "count"),
        )
        .reset_index()
    )

    # Calculate TotalRuntimeOfQuery
    grouped_data["TotalRuntimeOfQuery"] = grouped_data.groupby("query_hash")[
        "QueryDurationSeconds"
    ].transform("sum")

    # Calculate AvgDurationOfQuery
    grouped_data["AvgDurationOfQuery"] = (
        grouped_data["TotalRuntimeOfQuery"] / grouped_data["query_id"]
    )

    # Ranking by PopularityRank
    grouped_data["PopularityRank"] = grouped_data.groupby("QueryStartTime")[
        "AvgDurationOfQuery"
    ].rank(method="dense", ascending=False)

    # Join with unique_queries
    unique_queries = raw_queries.drop_duplicates(subset="query_hash", keep="first")[
        ["query_hash", "query_text"]
    ].reset_index(drop=True)

    result_data = grouped_data.merge(unique_queries, on="query_hash", how="left")

    # Filter by PopularityRank <= 10
    result_data = result_data[result_data["PopularityRank"] <= 10]

    # Print the resulting DataFrame
    result_data["QueryStartTime"] = pd.to_datetime(
        result_data["QueryStartTime"]
    )  # Convert timestamp column to datetime if needed
    print(result_data.columns)
    result_data["QueryStartTime"] = result_data["QueryStartTime"].dt.hour

    print("aaaaaaaaaaaaaaaaaaa")

    # Top 10 Longest Running Queries By Day
    start_time_threshold = pd.Timestamp.now() - pd.Timedelta(hours=12)
    filtered_queries = raw_queries[raw_queries["QueryStartTime"] > start_time_threshold]

    # Perform grouping and aggregation by day
    grouped_queries_by_day = (
        filtered_queries.groupby(
            [pd.Grouper(key="QueryStartTime", freq="D"), "query_hash"]
        )
        .agg(
            {
                "QueryDurationSeconds": lambda x: x.sum() / 1000,
                "query_id": "count",
            }
        )
        .reset_index()
    )

    # Calculate additional metrics
    grouped_queries_by_day["TotalRuntimeOfQuery"] = grouped_queries_by_day.groupby(
        "query_hash"
    )["QueryDurationSeconds"].transform("sum")

    # Calculate AvgDurationOfQuery
    grouped_queries_by_day["AvgDurationOfQuery"] = (
        grouped_queries_by_day["TotalRuntimeOfQuery"]
        / grouped_queries_by_day["query_id"]
    )

    # Ranking
    grouped_queries_by_day["PopularityRank"] = grouped_queries_by_day.groupby(
        "QueryStartTime"
    )["AvgDurationOfQuery"].rank(method="dense", ascending=False)

    # Join with unique_queries
    by_day_results = grouped_queries_by_day.merge(
        unique_queries, on="query_hash", how="left"
    )

    # Filter by PopularityRank <= 10
    by_day_results = by_day_results[by_day_results["PopularityRank"] <= 10]

    # Select desired columns
    by_day_results = by_day_results[
        [
            "query_text",
            "QueryStartTime",
            "query_hash",
            "TotalRuntimeOfQuery",
            "AvgDurationOfQuery",
            "query_id",
            "PopularityRank",
        ]
    ]

    # Print the resulting DataFrame
    print(by_day_results)

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
    print(raw_queries)
    # query_start_time = raw_queries["QueryStartTime"].values
    # query_end_time = raw_queries["QueryEndTime"].values
    # query_duration_seconds = raw_queries["QueryDurationSeconds"].values
    # if selected_db is not None:
    #     ins = rawquery_tbl.insert().values(
    #         QueryStartTime=query_start_time,
    #         QueryEndTime=query_end_time,
    #         QueryDurationSeconds=query_duration_seconds,
    #     )
    #     conn = sound_engine.connect()
    #     conn.execute(ins)
    #     conn.close()
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
            create_accordion_item(
                "Queries Over Time - general", [create_ag_grid(grouped_queries)]
            ),
            create_accordion_item(
                "Top 10 Queries by Duration", [create_ag_grid(result_data)]
            ),
            create_accordion_item(
                "Top 10 Queries by Duration Viz", [create_top_ten_figure(result_data)]
            ),
            create_accordion_item(
                "Top 10 Queries by Day", [create_ag_grid(by_day_results)]
            ),
            # create_accordion_item(
            #     "Most Often Run Queries by Day", [create_ag_grid(most_often_query)]
            # ),
            create_accordion_item(
                "Most Expensive Merge/Delete Operations",
                [create_ag_grid(merge_expense)],
            ),
        ],
    )


# @callback(
#     Output("sqlalchemycheck", "children"),
#     [Input("output-db-select", "value")],
# )
# def write_temp_view(selected_db):
#     stmt = f"""
#     SELECT
#         FROM_UNIXTIME(query_start_time_ms/1000) AS QueryStartTime,
#         FROM_UNIXTIME(query_end_time_ms/1000) AS QueryEndTime,
#         duration/1000 AS QueryDurationSeconds
#     FROM
#         main.{selected_db}.raw_query_history_statistics
# """

#     raw_queries = pd.read_sql_query(stmt, sound_engine)
#     QueryStartTime = raw_queries["QueryStartTime"]
#     QueryEndTime = raw_queries["QueryEndTime"]
#     QueryDurationSeconds = raw_queries["QueryDurationSeconds"]

#     if selected_db is not None:
#         ins = rawquery_tbl.insert().values(
#             QueryStartTime=QueryStartTime,
#             QueryEndTime=QueryEndTime,
#             QueryDurationSeconds=QueryDurationSeconds,
#         )
#         conn = sound_engine.connect()
#         conn.execute(ins)
#         conn.close()

#     return "success"


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
    except Exception as e:
        print(f"Error executing SQL query", e)
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
    position={"bottom": 40, "left": 20},
)


CHAT_MODAL = dmc.Affix(
    position={"bottom": 140, "left": 20},
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
                        style={"max-height": "200px"},
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
            dcc.Store(
                id="prefix-path-store",
                data=os.getenv("DASH_REQUESTS_PATHNAME_PREFIX", "/"),
            ),
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
    State("prefix-path-store", "data"),
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
