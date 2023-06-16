import dash
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash.exceptions import PreventUpdate

import pandas as pd
import dash_ag_grid as dag
from databricks.connect import DatabricksSession
import sqlalchemy.exc
import os
from configparser import ConfigParser
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


CATALOG = "main"
SCHEMA = "information_schema"


db = SQLAlchemy()

metadata = MetaData()


class RawQueryTempView(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    QueryStartTime = db.Column(db.DateTime)
    QueryEndTime = db.Column(db.DateTime)
    QueryDurationSeconds = db.Column(db.Numeric(precision=10, scale=2))


rawquery_tbl = Table("raw_queries", RawQueryTempView.metadata)


def layout():
    # big_engine = create_engine(
    #     f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
    # )
    # schemas_init_statment = f"SELECT schema_name FROM {CATALOG}.{SCHEMA}.SCHEMATA ORDER BY created DESC;"  # ORDER BY created DESC
    # schema_list = pd.read_sql_query(schemas_init_statment, big_engine)
    # schema_select_data = [
    #     {"label": c, "value": c} for c in schema_list.schema_name.unique()
    # ]

    return html.Div(
        [
            dmc.Title("Results"),
            dmc.Divider(variant="solid"),
            dmc.Space(h=20),
            dmc.Group(
                position="left",
                children=[
                    dmc.Select(
                        id="output_db_select",
                        # options=[],
                        data=[],
                        placeholder="Select one",
                        searchable=True,
                        style={
                            "width": "300px",
                            "position": "relative",
                            "top": "0px",
                        },
                    ),
                    dmc.Button(
                        "Refresh",
                        id="refresh-button-step3",
                        variant="default",
                        style={
                            "width": "120px",
                            "position": "relative",
                            "top": "0px",
                        },
                    ),
                    html.Div(
                        id="engine-test-result-step3",
                        style={
                            "width": "300px",
                            "position": "relative",
                            "left": "20px",
                            "top": "0px",
                        },
                    ),
                    html.Div(
                        style={
                            "width": "300px",
                            "position": "relative",
                            "left": "350px",
                            "top": "0px",
                        },
                        children=[
                            dcc.Dropdown(
                                id="profile-dropdown-step3",
                                options=[],
                                value="Select Profile",
                                style={
                                    "width": "200px",
                                    "position": "relative",
                                    "left": "40px",
                                    "top": "0px",
                                },
                            )
                        ],
                    ),
                ],
            ),
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
            dmc.Space(h=20),
            dmc.LoadingOverlay(
                overlayOpacity=0.95,
                loaderProps=dict(color="#FF3621", variant="bars"),
                children=html.Div(id="result-page-layout"),
            ),
            html.Div(id="sqlalchemycheck"),
            dcc.Store(id="hostname-store3", storage_type="memory"),
            dcc.Store(id="path-store3", storage_type="memory"),
            dcc.Store(id="token-store3", storage_type="memory"),
            dcc.Store(id="cluster-id-store3", storage_type="memory"),
            dcc.Store(id="cluster-name-store3", storage_type="memory"),
            dcc.Store(id="user-name-store3", storage_type="memory"),
            # dcc.Store(id="output-db-select", storage_type="memory"),
            dcc.Interval(id="interval", interval=1000 * 60 * 60 * 24, n_intervals=0),
            component_chatbot(),
        ]
    )


@callback(
    Output("output_db_select", "data"),
    Input("refresh-button-step3", "n_clicks"),
    Input("hostname-store3", "data"),
    Input("path-store3", "data"),
    Input("token-store3", "data"),
)
def fetch_schema_names(n_clicks, hostname, path, token):
    if not hostname or not path or not token:
        return []

    engine_url = f"databricks://token:{token}@{hostname}/?http_path={path}&catalog={CATALOG}&schema={SCHEMA}"
    big_engine = create_engine(engine_url)

    schemas_init_statement = (
        f"SELECT schema_name FROM {CATALOG}.{SCHEMA}.SCHEMATA ORDER BY created DESC;"
    )
    schema_list = pd.read_sql_query(schemas_init_statement, big_engine)
    schema_select_data = [
        {"label": c, "value": c} for c in schema_list.schema_name.unique()
    ]

    return schema_select_data


@callback(
    Output("profile-dropdown-step3", "options"),
    [Input("refresh-button-step3", "n_clicks"), Input("interval", "n_intervals")],
    State("profile-dropdown-step3", "value"),
)
def populate_profile_dropdown(n_clicks, n_intervals, profile_name):
    config = ConfigParser()
    file_path = os.path.expanduser("~/.databrickscfg")

    if not os.path.exists(file_path):
        return []

    config.read(file_path)
    options = []

    for section in config.sections():
        if (
            config.has_option(section, "host")
            and config.has_option(section, "path")
            and config.has_option(section, "token")
            and config.has_option(section, "cluster_name")
            and config.has_option(section, "cluster_id")
            and config.has_option(section, "user_name")
        ):
            options.append({"label": section, "value": section})

    return options


@callback(
    [
        Output("hostname-store3", "data"),
        Output("token-store3", "data"),
        Output("path-store3", "data"),
        Output("cluster-name-store3", "data"),
        Output("cluster-id-store3", "data"),
        Output("user-name-store3", "data"),
    ],
    [Input("profile-dropdown-step3", "value")],
    prevent_initial_call=True,
)
def parse_databricks_config(profile_name):
    if profile_name:
        config = ConfigParser()
        file_path = os.path.expanduser("~/.databrickscfg")

        if os.path.exists(file_path):
            config.read(file_path)

            if config.has_section(profile_name):
                host = config.get(profile_name, "host")
                token = config.get(profile_name, "token")
                path = config.get(profile_name, "path")
                cluster_name = config.get(profile_name, "cluster_name")
                cluster_id = config.get(profile_name, "cluster_id")
                user_name = config.get(profile_name, "user_name")
                host = host.replace("https://", "")

                return host, token, path, cluster_name, cluster_id, user_name

    return None, None, None, None, None, None


@callback(
    Output("engine-test-result-step3", "children"),
    Input("profile-dropdown-step3", "value"),
    [
        State("hostname-store3", "data"),
        State("path-store3", "data"),
        State("token-store3", "data"),
    ],
    prevent_initial_call=True,
)
def test_engine_and_spark_connection(profile_name, hostname, path, token):
    if profile_name:
        (
            host,
            token,
            path,
            cluster_name,
            cluster_id,
            user_name,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            # Modify the path to remove "/sql/1.0/warehouses/"
            sql_warehouse = path.replace("/sql/1.0/warehouses/", "")
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            engine = create_engine(engine_url)

            try:
                # Test the engine connection by executing a sample query
                with engine.connect() as connection:
                    result = connection.execute("SELECT 1")
                    test_value = result.scalar()

                    if test_value == 1:
                        os.environ["USER"] = "anything"
                        spark = DatabricksSession.builder.remote(
                            f"sc://{host}:443/;token={token};x-databricks-cluster-id={cluster_id}"
                        ).getOrCreate()

                        try:
                            # Test the Spark connection by executing a sample SQL command
                            spark_result = spark.sql("SELECT 1")
                            spark_test_value = spark_result.collect()[0][0]

                            if spark_test_value == 1:
                                return dmc.LoadingOverlay(
                                    dmc.Badge(
                                        id="spark-connection-badge",
                                        variant="dot",
                                        color="green",
                                        size="lg",
                                        children=[
                                            html.Span(
                                                f"Connected to SQL Warehouse: {sql_warehouse} + Cluster: {cluster_name}"
                                            )
                                        ],
                                    ),
                                    loaderProps={
                                        "variant": "dots",
                                        "color": "orange",
                                        "size": "xl",
                                    },
                                )
                            elif (
                                "SPARK CONNECTION FAILED: THE CLUSTER" in str(e).upper()
                            ):
                                return dmc.LoadingOverlay(
                                    dmc.Badge(
                                        id="spark-connection-badge",
                                        variant="dot",
                                        color="yellow",
                                        size="lg",
                                        children=[
                                            html.Span(
                                                f"Spark Connection Pending: {cluster_name}"
                                            )
                                        ],
                                    ),
                                    loaderProps={
                                        "variant": "dots",
                                        "color": "orange",
                                        "size": "xl",
                                    },
                                )
                        except Exception as e:
                            return dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="spark-connection-badge",
                                    variant="dot",
                                    color="red",
                                    size="lg",
                                    children=[
                                        html.Span(f"Spark Connection failed: {str(e)}")
                                    ],
                                ),
                                loaderProps={
                                    "variant": "dots",
                                    "color": "orange",
                                    "size": "xl",
                                },
                            )
            except sqlalchemy.exc.OperationalError as e:
                return dmc.LoadingOverlay(
                    dmc.Badge(
                        id="spark-connection-badge",
                        variant="dot",
                        color="red",
                        size="lg",
                        children=[
                            html.Span(f"SQL Alchemy Connection failed: {str(e)}")
                        ],
                    ),
                    loaderProps={
                        "variant": "dots",
                        "color": "orange",
                        "size": "xl",
                    },
                )

    return html.Div("Please select a profile.", style={"color": "orange"})


@callback(
    Output("result-page-layout", "children"),
    [Input("refresh-button-step3", "n_clicks"), Input("output_db_select", "value")],
    [
        State("hostname-store3", "data"),
        State("path-store3", "data"),
        State("token-store3", "data"),
    ],
    prevent_initial_call=True,
)
def create_dynamic_results_layout(n_clicks, selected_db, hostname, path, token):
    results_engine = create_engine(
        f"databricks://token:{token}@{hostname}/?http_path={path}&catalog={CATALOG}&schema={selected_db}"
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
    # print(grouped_queries)

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
    # print(result_data.columns)
    result_data["QueryStartTime"] = result_data["QueryStartTime"].dt.hour

    # print("aaaaaaaaaaaaaaaaaaa")

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
    # print(by_day_results)

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
    # print(raw_queries)
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


def dbx_SQL_query(query, token, hostname, path):
    try:
        results_engine = create_engine(
            f"databricks://token:{token}@{hostname}/?http_path={path}&catalog=hive_metastore&schema=default"
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
