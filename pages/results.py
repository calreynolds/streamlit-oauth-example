import dash
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from dash.exceptions import PreventUpdate
import plotly_express as px
import dash_mantine_components as dmc
import dash_chart_editor as dce
import dash_bootstrap_components as dbc
import components as comp

import pandas as pd
import dash_ag_grid as dag
import sqlalchemy.exc
import os
from configparser import ConfigParser
from sqlalchemy.engine import create_engine
import result_page_table_config 
from result_page_table_config import (
    create_accordion_item,
    create_ag_grid,
    create_bar_chart,
    cardinality_bar_chart,
    create_bar_line_query_daily_chart,
)
import json
import requests
from datetime import datetime, timedelta
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table, create_engine, MetaData

# dash.register_page(__name__, path="/delta-optimizer/optimizer-results", title="Results")


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
    return dmc.MantineProvider(
        children=dmc.NotificationsProvider(
            [
                html.Div(id="cluster-loading-notification-step3"),
                html.Div(id="cluster-loaded-notification-step3"),
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
                                dmc.Select(
                                    id="profile-dropdown-step3",
                                    data=[],
                                    value="Select Profile",
                                    style={
                                        "width": "230px",
                                        "position": "relative",
                                        "left": "55px",
                                        "top": "0px",
                                    },
                                )
                            ],
                        ),
                    ],
                ),
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
                dcc.Interval(
                    id="interval", interval=1000 * 60 * 60 * 24, n_intervals=0
                ),
                component_chatbot(),
            ]
        )
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
    Output("profile-dropdown-step3", "data"),
    Input("profile-dropdown-step3", "value"),
)
def populate_profile_dropdown(profile_name):
    config = ConfigParser()
    file_path = os.path.expanduser("~/.databrickscfg")

    if not os.path.exists(file_path):
        return [], []

    config.read(file_path)
    options = []

    for section in config.sections():
        if (
            config.has_option(section, "host")
            and config.has_option(section, "path")
            and config.has_option(section, "token")
            # and config.has_option(section, "cluster_name")
            # and config.has_option(section, "cluster_id")
            # and config.has_option(section, "user_name")
        ):
            options.append({"label": section, "value": section})

    return options


@callback(
    [
        Output("hostname-store3", "data"),
        Output("token-store3", "data"),
        Output("path-store3", "data"),
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

                host = host.replace("https://", "")

                return host, token, path

    return None, None, None


@callback(
    [
        Output("cluster-loading-notification-step3", "children"),
        Output("cluster-loaded-notification-step3", "children"),
        Output("engine-test-result-step3", "children"),
    ],
    [
        Input("profile-dropdown-step3", "value"),
        Input("refresh-button-step3", "n_clicks"),
    ],
    [
        State("hostname-store3", "data"),
        State("path-store3", "data"),
        State("token-store3", "data"),
    ],
)
def get_cluster_state(profile_name, n_clicks, host, path, token):
    if n_clicks or profile_name:
        if profile_name:
            host, token, path = parse_databricks_config(profile_name)
            if host and token and path:
                sqlwarehouse = path.replace("/sql/1.0/warehouses", "")

                try:
                    test_job_uri = (
                        f"https://{host}/api/2.0/sql/warehouses/{sqlwarehouse}"
                    )
                    headers_auth = {"Authorization": f"Bearer {token}"}
                    test_job = requests.get(test_job_uri, headers=headers_auth).json()
                    # print(test_job)

                    if test_job["state"] == "TERMINATED":
                        return (
                            compo.cluster_loading("Cluster is loading..."),
                            dash.no_update,
                            dash.no_update,
                        )

                    if test_job["state"] == "STARTING":
                        return (
                            compo.cluster_loading("Cluster is loading..."),
                            dash.no_update,
                            dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="engine-connection-badge",
                                    variant="gradient",
                                    gradient={"from": "yellow", "to": "orange"},
                                    size="lg",
                                    children=[
                                        html.Span(f"Connecting to Workspace: {host} ")
                                    ],
                                ),
                            ),
                        )
                    elif test_job["state"] == "RUNNING":
                        return (
                            dash.no_update,
                            compo.cluster_loaded("Cluster is loaded"),
                            dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="engine-connection-badge",
                                    variant="gradient",
                                    gradient={"from": "teal", "to": "lime", "deg": 105},
                                    color="green",
                                    size="lg",
                                    children=[
                                        html.Span(f"Connected to Workspace: {host} ")
                                    ],
                                ),
                            ),
                        )

                except Exception as e:
                    print(f"Error occurred while testing engine connection: {str(e)}")

    return dash.no_update, dash.no_update, dash.no_update


cardinality_stats = pd.DataFrame()
most_expensive = pd.DataFrame()
num_queries_per_day = pd.DataFrame()


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
    global cardinality_stats
    cardinality_stats = pd.read_sql_query(get_cardinality, results_engine)
    cardinality_stats["CardinalityProportionScaledUp"] = (
        cardinality_stats["CardinalityProportionScaled"] * 100
    ).round(2)
    cardinality_stats = cardinality_stats.sort_values(
        "CardinalityProportionScaledUp", ascending=False
    )
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
    global most_expensive
    most_expensive = pd.read_sql_query(get_most_expensive, results_engine)
    max = most_expensive["TotalRuntimeOfQuery"].max()
    most_expensive["ComparativeQueryDuration"] = (
        most_expensive["TotalRuntimeOfQuery"] * 100
    )
    most_expensive["ComparativeQueryDuration"] = (
        most_expensive["ComparativeQueryDuration"] / max
    )
    most_expensive["ComparativeQueryDuration"] = most_expensive[
        "ComparativeQueryDuration"
    ].round(2)

    raw_queries["QueryStartTime"] = pd.to_datetime(raw_queries["QueryStartTime"])

    raw_queries_2 = raw_queries.copy()
    raw_queries_2["QueryStartTime"] = raw_queries_2["QueryStartTime"].dt.strftime(
        "%m-%d-%Y"
    )

    # Group the data by date and calculate the average runtime per day
    avg_runtime_per_day = raw_queries_2.groupby("QueryStartTime")[
        "QueryDurationSeconds"
    ].mean()

    # Group the data by date and calculate the number of queries per day
    global num_queries_per_day
    num_queries_per_day = raw_queries_2.groupby("QueryStartTime")["query_hash"].count()

    # Combine the two Series into a DataFrame
    result = pd.concat([avg_runtime_per_day, num_queries_per_day], axis=1)
    result.columns = ["AverageRuntime", "NumberOfQueries"]

    # Reset index to make 'date' a column again
    daily_queries = result.reset_index()

    # -----------------------------------------------------

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
    result_data["QueryStartTime"] = result_data["QueryStartTime"].dt.hour

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

    get_merge_expense = f"SELECT * FROM {selected_db}.write_statistics_merge_predicate"
    merge_expense = pd.read_sql_query(get_merge_expense, results_engine)

    max = merge_expense["AvgMergeRuntimeMs"].max()
    merge_expense["ComparativeMergeRuntime"] = merge_expense["AvgMergeRuntimeMs"] * 100
    merge_expense["ComparativeMergeRuntime"] = (
        merge_expense["ComparativeMergeRuntime"] / max
    )
    merge_expense["ComparativeMergeRuntime"] = merge_expense[
        "ComparativeMergeRuntime"
    ].round(2)

    grouped = merge_expense.groupby(
        ["TableName", "AvgMergeRuntimeMs", "ComparativeMergeRuntime"]
    )

    # Initialize an empty list to hold the final data
    data = []

    # Loop through each group
    for name, group in grouped:
        table_name, avg_merge_runtime_ms, comparative_merge_runtime = name

        # Convert each group into a list of dictionaries
        # (where each dictionary corresponds to a row in the original DataFrame)
        drilldown = group[
            [
                "ColumnName",
                "HasColumnInMergePredicate",
                "NumberOfVersionsPredicateIsUsed",
            ]
        ].to_dict("records")

        # Append the result to the final data list
        data.append(
            {
                "TableName": table_name,
                "AvgMergeRuntimeMs": avg_merge_runtime_ms,
                "ComparativeMergeRuntime": comparative_merge_runtime,
                "drilldown": drilldown,
            }
        )

    expensive_master_column_defs = [
        {
            "headerName": "Table Name",
            "field": "TableName",
            "cellRenderer": "agGroupCellRenderer",
        },
        {"headerName": "Average Merge Runtime ms", "field": "AvgMergeRuntimeMs"},
        {
            "headerName": "Comparative Merge Runtime (ms)",
            "field": "ComparativeMergeRuntime",
            "cellRenderer": "DBC_Progress2",
            "cellRendererParams": {
                "color": "rgb(27, 49, 57)",
                "label": True,
                "striped": True,
                "style": {"height": 30},
                "animated": True,
            },
        },
    ]

    expensive_detail_column_defs = [
        {"headerName": "Column Name", "field": "ColumnName"},
        {
            "headerName": "HasColumnInMergePredicate",
            "field": "HasColumnInMergePredicate",
        },
        {
            "headerName": "Number of Versions Predicate is Used In",
            "field": "NumberOfVersionsPredicateIsUsed",
        },
    ]

    return dmc.AccordionMultiple(
        children=[
            create_accordion_item(
                "Most Recent Strategy Result",
                [
                    create_ag_grid(
                        optimizer_results,
                        [
                            "UpdateTimestamp",
                        ],
                    )
                ],
                "mdi:recent",
            ),
            create_accordion_item(
                "Table Statistics",
                [
                    dcc.Graph(
                        figure=create_bar_chart(
                            results_stats,
                            "TableName",
                            "sizeInGB",
                            "sizeInGB",
                            "Top 10 Tables by Size (GB)",
                            10,
                        )
                    ),
                    create_ag_grid(
                        results_stats,
                        ["UpdateTimestamp", "sizeInBytes"],
                    ),
                ],
                "wpf:statistics",
            ),
            create_accordion_item(
                "Cardinality Sampling Statistics",
                [
                    # dcc.Graph(
                    #     id="cardinality_bar",
                    # ),
                    # dmc.Center(
                    #     style={"height": 50, "width": "100%"},
                    #     children=[
                    #         dmc.Group(
                    #             children=[
                    #                 dmc.Checkbox(
                    #                     id="reads",
                    #                     label="Tables used in Reads",
                    #                     size="xl",
                    #                     checked=True,
                    #                 ),
                    #                 dmc.Checkbox(
                    #                     id="writes",
                    #                     label="Tables used in Writes",
                    #                     size="xl",
                    #                     checked=True,
                    #                 ),
                    #             ],
                    #         )
                    #     ],
                    # ),
                    html.Div(
                        dag.AgGrid(
                            id="cardinality-ag-grid",
                            columnDefs=[
                                {"field": "TableName"},
                                {
                                    "field": "CardinalityProportionScaledUp",
                                    "cellRenderer": "DBC_Progress",
                                    "cellRendererParams": {
                                        "color": "rgb(27, 49, 57)",
                                        "label": True,
                                        "striped": True,
                                        "style": {"height": 30},
                                        "animated": True,
                                    },
                                },
                                {"field": "ColumnName"},
                                {"field": "SampleSize"},
                                {"field": "TotalCountInSample"},
                                {"field": "CardinalityProportionScaled"},
                                {"field": "CardinalityProportion"},
                            ],
                            rowData=cardinality_stats.to_dict(
                                orient="records"
                            ),  # Convert DataFrame to list of dicts
                            columnSize="sizeToFit",
                            style={"height": "550px"},
                            dashGridOptions={
                                "rowSelection": "multiple",
                            },
                            defaultColDef=dict(
                                resizable=True,
                                editable=True,
                                sortable=True,
                                autoHeight=True,
                                width=250,
                            ),
                        )
                    ),
                ],
                "material-symbols:bar-chart-4-bars",
            ),
            create_accordion_item(
                "Day-by-day Query Analysis",
                [
                    dcc.Graph(
                        id="daytoday",
                        figure=create_bar_line_query_daily_chart(daily_queries),
                    ),
                ],
                "ph:calendar",
            ),
            create_accordion_item(
                "Most Expensive Queries",
                [
                    # dcc.Graph(
                    #     figure=create_bar_chart(
                    #         most_expensive,
                    #         "query_hash",
                    #         "TotalRuntimePerDay",
                    #         "TotalRuntimePerDay",
                    #         "Longest Cumulative Running Queries Per Day (Drag and Drop to filter down for advanced analysis)",
                    #     ),
                    #     id="expensive-bar-chart",
                    # ),
                    html.Div(
                        dag.AgGrid(
                            id="expensiveAGGrid",
                            columnDefs=[
                                {"field": "query_hash"},
                                {
                                    "field": "ComparativeQueryDuration",
                                    "cellRenderer": "DBC_Progress2",
                                    "cellRendererParams": {
                                        "color": "rgb(27, 49, 57)",
                                        "label": True,
                                        "striped": True,
                                        "style": {"height": 30},
                                        "animated": True,
                                    },
                                },
                                {"field": "TotalRuntimeOfQuery"},
                                {"field": "query_text"},
                                {"field": "AvgDurationOfQuery"},
                                {"field": "TotalRunsOfQuery"},
                                {"field": "QueriesPerDay"},
                                {"field": "TotalRuntimePerDay"},
                            ],
                            rowData=most_expensive.to_dict(
                                orient="records"
                            ),  # Convert DataFrame to list of dicts
                            columnSize="sizeToFit",
                            style={"height": "550px"},
                            dashGridOptions={
                                "rowSelection": "multiple",
                            },
                            defaultColDef=dict(
                                resizable=True,
                                editable=True,
                                sortable=True,
                                autoHeight=True,
                                width=250,
                            ),
                        )
                    ),
                ],
                "streamline:money-cash-search-dollar-search-pay-product-currency-query-magnifying-cash-business-money-glass",
            ),
            create_accordion_item(
                "Most Expensive Merge/Delete Operations",
                [
                    html.Div(
                        dag.AgGrid(
                            id="expensive_ag_grid",
                            columnDefs=expensive_master_column_defs,
                            rowData=data,  # Convert DataFrame to list of dicts
                            columnSize="sizeToFit",
                            masterDetail=True,
                            detailCellRendererParams={
                                "detailGridOptions": {
                                    "columnDefs": expensive_detail_column_defs,
                                },
                                "detailColName": "drilldown",
                                "suppressCallback": True,
                            },
                            style={"height": "550px"},
                            dashGridOptions={
                                "rowSelection": "multiple",
                            },
                            defaultColDef=dict(
                                resizable=True,
                                editable=True,
                                sortable=True,
                                autoHeight=True,
                                width=250,
                            ),
                        )
                    )
                ],
                "ph:git-merge",
            ),
            create_accordion_item(
                "Raw Queries", [create_ag_grid(raw_queries)], "game-icons:raw-egg"
            ),
            create_accordion_item(
                "Dash Chart Editor Query Explorer",
                [
                    html.Div(
                        [
                            html.H4(
                                "Use Dash Chart Editor to create an interactive visualization."
                            ),
                            dce.DashChartEditor(
                                dataSources=most_expensive.to_dict("list"),
                                loadFigure=px.bar(
                                    most_expensive.sort_values(
                                        "AvgDurationOfQuery", ascending=False
                                    ),
                                    "query_hash",
                                    "AvgDurationOfQuery",
                                ).update_layout(
                                    xaxis={
                                        "tickmode": "array",
                                        "tickvals": list(
                                            range(len(most_expensive["query_hash"]))
                                        ),
                                        "ticktext": most_expensive["query_hash"]
                                        .str.slice(-8)
                                        .tolist(),
                                    }
                                ),
                            ),
                        ]
                    ),
                    create_ag_grid(most_expensive, ["TotalRuntimePerDay"]),
                ],
                "mdi:mountain",
            ),
        ],
    )


@callback(
    Output("cardinality_bar", "figure"),
    Output("cardinality-ag-grid", "rowData"),
    [Input("reads", "checked"), Input("writes", "checked")],
)
def update_cardinality(reads, writes):
    if reads and writes:
        filtered_df = cardinality_stats
    elif reads and not writes:
        filtered_df = cardinality_stats[cardinality_stats["IsUsedInReads"] == 1]
    elif not reads and writes:
        filtered_df = cardinality_stats[cardinality_stats["IsUsedInWrites"] == 1]
    else:
        filtered_df = pd.DataFrame()  # empty DataFrame
        return None, filtered_df

    fig = cardinality_bar_chart(
        filtered_df,
        "TableAndColumn",
        "CardinalityProportionScaled",
        "CardinalityProportionScaled",
        "Tables sorted by highest cardinality (proportion of unique data values)",
    )

    return fig, filtered_df.to_dict(orient="records")


@callback(
    Output("expensiveAGGrid", "rowData"), Input("expensive-bar-chart", "selectedData")
)
def update_expensive_ag_grid(selected_data):
    if not selected_data:
        return most_expensive.to_dict(orient="records")
    else:
        points = selected_data["points"]
        selected_indices = [point["label"] for point in points]
        filtered_df = most_expensive[
            most_expensive["query_hash"].isin(selected_indices)
        ]
        return filtered_df.to_dict(orient="records")


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
