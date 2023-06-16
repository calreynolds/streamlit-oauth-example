import os
import dash
import json
import requests
import sqlalchemy.exc
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
from dash.exceptions import PreventUpdate
import subprocess
from configparser import ConfigParser
from databricks.connect import DatabricksSession

dash.register_page(__name__, path="/build-strategy", title="Strategy Builder")


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


def layout():
    return (
        html.Div(
            [
                dmc.Title("Build Optimizer Strategy"),
                dmc.Divider(variant="solid"),
                dmc.Group(
                    position="left",
                    mt="xl",
                    children=[
                        dmc.Button(
                            "Build Strategy",
                            id="build-strategy",
                            variant="outline",
                            # color="#0f1d22",
                        ),
                        dmc.Button(
                            "Clear Selections",
                            id="clear-selection",
                            variant="default",
                            # color="#0f1d22",
                        ),
                        dmc.Button(
                            "Refresh",
                            id="refresh-button",
                            variant="default",
                        ),
                        html.Div(
                            id="engine-test-result",
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
                                    id="profile-dropdown-step1",
                                    options=[],
                                    value="Select Profile",
                                    style={
                                        "width": "200px",
                                        "position": "relative",
                                        "left": "45px",
                                        "top": "0px",
                                    },
                                )
                            ],
                        ),
                    ],
                ),
                dmc.Space(h=10),
                html.Div(id="job-id"),
                dmc.Space(h=10),
                dmc.Text(id="run-strategy-output", align="center"),
                dmc.Space(h=10),
                dmc.Text(id="run-strategy-output-schedule", align="center"),
                dmc.SimpleGrid(
                    [
                        dmc.TextInput(
                            id="outputdb",
                            label="Enter Delta Optimizer Output DB:",
                            placeholder="cataloge.database",
                        ),
                        dmc.TextInput(
                            id="optimizewarehouse",
                            label="Enter SQL Warehouse ID:",
                            placeholder="1234-123456-pane123",
                        ),
                        dmc.Stack(
                            [
                                dmc.NumberInput(
                                    id="optimizelookback",
                                    label="Enter Lookback Period in days:",
                                    stepHoldDelay=500,
                                    stepHoldInterval=100,
                                    min=1,
                                    value=3,
                                    style={"width": "300px"},
                                ),
                                dmc.Checkbox(id="startover", label="Start Over", mb=10),
                            ]
                        ),
                    ],
                    cols=2,
                ),
                dmc.Text(
                    align="center",
                    id="build-response",
                ),
                html.Div(id="load-optimizer-grid"),
                dmc.Space(h=10),
                dmc.Space(h=10),
                dmc.Space(h=15),
                dcc.Store(id="table_selection_store"),
                dcc.Store(id="schema_selection_store"),
                dcc.Store(id="catalog_selection_store"),
                dcc.Store(id="selected-profile-store"),
                dcc.Store(id="selected-cluster-store-step1"),
                dcc.Store(id="selected-cluster-store", storage_type="session"),
                dcc.Store(id="hostname-store", storage_type="memory"),
                dcc.Store(id="path-store", storage_type="memory"),
                dcc.Store(id="token-store", storage_type="memory"),
                dcc.Store(id="cluster-id-store", storage_type="memory"),
                dcc.Store(id="cluster-name-store", storage_type="memory"),
                dcc.Store(id="user-name-store", storage_type="memory"),
                dcc.Interval(
                    id="interval", interval=86400000, n_intervals=0
                ),  # Update every day
            ]
        ),
    )


@callback(
    Output("profile-dropdown-step1", "options"),
    Output("load-optimizer-grid", "children"),
    [Input("refresh-button", "n_clicks"), Input("interval", "n_intervals")],
    State("profile-dropdown-step1", "value"),
)
def populate_profile_dropdown(n_clicks, n_intervals, profile_name):
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
            and config.has_option(section, "cluster_name")
            and config.has_option(section, "cluster_id")
            and config.has_option(section, "user_name")
        ):
            options.append({"label": section, "value": section})

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
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            big_engine = create_engine(engine_url)

            tables_stmt = "SELECT table_catalog, table_schema, table_name, created, created_by, last_altered, last_altered_by FROM main.INFORMATION_SCHEMA.TABLES"
            tables_in_db = pd.read_sql_query(tables_stmt, big_engine)

            columnDefs = [
                {
                    "headerName": "Table Name",
                    "field": "table_name",
                    "checkboxSelection": True,
                    "headerCheckboxSelection": True,
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {
                        "buttons": ["apply", "reset"],
                    },
                    "minWidth": 180,
                },
                {
                    "headerName": "Database Name",
                    "field": "table_schema",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Catalog Name",
                    "field": "table_catalog",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                },
                {
                    "headerName": "Creator",
                    "field": "created_by",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Created On",
                    "field": "created",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Last Altered By",
                    "field": "last_altered_by",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Last Altered On",
                    "field": "last_altered",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
            ]

            rowData = tables_in_db.to_dict("records")

            optimizer_grid = [
                dag.AgGrid(
                    id="optimizer-grid",
                    enableEnterpriseModules=True,
                    columnDefs=columnDefs,
                    rowData=rowData,
                    style={"height": "550px"},
                    dashGridOptions={"rowSelection": "multiple", "sideBar": sideBar},
                    columnSize="sizeToFit",
                    defaultColDef=dict(
                        resizable=True,
                        editable=True,
                        sortable=True,
                        autoHeight=True,
                        width=90,
                    ),
                ),
                dmc.SimpleGrid(
                    cols=3,
                    children=[
                        html.Div(
                            [
                                dmc.Text("Catalogs", align="center", weight=550),
                                dmc.Text(id="catalog_selection_output", align="center"),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("DBs", align="center", weight=550),
                                dmc.Text(id="schema_selection_output", align="center"),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("Tables", align="center", weight=550),
                                dmc.Text(id="table_selection_output", align="center"),
                            ]
                        ),
                    ],
                ),
            ]

            return options, optimizer_grid

    return options, []


@callback(
    [
        Output("hostname-store", "data"),
        Output("token-store", "data"),
        Output("path-store", "data"),
        Output("cluster-name-store", "data"),
        Output("cluster-id-store", "data"),
        Output("user-name-store", "data"),
    ],
    [Input("profile-dropdown-step1", "value")],
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
    Output("engine-test-result", "children"),
    Input("profile-dropdown-step1", "value"),
    [
        State("hostname-store", "data"),
        State("path-store", "data"),
        State("token-store", "data"),
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
    Output("optimizer-grid", "selectedRows"),
    Output("outputdb", "value"),
    Output("optimizehostname", "value"),
    Output("optimizewarehouse", "value"),
    Output("optimizetoken", "value"),
    Output("optimizelookback", "value"),
    Output("startover", "checked"),
    Input("clear-selection", "n_clicks"),
    prevent_initial_call=True,
)
def restart_stepper(click):
    return [
        [],
        "",
        "",
        "",
        "",
        3,
        False,
    ]


@callback(
    Output("catalog_selection_output", "children"),
    Output("catalog_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
    prevent_initial_call=True,
)
def catalog(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_catalog_unique = set(selected_catalog)
        selected_catalog_unique_list = list(selected_catalog_unique)
        final = ",".join(selected_catalog_unique_list)
        # json_tables = json.dumps(final)
        return ", ".join(selected_catalog_unique_list), final
    return "No selections", dash.no_update


@callback(
    Output("schema_selection_output", "children"),
    Output("schema_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
    prevent_initial_call=True,
)
def schema(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_schema = [s["table_schema"] for s in selected]
        selected_schema_unique = set(selected_schema)
        selected_schema_unique_list = list(selected_schema_unique)
        final = [str("main." + i) for i in selected_schema_unique_list]
        final = ",".join(final)
        # json_tables = json.dumps(final)
        return ", ".join(selected_schema_unique_list), final
    return "No selections", dash.no_update


@callback(
    Output("table_selection_output", "children"),
    Output("table_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
    prevent_initial_call=True,
)
def tables(selected):
    if selected:
        selected_tables = [s["table_name"] for s in selected]
        selected_schema = [s["table_schema"] for s in selected]
        final = [
            "main." + schema + "." + table
            for schema, table in zip(selected_schema, selected_tables)
        ]
        final = ",".join(final)
        # json_tables = json.dumps(final)
        return ", ".join(selected_tables), final
    return "No selections", dash.no_update


@callback(
    [
        Output("build-response", "children"),
        Output("general-store", "data"),
    ],
    [Input("build-strategy", "n_clicks")],
    [
        State("optimizelookback", "value"),
        State("outputdb", "value"),
        State("hostname-store", "data"),
        State("optimizewarehouse", "value"),
        State("token-store", "data"),
        State("table_selection_store", "data"),
        State("schema_selection_store", "data"),
        State("catalog_selection_store", "data"),
        State("startover", "checked"),
        State("cluster-id-store", "data"),
        State("user-name-store", "data"),
    ],
    prevent_initial_call=True,
)
def delta_step_1_optimizer(
    n_clicks,
    lookback,
    outputdb,
    hostname,
    optimizewarehouse,
    token,
    tablelist,
    schemalist,
    cataloglist,
    startover,
    selected_cluster,
    user_name,
):
    if not n_clicks:
        raise PreventUpdate

    if not selected_cluster:
        return "Error: Please select a cluster.", None

    # Prepare the job payload
    optimize_job = {
        "name": "Delta_Optimizer_Step_1",
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "Delta_Optimizer-Step1",
                "notebook_task": {
                    "notebook_path": f"/Repos/{user_name}/edw-best-practices/Delta Optimizer/Step 1_ Optimization Strategy Builder",
                    "notebook_params": {
                        "Query History Lookback Period (days)": f"{lookback}",
                        "Optimizer Output Database:": f"{outputdb}",
                        "Server Hostname:": f"{hostname}",
                        "Catalog Filter Mode": "include_list",
                        "<dbx_token>": f"{token}",
                        "Catalog Filter List (Csv List)": f"{cataloglist}",
                        "Database Filter List (catalog.database) (Csv List)": schemalist,
                        "SQL Warehouse Ids (csv list)": f"{optimizewarehouse}",
                        "Table Filter Mode": "include_list",
                        "Database Filter Mode": "include_list",
                        "Table Filter List (catalog.database.table) (Csv List)": tablelist,
                        "Start Over?": "Yes" if startover else "No",
                    },
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": selected_cluster,
                "timeout_seconds": 0,
                "email_notifications": {"on_failure": [f"{user_name}"]},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
            }
        ],
        "format": "MULTI_TASK",
    }

    try:
        # Send the request to create the job
        create_job_uri = f"https://{hostname}/api/2.1/jobs/create"
        headers_auth = {"Authorization": f"Bearer {token}"}
        create_job_resp = requests.post(
            create_job_uri, json=optimize_job, headers=headers_auth
        ).json()

        if "job_id" in create_job_resp:
            job_id = create_job_resp["job_id"]

            # Send the request to run the job
            run_now_uri = f"https://{hostname}/api/2.1/jobs/run-now"
            job_run = {
                "job_id": job_id,
                "notebook_params": {
                    "Query History Lookback Period (days)": f"{lookback}",
                    "Optimizer Output Database:": f"{outputdb}",
                    "Server Hostname:": f"{hostname}",
                    "Catalog Filter Mode": "include_list",
                    "<dbx_token>": f"{token}",
                    "Catalog Filter List (Csv List)": f"{cataloglist}",
                    "Database Filter List (catalog.database) (Csv List)": schemalist,
                    "SQL Warehouse Ids (csv list)": optimizewarehouse,
                    "Table Filter Mode": "include_list",
                    "Database Filter Mode": "include_list",
                    "Table Filter List (catalog.database.table) (Csv List)": tablelist,
                    "Start Over?": f"{startover}",
                },
            }

            run_resp = requests.post(
                run_now_uri, json=job_run, headers=headers_auth
            ).json()

            if "run_id" in run_resp:
                return [
                    f"Optimizer ran with Job ID: {job_id}",
                    dmc.Space(h=10),
                    "You can now use the optimizer strategy to optimize your tables.",
                    dmc.Anchor(
                        "Delta Optimizer - Runner page.",
                        href=dash.get_relative_path("/optimizer-runner"),
                        target="_blank",
                        style={"font-size": "14px"},
                    ),
                ], job_id

            # Handle specific error conditions
            if "error" in run_resp:
                error_message = run_resp["error"]["message"]
                if "run_id" in run_resp["error"]:
                    job_id = run_resp["error"]["run_id"]
                    return (
                        f"Error occurred while running the optimizer: {error_message} (Job ID: {job_id})",
                        None,
                    )
                else:
                    return (
                        f"Error occurred while running the optimizer: {error_message}",
                        None,
                    )

        return "Error occurred while creating the job.", None

    except requests.exceptions.RequestException as e:
        return f"Error occurred while running the optimizer: {str(e)}", None
