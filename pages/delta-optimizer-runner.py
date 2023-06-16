import os
import dash
import json
import requests
from dash import html, callback, Input, Output, ctx, dcc, State
import dash_mantine_components as dmc
import subprocess
from databricks.connect import DatabricksSession
import sqlalchemy.exc
from configparser import ConfigParser

# import dash_dangerously_set_inner_html as ddsih
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/optimizer-runner", title="Runner")
CATALOG = "main"
SCHEMA = "information_schema"

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
    return html.Div(
        [
            dmc.Title("Run+Schedule Optimizer Strategy"),
            dmc.Divider(variant="solid"),
            dmc.Space(h=20),
            dmc.Group(
                position="left",
                children=[
                    dmc.Button(
                        "Run Strategy", id="run-strategy-button", variant="outline"
                    ),
                    dmc.Button("Schedule", id="checksql", variant="outline"),
                    dmc.TextInput(id="schedule", type="text", value="0 0 10 * * ?"),
                    html.Div(
                        id="engine-test-result-step2",
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
                                id="profile-dropdown-step2",
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
            dmc.Space(h=10),
            dmc.Button(
                "Refresh",
                id="refresh-button-step2",
                variant="default",
            ),
            dmc.Text(id="run-strategy-output"),
            dmc.Space(h=10),
            dmc.Text(id="run-strategy-output-schedule"),
            dmc.Space(h=30),
            html.Div(id="load-optimizer-grid-step2"),
            dmc.Space(h=20),
            dmc.Space(h=20),
            dmc.Text(id="container-button-timestamp", align="center"),
            dcc.Store(id="table_selection_store1"),
            dcc.Store(id="table_selection_store_now"),
            dcc.Store(id="schema_selection_store1"),
            dcc.Store(id="catalog_selection_store1"),
            dcc.Store(id="hostname-store2", storage_type="memory"),
            dcc.Store(id="path-store2", storage_type="memory"),
            dcc.Store(id="token-store2", storage_type="memory"),
            dcc.Store(id="cluster-id-store2", storage_type="memory"),
            dcc.Store(id="cluster-name-store2", storage_type="memory"),
            dcc.Store(id="user-name-store2", storage_type="memory"),
            dcc.Interval(id="interval2", interval=86400000, n_intervals=0),
            html.Div(id="table_selection_output_now"),
        ]
    )


@callback(
    [
        Output("hostname-store2", "data"),
        Output("token-store2", "data"),
        Output("path-store2", "data"),
        Output("cluster-name-store2", "data"),
        Output("cluster-id-store2", "data"),
        Output("user-name-store2", "data"),
    ],
    [Input("profile-dropdown-step2", "value")],
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
    Output("profile-dropdown-step2", "options"),
    Output("load-optimizer-grid-step2", "children"),
    [Input("refresh-button-step2", "n_clicks"), Input("interval2", "n_intervals")],
    State("profile-dropdown-step2", "value"),
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

            tables_stmt = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'optimizer_results';"
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
                    id="optimizer-grid-step2",
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
                dmc.Space(h=20),
                dmc.SimpleGrid(
                    cols=3,
                    children=[
                        html.Div(
                            [
                                dmc.Text("Catalogs", align="center", weight=550),
                                dmc.Text(
                                    id="catalog_selection_output1", align="center"
                                ),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("DBs", align="center", weight=550),
                                dmc.Text(id="schema_selection_output1", align="center"),
                            ]
                        ),
                        html.Div(
                            [
                                dmc.Text("Tables", align="center", weight=550),
                                dmc.Text(id="table_selection_output1", align="center"),
                            ]
                        ),
                    ],
                ),
            ]

            return options, optimizer_grid

    return options, []


@callback(
    Output("engine-test-result-step2", "children"),
    Input("profile-dropdown-step2", "value"),
    [
        State("hostname-store2", "data"),
        State("path-store2", "data"),
        State("token-store2", "data"),
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
    Output("selected-cluster-output-step2", "children"),
    Input("cluster-dropdown-step2", "value"),
)
def display_selected_cluster(selected_cluster):
    return f"Selected Cluster: {selected_cluster}"


@callback(
    [
        Output("table_selection_output_now", "children"),
        Output("table_selection_store_now", "data"),
    ],
    [Input("table_selection_store1", "data")],
    State("profile-dropdown-step2", "value"),
    prevent_initial_call=True,
)
def create_ag_grid(selected_table, profile_name):
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
            engine = create_engine(engine_url)
    stmt = f"select * from {selected_table}"
    df = pd.read_sql_query(stmt, engine)
    columnDefs = [
        {
            "headerName": x,
            "field": x,
            "filter": True,
            "floatingFilter": True,
            "filterParams": {"buttons": ["apply", "reset"]},
        }
        for x in df.columns
    ]
    rowData = df.to_dict("records")

    ag_grid_component = (
        dag.AgGrid(
            id="table-grid",
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
    )

    return ag_grid_component, selected_table


@callback(
    Output("catalog_selection_output1", "children"),
    Output("catalog_selection_store1", "data"),
    Input("optimizer-grid-step2", "selectedRows"),
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
    Output("schema_selection_output1", "children"),
    Output("schema_selection_store1", "data"),
    Input("optimizer-grid-step2", "selectedRows"),
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
    Output("table_selection_output1", "children"),
    Output("table_selection_store1", "data"),
    Input("optimizer-grid-step2", "selectedRows"),
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
    Output("run-strategy-output", "children"),
    [
        Input("run-strategy-button", "n_clicks"),
    ],
    State("schema_selection_store1", "data"),
    State("cluster-id-store2", "data"),
    State("user-name-store2", "data"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(
    n_clicks, selected_schema, selected_cluster, user_name, hostname, token
):
    if not selected_cluster:
        return "Please select a cluster first."
    optimize_job_two = {
        "name": "Delta_Optimizer_Step_2",
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "schedule": {
            "quartz_cron_expression": "0 0 10 * * ?",
            "timezone_id": "US/Pacific",
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "Delta_Optimizer_Step_2",
                "notebook_task": {
                    "notebook_path": f"/Repos/{user_name}/edw-best-practices/Delta Optimizer/Step 2_ Strategy Runner",
                    "notebook_params": {
                        "Optimizer Output Database:": selected_schema,
                        "exclude_list(csv)": "",
                        "include_list(csv)": "",
                        "table_mode": "include_all_tables",
                    },
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": selected_cluster,
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
    headers_auth2 = {"Authorization": f"Bearer {token}"}
    uri2 = f"https://{hostname}/api/2.1/jobs/create"
    endp_resp2 = requests.post(uri2, data=job_json2, headers=headers_auth2).json()
    # Run Job
    optimize_job_two = endp_resp2["job_id"]
    run_now_uri2 = f"https://{hostname}/api/2.1/jobs/run-now"
    job_run_2 = {
        "job_id": endp_resp2["job_id"],
        "notebook_params": {
            "Optimizer Output Database:": selected_schema,
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
    return html.Pre(msg2)


@callback(
    Output("run-strategy-output-schedule", "children"),
    [
        Input("checksql", "n_clicks"),
        Input("schedule", "value"),
        # Input("schema_selection_output1", "value"),
    ],
    [
        State("schema_selection_store1", "data"),
        State("cluster-id-store2", "data"),
        State("user-name-store2", "data"),
        State("hostname-store2", "data"),
        State("token-store2", "data"),
    ],
    prevent_initial_call=True,
)
def delta_step_2_optimizer_schedule(
    n_clicks, schedule, selected_schema, selected_cluster, user_name, hostname, token
):
    optimize_job_two_schedule = {
        "name": "Delta_Optimizer_Step_2",
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "schedule": {
            "quartz_cron_expression": schedule,
            "timezone_id": "US/Pacific",
            "pause_status": "UNPAUSED",
        },
        "tasks": [
            {
                "task_key": "Delta_Optimizer_Step_2",
                "notebook_task": {
                    "notebook_path": f"/Repos/{user_name}/edw-best-practices/Delta Optimizer/Step 2_ Strategy Runner",
                    "notebook_params": {
                        "Optimizer Output Database:": selected_schema,
                        "exclude_list(csv)": "",
                        "include_list(csv)": "",
                        "table_mode": "include_all_tables",
                    },
                    "source": "WORKSPACE",
                },
                "existing_cluster_id": f"{selected_cluster}",
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
    job_json2 = json.dumps(optimize_job_two_schedule)
    # Get this from a secret or param
    headers_auth2 = {"Authorization": f"Bearer {token}"}
    uri2 = f"https://{hostname}/api/2.1/jobs/create"
    endp_resp2 = requests.post(uri2, data=job_json2, headers=headers_auth2).json()
    # Run Job
    optimize_job_two_schedule = endp_resp2["job_id"]
    run_now_uri2 = f"https://{hostname}/api/2.1/jobs/run-now"
    job_run_2 = {
        "job_id": endp_resp2["job_id"],  # Removed the curly braces
        "notebook_params": {
            "Optimizer Output Database:": selected_schema,
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
