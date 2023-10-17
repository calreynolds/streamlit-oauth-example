import os
import dash
import time
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
import components as comp
from components import GitSource, Schedule, Library, NewCluster, NotebookTask
from flask import session
from databricks.sdk.oauth import OAuthClient, Consent, SessionCredentials
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")
DATABRICKS_APP_URL = os.environ.get("DATABRICKS_APP_URL")
APP_NAME= "delta-optimizer"


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
    return dmc.MantineProvider(
        children=dmc.NotificationsProvider(
            [
                html.Div(
                    id="engine-test-result",
                    className="engine-test-result",
                    # style={
                    #     "width": "600px",
                    #     "position": "relative",
                    #     "left": "20px",
                    #     "top": "0px",
                    # },
                ),
                html.Div(
                    [
                        html.Div(id="notifications-container-step1"),
                        html.Div(id="cluster-loading-notification"),
                        html.Div(id="cluster-loaded-notification"),
                        dmc.Title(
                            "Build Optimizer Strategy",
                            style={
                                "fontSize": "24px",
                                "marginTop": "5px",  # Adjust the font size as needed
                            },
                        ),
                        dmc.Space(h=10),
                        dmc.Divider(variant="solid"),
                        dmc.Space(h=10),
                        # html.Div(id="job-id"),
                        # dmc.Text(id="run-strategy-output", align="right"),
                        # dmc.Text(id="run-strategy-output-schedule", align="center"),
                        dmc.Group(
                            position="left",
                            children=[
                                dmc.TextInput(
                                    id="outputdb",
                                    label="Name Output DB:",
                                    placeholder="catalog.database",
                                    className="input-field",
                                ),
                                dmc.TextInput(
                                    id="optimizewarehouse",
                                    label="SQL Warehouse:",
                                    placeholder="1234-123456-pane123",
                                    className="input-field",
                                ),
                                dmc.NumberInput(
                                    id="optimizelookback",
                                    label="Lookback Days:",
                                    stepHoldDelay=500,
                                    stepHoldInterval=100,
                                    min=1,
                                    value=3,
                                    style={"width": "100px"},
                                ),
                                dmc.Switch(
                                    id="startover",
                                    label="Start Over",
                                    style={
                                        "width": "200px",
                                        "position": "relative",
                                        "left": "20px",
                                        "top": "10px",
                                    },
                                ),
                            ],
                        ),
                        dmc.Text(
                            align="center",
                            id="build-response",
                            style={
                                "width": "500px",
                                "position": "relative",
                                "left": "400px",
                                "top": "0px",
                            },
                        ),
                        dmc.Space(h=10),
                        html.Div(
                            children=dmc.LoadingOverlay(
                                html.Div("Loading...", id="load-optimizer-grid"),
                                loaderProps={
                                    "variant": "dots",
                                    "color": "orange",
                                    "size": "xxl",
                                    # Adjust the loader style here
                                },
                            )
                        ),
                        dmc.Space(h=10),
                        dmc.Group(
                            position="left",
                            mt="xl",
                            children=[
                                dmc.Button(
                                    "Build Strategy",
                                    id="build-strategy",
                                    variant="outline",
                                ),
                                dmc.Button(
                                    "Clear Selections",
                                    id="clear-selections",
                                    variant="default",
                                ),
                                dmc.Button(
                                    "Refresh",
                                    id="refresh-button",
                                    variant="default",
                                ),
                            ],
                        ),
                        dmc.Space(h=10),
                        dmc.Space(h=15),
                        dcc.Store(id="table_selection_store"),
                        dcc.Store(id="schema_selection_store"),
                        dcc.Store(id="catalog_selection_store"),
                        dcc.Store(id="selected-profile-store"),
                        dcc.Store(id="selected-cluster-store-step1"),
                        dcc.Store(id="selected-cluster-store", storage_type="session"),
                        dcc.Store(id="cluster-state-store", storage_type="memory"),
                        dcc.Store(id="hostname-store", storage_type="memory"),
                        dcc.Store(id="path-store", storage_type="memory"),
                        dcc.Store(id="token-store", storage_type="memory"),
                        dcc.Store(id="profile-store", storage_type="local"),
                        dcc.Interval(
                            id="interval", interval=86400000, n_intervals=0
                        ),  # Update every day
                        dcc.Interval(id="interval_test", interval=1000, n_intervals=0),
                        html.Div(id="dummy-output1"),
                    ]
                ),
            ]
        )
    )


@callback(
    Output("load-optimizer-grid", "children"),
    Input("refresh-button", "n_clicks"),
    State("profile-store", "data"),
)
def populate_profile_dropdown(n_clicks, profile_name):
    if n_clicks or profile_name:
        if profile_name:
            config = ConfigParser()
            file_path = os.path.expanduser("./.databrickscfg")
            print(profile_name)
            if os.path.exists(file_path):
                config.read(file_path)

            options = []

            for section in config.sections():
                if (
                    config.has_option(section, "host")
                    and config.has_option(section, "path")
                    and config.has_option(section, "token")
                ):
                    options.append({"label": section, "value": section})

            if config.has_section(profile_name):
                

                
                

                columnDefs = [
                    {
                        "headerName": "Table Name",
                        "field": "table_name",
                        "checkboxSelection": True,
                        # "headerCheckboxSelection": True,
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
                        "minWidth": 180,
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
            
            host = config.get(profile_name, "host")
            path = config.get(profile_name, "path")
            token = config.get(profile_name, "token")
            host = host.replace("https://", "")
            if host and token and path:
                    engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog={catalog}&schema={schema}"
            print(engine_url)
            big_engine = create_engine(engine_url)
            tables_stmt = "SELECT table_catalog, table_schema, table_name, created, created_by, last_altered, last_altered_by FROM system.INFORMATION_SCHEMA.TABLES"    
            tables_in_db = pd.read_sql_query(tables_stmt, big_engine)
            rowData = tables_in_db.to_dict("records")

            optimizer_grid = [
                dag.AgGrid(
                    id="optimizer-grid",
                    enableEnterpriseModules=True,
                    columnDefs=columnDefs,
                    rowData=rowData,
                    style={"height": "550px"},
                    dashGridOptions={"rowSelection": "multiple"},
                    columnSize="sizeToFit",
                    defaultColDef=dict(
                        resizable=True,
                        editable=True,
                        sortable=True,
                        autoHeight=True,
                        width=90,
                    ),
                    className="ag-theme-alpine",
                ),
            ]

            return optimizer_grid

    return []


@callback(
    [
        Output("hostname-store", "data"),
        Output("token-store", "data"),
        Output("path-store", "data"),
    ],
    [Input("profile-store", "value")],
    prevent_initial_call=True,
)
def parse_databricks_config(profile_name):
    if profile_name:
        print(profile_name + "test")
        config = ConfigParser()
        file_path = os.path.expanduser("./.databrickscfg")

        if os.path.exists(file_path):
            config.read(file_path)

            if config.has_section(profile_name):
                host = config.get(profile_name, "host")
                token = config.get(profile_name, "token")
                path = config.get(profile_name, "path")
                host = host.replace("https://", "")
                print(host + "test")
                print(token + "test")
                print(path + "test")

                return host, token, path

    return (
        None,
        None,
        None,
    )


@callback(
    Output("hidden", "children"),
    [Input("profile-store", "data")],
    [
        State("hostname-store", "data"),
        State("path-store", "data"),
        State("token-store", "data"),
    ],
)
def your_callback_function(profile_name, host, path, token):
    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
        engine = create_engine(engine_url)
        result = engine.execute("SELECT 1").fetchone()

        return str(result)

    return "Invalid configuration"


@callback(
    [
        Output("cluster-loading-notification", "children"),
        Output("cluster-loaded-notification", "children"),
        Output("engine-test-result", "children"),
    ],
    [
        # Input("profile-dropdown-step1", "value"),
        Input("refresh-button", "n_clicks"),
    ],
    [
        State("profile-store", "data"),
        State("hostname-store", "data"),
        State("path-store", "data"),
        State("token-store", "data"),
    ],
)
def get_cluster_state(n_clicks, profile_name, host, path, token):
    if n_clicks or profile_name:
        if profile_name:
            host, token, path = parse_databricks_config(profile_name)
            if host and token and path:
                sqlwarehouse = path.replace("/sql/1.0/warehouses/", "")
                print(sqlwarehouse)
                print(host)
                print(token)
                try:
                    test_job_uri = (
                        f"https://{host}/api/2.0/sql/warehouses/{sqlwarehouse}"
                    )
                    # print(test_job_uri)
                    headers_auth = {"Authorization": f"Bearer {token}"}
                    test_job = requests.get(test_job_uri, headers=headers_auth).json()
                    # print(test_job)

                    if test_job["state"] == "TERMINATED":
                        return (
                            comp.cluster_loading("Cluster is loading..."),
                            dash.no_update,
                            dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="engine-connection-badge",
                                    variant="gradient",
                                    color="yellow",
                                    gradient={"from": "yeloow", "to": "orange"},
                                    size="lg",
                                    children=[
                                        html.Span(f"Connecting to Workspace: {host} ")
                                    ],
                                ),
                            ),
                        )

                    if test_job["state"] == "STARTING":
                        return (
                            comp.cluster_loading("Cluster is loading..."),
                            dash.no_update,
                            dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="engine-connection-badge",
                                    variant="dot",
                                    gradient={"from": "yellow", "to": "orange"},
                                    color="yellow",
                                    size="lg",
                                    children=[
                                        html.Span(
                                            f"Connecting to Workspace: {host}",
                                            style={"color": "white"},
                                        )
                                    ],
                                ),
                            ),
                        )
                    elif test_job["state"] == "RUNNING":
                        return (
                            dash.no_update,
                            comp.cluster_loaded("Cluster is loaded"),
                            dmc.LoadingOverlay(
                                dmc.Badge(
                                    id="engine-connection-badge",
                                    variant="dot",
                                    # gradient={"from": "teal", "to": "lime", "deg": 105},
                                    color="green",
                                    size="lg",
                                    children=[
                                        html.Span(
                                            f"Connected to: {host} ",
                                            style={"color": "white"},
                                        )
                                    ],
                                ),
                            ),
                        )

                except Exception as e:
                    print(f"Error occurred while testing engine connection: {str(e)}")

    return dash.no_update, dash.no_update, dash.no_update


# @callback(
#     Output("optimizer-grid", "selectedRows"),
#     Output("outputdb", "value"),
#     # Output("optimizehostname", "value"),
#     Output("optimizewarehouse", "value"),
#     # Output("optimizetoken", "value"),
#     Output("optimizelookback", "value"),
#     Output("startover", "checked"),
#     Input("clear-selections", "n_clicks"),
#     prevent_initial_call=True,
# )
# def restart_stepper(click):
#     return [
#         [],
#         "",
#         "",
#         3,
#         False,
#     ]


@callback(
    # Output("catalog_selection_output", "children"),
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
    # Output("schema_selection_output", "children"),
    Output("schema_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
    prevent_initial_call=True,
)
def schema(selected):
    if selected:
        selected_schema = [s["table_schema"] for s in selected]
        selected_schema_unique = set(selected_schema)
        selected_schema_unique_list = list(selected_schema_unique)
        final = [str("main." + i) for i in selected_schema_unique_list]
        final = ",".join(final)
        # json_tables = json.dumps(final)
        return ", ".join(selected_schema_unique_list), final
    return "No selections", dash.no_update


@callback(
    # Output("table_selection_output", "children"),
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
        # Output("general-store", "data"),
        Output("notifications-container-step1", "children"),
        Output("outputdb", "value"),
        Output("load-optimizer-grid", "selectedRows"),
        Output("optimizewarehouse", "value"),
        Output("optimizelookback", "value"),
        Output("startover", "checked"),
    ],
    [
        Input("build-strategy", "n_clicks"),
        Input("clear-selections", "n_clicks"),
    ],
    [
        State("optimizelookback", "value"),
        State("outputdb", "value"),
        State("optimizewarehouse", "value"),
        State("table_selection_store", "data"),
        State("schema_selection_store", "data"),
        State("catalog_selection_store", "data"),
        State("startover", "checked"),
        State("profile-store", "data"),
    ],
    prevent_initial_call=True,
)
def test_states(
    build_n_clicks,
    clear_n_clicks,
    optimizelookback,
    outputdb,
    optimizewarehouse,
    table_selection,
    schema_selection,
    catalog_selection,
    startover,
    profile_data,
):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    # If clear-selections button was clicked
    if button_id == "clear-selections":
        return (
            dash.no_update,
            dash.no_update,
            dash.no_update,
            [],  # Reset selectedRows
            "",  # Reset outputdb value
            "",  # Reset optimizewarehouse value
            3,  # Reset optimizelookback value
            False,  # Reset startover checked state
        )

    if not build_n_clicks:
        raise PreventUpdate

    # Check for mandatory fields
    if any(
        field is None or field == ""
        for field in [
            optimizelookback,
            outputdb,
            table_selection,
            schema_selection,
            catalog_selection,
            optimizewarehouse,
        ]
    ):
        return (
            dash.no_update,
            dash.no_update,
            comp.notification_job1_error("Please fill out all fields."),
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    # Print the state values to the terminal
    print(f"Lookback Value: {optimizelookback}")
    print(f"Output DB Value: {outputdb}")
    print(f"Warehouse Value: {optimizewarehouse}")
    print(f"Table Selection: {table_selection}")
    print(f"Schema Selection: {schema_selection}")
    print(f"Catalog Selection: {catalog_selection}")
    print(f"Start Over Checked: {startover}")
    print(f"Profile Data: {profile_data}")

    # Get the necessary data and construct the job configuration
    if profile_data:
        print(profile_data + "test")
    config = ConfigParser()
    file_path = os.path.expanduser("./.databrickscfg")

    if os.path.exists(file_path):
        config.read(file_path)

    if config.has_section(profile_data):
        host_full = config.get(profile_data, "host")
        token = config.get(profile_data, "token")
        path = config.get(profile_data, "path")
        host = host_full.replace("https://", "")

        oauth_client = OAuthClient(
        host=DATABRICKS_HOST,
        client_id=DATABICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
        redirect_url=DATABRICKS_APP_URL,
        scopes=["all-apis"]
        )
        consent = oauth_client.initiate_consent(
        )
        session["consent"] = consent.as_dict()
        credentials_provider = SessionCredentials.from_dict(oauth_client, session["creds"])
        w= WorkspaceClient(
            host=oauth_client.host,
            product=APP_NAME,
            credentials_provider=credentials_provider
        )
        base_parameters = {
            "Query History Lookback Period (days)": str(optimizelookback),
            "Optimizer Output Database:": str(outputdb),
            "Server Hostname:": str(host),
            "Catalog Filter Mode": "include_list",
            "Access Token:": str(token),
            "Catalog Filter List (Csv List)": str(catalog_selection),
            "Database Filter List (catalog.database) (Csv List)": str(schema_selection),
            "SQL Warehouse Ids (csv list)": str(optimizewarehouse),
            "Table Filter Mode": "include_list",
            "Database Filter Mode": "include_list",
            "Table Filter List (catalog.database.table) (Csv List)": str(
                table_selection
            ),
            "Start Over?": "Yes" if startover else "No",
        }

        created_job = w.jobs.create(
            name=f"{outputdb} Optimizer Strategy",
            git_source=GitSource(
                git_url="https://github.com/noshadeson/edw-best-practices.git",
                git_provider="GITHUB",
                git_branch="patch-1",
            ),
            max_concurrent_runs=1,
            schedule=Schedule("0 0 10 * * ?", "US/Pacific", "UNPAUSED"),
            tasks=[
                jobs.Task(
                    task_key="Delta_Optimizer-Step1",
                    notebook_task=NotebookTask(
                        "Delta Optimizer/Step 1_ Optimization Strategy Builder",
                        base_parameters,
                    ),
                    new_cluster=NewCluster(
                        "i3.xlarge",
                        "12.1.x-scala2.12",
                        8,
                        {"spark.databricks.delta.preview.enabled": "true"},
                        {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                        True,
                    ),
                    # aws_attributes={"availability": "ON_DEMAND"},
                    libraries=[
                        Library("dbfs:/tmp/deltaoptimizer-1.5.0-py3-none-any.whl"),
                    ],
                    timeout_seconds=0,
                ),
            ],
            timeout_seconds=0,
        )

        job_id = created_job.job_id
        print(job_id)
        run_resp = w.jobs.run_now(job_id=created_job.job_id)
        print(run_resp)
        run_id = run_resp.response.run_id
        print(run_id)

    if run_id is not None:
        return (
            [
                f"Optimizer ran with Job ID: {job_id}. You can now use the optimizer strategy to optimize your tables.",
            ],
            job_id,
            comp.notification_user_step_1(f"Optimizer ran with Job ID: {job_id}"),
            [],  # Reset selectedRows
            "",  # Reset outputdb value
            "",  # Reset optimizewarehouse value
            False,  # Reset startover checked state
        )

    if "error" in run_resp.response:
        error_message = run_resp["error"]["message"]
        error_message_string = str(error_message)

        if "run_id" in run_resp["error"]:
            job_id = run_resp["error"]["run_id"]
            return (
                f"Error occurred while running the optimizer: {error_message} (Job ID: {job_id})",
                None,
                comp.notification_user_step_1(
                    f"Error while running optimizer{error_message_string}"
                ),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )
        else:
            return (
                f"Error occurred while running the optimizer: {error_message}",
                None,
                comp.notification_user_step_1(error_message_string),
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
                dash.no_update,
            )

    return (
        "Error occurred while creating the job.",
        None,
        None,
        dash.no_update,
        dash.no_update,
        dash.no_update,
        dash.no_update,
        dash.no_update,
    )
