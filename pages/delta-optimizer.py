import dash
import json
import requests
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/optimizer", title="Delta Optimizer")

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
catalogs_init_statment = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.CATALOGS;"
catalog_list = pd.read_sql_query(catalogs_init_statment, big_engine)


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
            dmc.Title("Select Tables to Optimize:"),
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
            dmc.TextInput(
                id="outputdb",
                label="Enter Delta Optimizer Output DB:",
                placeholder="cataloge.database",
            ),
            dmc.TextInput(
                id="optimizehostname",
                label="Enter Server Hostname:",
                placeholder="dbc-a2c61234-1234.cloud.databricks.com",
            ),
            dmc.TextInput(
                id="optimizewarehouse",
                label="Enter SQL Warehouse ID:",
                placeholder="1234-123456-pane123",
            ),
            dmc.TextInput(
                id="optimizetoken",
                label="Enter Access Token:",
                placeholder="token",
            ),
            dmc.NumberInput(
                id="optimizelookback",
                label="Enter Lookback Period in days:",
                stepHoldDelay=500,
                stepHoldInterval=100,
                value=3,
            ),
            dmc.Checkbox(
                id="startover", label="Start Over", mb=10
            ),  # todo change input
            dmc.Group(
                grow=True,
                children=[
                    dmc.Button(
                        "Run Step 1 in Delta Optimizer",
                        id="stepone_optimizer",
                    ),
                    dmc.Button(
                        "Run Step 2 in Delta Optimizer",
                        id="steptwo_optimizer",
                        disabled=True,
                    ),
                    dmc.Button(
                        "Analyze Results of Delta Optimizer",
                        id="stepthree_optimizer",
                        disabled=True,
                    ),
                ],
            ),
            html.Div(id="statuswindow"),
            html.Div(id="jobtwowindow"),
            html.Div(id="table_selection_output"),
            html.Div(id="schema_selection_output"),
            html.Div(id="catalog_selection_output"),
            html.Div(id="optimizer-analyze-result"),
            dcc.Store(id="table_selection_store"),
            dcc.Store(id="schema_selection_store"),
            dcc.Store(id="catalog_selection_store"),
        ]
    )


@callback(
    Output("table_selection_output", "children"),
    Output("table_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
)
def selected(selected):
    if selected:
        selected_tables = [s["table_name"] for s in selected]
        json_tables = json.dumps(selected_tables)
        return f"You selected tables: {selected_tables}", json_tables
    return "No selections", dash.no_update


@callback(
    Output("schema_selection_output", "children"),
    Output("schema_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
)
def selected(selected):
    if selected:
        selected_schema = [s["table_schema"] for s in selected]
        selected_schema_unique = set(selected_schema)
        selected_schema_unique_list = list(selected_schema_unique)
        json_tables = json.dumps(selected_schema_unique_list)
        return f"You selected databases: {selected_schema_unique}", json_tables
    return "No selections", dash.no_update


@callback(
    Output("catalog_selection_output", "children"),
    Output("catalog_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
)
def selected(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_catalog_unique = set(selected_catalog)
        selected_catalog_unique_list = list(selected_catalog_unique)
        json_tables = json.dumps(selected_catalog_unique_list)
        return f"You selected catalogs: {selected_catalog_unique}", json_tables
    return "No selections", dash.no_update


@callback(
    Output("statuswindow", "children"),
    Output("steptwo_optimizer", "disabled"),
    Input("stepone_optimizer", "n_clicks"),
    State("optimizelookback", "value"),
    State("outputdb", "value"),
    State("optimizehostname", "value"),
    State("optimizewarehouse", "value"),
    State("optimizetoken", "value"),
    State("table_selection_store", "data"),
    State("schema_selection_store", "data"),
    State("catalog_selection_store", "data"),
    State("startover", "checked"),
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
):
    # Build and Trigger Databricks Jobs
    if "stepone_optimizer" == ctx.triggered_id:
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
                        "notebook_path": "/Repos/sach@streamtostream.com/edw-best-practices/Delta Optimizer/Step 1_ Optimization Strategy Builder",
                        "notebook_params": {
                            "Query History Lookback Period (days)": f"{lookback}",
                            "Optimizer Output Database:": f"{outputdb}",
                            "Server Hostname:": f"{hostname}",
                            "Catalog Filter Mode": "include_list",
                            "<dbx_token>": f"{token}",
                            "Catalog Filter List (Csv List)": f"{cataloglist}",
                            "Database Filter List (catalog.database) (Csv List)": f"{schemalist}",
                            "SQL Warehouse Ids (csv list)": f"{optimizewarehouse}",
                            "Table Filter Mode": "include_list",
                            "Database Filter Mode": "include_list",
                            "Table Filter List (catalog.database.table) (Csv List)": f"{tablelist}",
                            "Start Over?": "Yes" if startover else "No",
                        },
                        "source": "WORKSPACE",
                    },
                    "existing_cluster_id": "0510-131932-sflv6c6d",
                    "libraries": [
                        {
                            "whl": "dbfs:/FileStore/jars/3dd43508_9b7a_4c5b_ba46_5917b1755868/deltaoptimizer-1.4.0-py3-none-any.whl"
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {"on_failure": ["sach@streamtostream.com"]},
                    "notification_settings": {
                        "no_alert_for_skipped_runs": False,
                        "no_alert_for_canceled_runs": False,
                        "alert_on_last_attempt": False,
                    },
                }
            ],
            "format": "MULTI_TASK",
        }
        job_json = json.dumps(optimize_job)
        # Get this from a secret or param
        headers_auth = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        uri = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/create"
        endp_resp = requests.post(uri, data=job_json, headers=headers_auth).json()
        # Run Job
        optimize_job = endp_resp["job_id"]
        run_now_uri = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/run-now"
        job_run = {
            "job_id": 20751302339346,
            "notebook_params": {
                "Query History Lookback Period (days)": f"{lookback}",
                "Optimizer Output Database:": f"{outputdb}",
                "Server Hostname:": f"{hostname}",
                "Catalog Filter Mode": "include_list",
                "<dbx_token>": f"{token}",
                "Catalog Filter List (Csv List)": f"{cataloglist}",
                "Database Filter List (catalog.database) (Csv List)": f"{schemalist}",
                "SQL Warehouse Ids (csv list)": WAREHOUSE_ID,
                "Table Filter Mode": "include_list",
                "Database Filter Mode": "include_list",
                "Table Filter List (catalog.database.table) (Csv List)": f"{tablelist}",
                "Start Over?": f"{startover}",
            },
        }
        job_run_json = json.dumps(job_run)
        run_resp = requests.post(
            run_now_uri, data=job_run_json, headers=headers_auth
        ).json()
        msg = f"Optimizer Ran with Job Id: {endp_resp['job_id']} \n run message: {run_resp}"
        return html.Div(msg), False


@callback(
    Output("jobtwowindow", "children"),
    Output("stepthree_optimizer", "disabled"),
    Input("steptwo_optimizer", "n_clicks"),
    State("outputdb", "value"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(n_clicks, outputdpdn2):
    # Build and Trigger Databricks Jobs
    if "steptwo_optimizer" == ctx.triggered_id:
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
                            "Optimizer Output Database:": f"{outputdpdn2}",
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
                "Optimizer Output Database:": f"{outputdpdn2}",
                "exclude_list(csv)": "",
                "include_list(csv)": "",
                "table_mode": "include_all_tables",
            },
        }
        job_run_json2 = json.dumps(job_run_2)
        run_resp2 = requests.post(
            run_now_uri2, data=job_run_json2, headers=headers_auth2
        ).json()
        msg2 = f"Optimizer Ran with Job Id: {endp_resp2['job_id']} \n run message: {run_resp2}"
        return html.Div(msg2), False


@callback(
    Output("optimizer-analyze-result", "children"),
    Input("stepthree_optimizer", "n_clicks"),
    State("outputdb", "value"),
    prevent_initial_call=True,
)
def delta_step_3_optimizer_analyze(n_clicks, outputdb):
    results_engine = create_engine(
        f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={outputdb}"
    )
    get_optimizer_results = f"Select * FROM {outputdb}.optimizer_results"
    optimizer_results = pd.read_sql_query(get_optimizer_results, results_engine)
    get_results_stats = f"Select * FROM {outputdb}.all_tables_table_stats"
    results_stats = pd.read_sql_query(get_results_stats, results_engine)
    get_cardinality = f"Select * FROM {outputdb}.all_tables_cardinality_stats WHERE IsUsedInReads = 1 OR IsUsedInWrites = 1"
    cardinality_stats = pd.read_sql_query(get_cardinality, results_engine)
    get_raw_queries = f"SELECT * from_unixtime(query_start_time_ms/1000) AS QueryStartTime, from_unixtime(query_end_time_ms/1000) AS QueryEndTime, duration/1000 AS QueryDurationSeconds FROM {outputdb}.raw_query_history_statistics"

    return "Successfully finished, you may view reuslts under Delta Optimizer - Results page."
