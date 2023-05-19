import dash
import json
import requests
from dash import html, dcc, callback, Input, Output, State
import dash_mantine_components as dmc
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path="/optimizer", title="Delta Optimizer")

SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
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
            dmc.Space(h=15),
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
            dmc.Space(h=20),
            dmc.Divider(variant="dashed", color="#FF3621"),
            dmc.Space(h=5),
            dmc.SimpleGrid(
                [
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
                    dmc.Group(
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
            dmc.Space(h=40),
            # Stepper
            html.Div(
                [
                    dmc.Stepper(
                        id="stepper-basic-usage",
                        active=0,
                        breakpoint="sm",
                        color="#FF3621",
                        children=[
                            dmc.StepperStep(
                                label="First Step",
                                description="Strategy Builder",
                                id="stepper-step-1",
                            ),
                            dmc.StepperStep(
                                label="Second step",
                                description="Strategy Execution",
                                children=dmc.Text("Optimize", align="center"),
                                id="stepper-step-2",
                            ),
                            dmc.StepperStep(
                                label="Final step",
                                description="Analyze Profile",
                                id="stepper-step-3",
                            ),
                            dmc.StepperCompleted(
                                children=dmc.Text(
                                    align="center",
                                    children=[
                                        "Successfully finished, you may view reuslts under ",
                                        dmc.Anchor(
                                            "Delta Optimizer - Results page.",
                                            href=dash.get_relative_path(
                                                "/optimizer-results"
                                            ),
                                        ),
                                    ],
                                )
                            ),
                        ],
                    ),
                    dmc.Group(
                        position="center",
                        mt="xl",
                        children=[
                            dmc.Button(
                                "Restart",
                                id="stepper-restart",
                                variant="default",
                                color="#FF3621",
                            ),
                            dmc.Button(
                                "Next step",
                                id="stepper-next",
                                variant="outline",
                                color="#FF3621",
                            ),
                        ],
                    ),
                ]
            ),
            dcc.Store(id="table_selection_store"),
            dcc.Store(id="schema_selection_store"),
            dcc.Store(id="catalog_selection_store"),
        ]
    )


@callback(
    Output("stepper-basic-usage", "active", allow_duplicate=True),
    Input("stepper-restart", "n_clicks"),
    prevent_initial_call=True,
)
def restart_stepper(restart):
    return 0


@callback(
    # Output("stepper-basic-usage", "active"),
    Output("stepper-step-1", "loading"),
    Output("stepper-step-2", "loading"),
    Output("stepper-step-3", "loading"),
    Input("stepper-next", "n_clicks"),
    State("stepper-basic-usage", "active"),
    prevent_initial_call=True,
)
def update_stepper(next_, step):
    step_1_loading = step == 0
    step_2_loading = step == 1
    step_3_loading = step == 2
    return step_1_loading, step_2_loading, step_3_loading


@callback(
    Output("catalog_selection_output", "children"),
    Output("catalog_selection_store", "data"),
    Input("optimizer-grid", "selectedRows"),
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
    Output("stepper-step-2", "children"),
    Output("stepper-step-1", "loading", allow_duplicate=True),
    Output("stepper-basic-usage", "active", allow_duplicate=True),
    Output("stepper-next", "disabled", allow_duplicate=True),
    Input("stepper-step-1", "loading"),
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
    loading_trigger,
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
    if not loading_trigger:
        raise PreventUpdate

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
                        "Database Filter List (catalog.database) (Csv List)": schemalist,
                        "SQL Warehouse Ids (csv list)": f"{optimizewarehouse}",
                        "Table Filter Mode": "include_list",
                        "Database Filter Mode": "include_list",
                        "Table Filter List (catalog.database.table) (Csv List)": tablelist,
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
            "Database Filter List (catalog.database) (Csv List)": schemalist,
            "SQL Warehouse Ids (csv list)": optimizewarehouse,
            "Table Filter Mode": "include_list",
            "Database Filter Mode": "include_list",
            "Table Filter List (catalog.database.table) (Csv List)": tablelist,
            "Start Over?": f"{startover}",
        },
    }
    job_run_json = json.dumps(job_run)
    run_resp = requests.post(
        run_now_uri, data=job_run_json, headers=headers_auth
    ).json()
    msg = f"Optimizer Ran with Job Id: {endp_resp['job_id']} \n run message: {run_resp}"
    return dmc.Text(msg, align="center"), False, 1, False


@callback(
    Output("stepper-step-3", "children"),
    Output("stepper-step-2", "loading", allow_duplicate=True),
    Output("stepper-basic-usage", "active", allow_duplicate=True),
    Output("stepper-next", "disabled", allow_duplicate=True),
    Input("stepper-step-2", "loading"),
    State("outputdb", "value"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(loading_trigger, outputdpdn2):
    if not loading_trigger:
        raise PreventUpdate

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
    msg2 = (
        f"Optimizer Ran with Job Id: {endp_resp2['job_id']} \n run message: {run_resp2}"
    )
    return dmc.Text(msg2, align="center"), False, 2, False


@callback(
    Output("stepper-step-3", "loading", allow_duplicate=True),
    Output("stepper-basic-usage", "active", allow_duplicate=True),
    Output("stepper-next", "disabled", allow_duplicate=True),
    Input("stepper-step-3", "loading"),
    State("outputdb", "value"),
    prevent_initial_call=True,
)
def delta_step_3_optimizer_analyze(loading_trigger, outputdb):
    if not loading_trigger:
        raise PreventUpdate

    return False, 3, True
