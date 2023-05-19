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
            dmc.Title("Build Optimizer Strategy"),
            dmc.Space(h=10),
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
            dmc.Space(h=10),
            dmc.Text(
                align="center",
                id="build-response",
            ),
            # Stepper
            dmc.Group(
                position="center",
                mt="xl",
                children=[
                    dmc.Button(
                        "Clear Selections",
                        id="clear-selection",
                        variant="default",
                        color="#FF3621",
                    ),
                    dmc.Button(
                        "Build Strategy",
                        id="build-strategy",
                        variant="outline",
                        color="#FF3621",
                    ),
                ],
            ),
            dmc.Space(h=40),
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
            dcc.Store(id="table_selection_store"),
            dcc.Store(id="schema_selection_store"),
            dcc.Store(id="catalog_selection_store"),
        ]
    )


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
    Output("build-response", "children"),
    Output("general-store", "data"),
    Input("build-strategy", "n_clicks"),
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
    if not n_clicks:
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
    return [
        f"Optimizer Ran with Job Id: {endp_resp['job_id']} \n run message: {run_resp} successfully finished.",
        dmc.Space(h=10),
        "You may view reuslts under ",
        dmc.Anchor(
            "Delta Optimizer - Results page.",
            href=dash.get_relative_path("/optimizer-results"),
        ),
    ], {"outputdpdn2": outputdb}
