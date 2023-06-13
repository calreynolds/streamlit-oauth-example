import dash
import json
import requests
from dash import html, callback, Input, Output, ctx, dcc, State
import dash_mantine_components as dmc
import subprocess

# import dash_dangerously_set_inner_html as ddsih
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/optimizer-runner", title="Runner")


SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"
engine = create_engine(
    f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
)
tables_stmt = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'optimizer_results';"
tables_in_db = pd.read_sql_query(tables_stmt, engine)
catalogs_init_statment = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.CATALOGS;"
catalog_list = pd.read_sql_query(catalogs_init_statment, engine)

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
            # dmc.Button(
            #     "Build Dataframe with Selected Tables",
            #     id="build-table",
            #     variant="gradient",
            #     size="lg",
            # ),
            dmc.Title("Run/Schedule Optimizer Strategy"),
            dmc.Space(h=20),
            dmc.Group(
                position="left",
                children=[
                    dmc.Button(
                        "Run Strategy", id="run-strategy-button", variant="outline"
                    ),
                    dmc.Button("Schedule", id="checksql", variant="outline"),
                    dcc.Input(id="schedule", type="text", value="0 0 10 * * ?"),
                ],
            ),
            dmc.Text(id="run-strategy-output"),
            dmc.Text(id="run-strategy-output-schedule"),
            # dmc.Space(h=10),
            # html.Div(
            #     children=[
            #         html.Label("Select Cluster:"),
            #         dcc.Dropdown(id="cluster-dropdown", options=[], value=None),
            #     ]
            # ),
            # dcc.Store(
            #     id="selected-cluster-store", storage_type="memory"
            # ),  # Make sure to include the dcc.Store component
            html.H5("Cluster Selection"),
            dcc.Dropdown(id="cluster-dropdown-step2"),
            html.Div(id="selected-cluster-output-step2"),
            dmc.Space(h=10),
            dmc.Button("Refresh Cluster", id="choose-cluster-button", n_clicks=0),
            html.H5("Select User"),
            dcc.Dropdown(id="group-members-dropdown-step2"),
            dmc.Space(h=10),
            dmc.Button("Get Users", id="refresh-button-step2"),
            html.H1("Select Optimization Strategy"),
            dmc.Space(h=10),
            dag.AgGrid(
                id="scheduler-grid",
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
            # dag.AgGrid(
            #     columnDefs=[
            #         {"headerName": i, "field": i} for i in catalog_list.columns
            #     ],
            #     rowData=catalog_list.to_dict("records"),
            #     columnSize="sizeToFit",
            #     defaultColDef=dict(
            #         resizable=True,
            #     ),
            # ),
            # dmc.Space(h=20),
            # dmc.Group(
            #     grow=True,
            #     children=[
            #         dmc.Button(
            #             "Build Database Tables",
            #             id="build_db_btn",
            #             variant="outline",
            #             size="lg",
            #         ),
            #         dmc.Button(
            #             "Drop Database Tables",
            #             id="drop_tabes_btn",
            #             variant="outline",
            #             size="lg",
            #         ),
            #         dmc.Button(
            #             "Get Table List",
            #             id="fetch_tables_btn",
            #             variant="outline",
            #             size="lg",
            #         ),
            #         dmc.Button(
            #             "Run ELT Pipeline",
            #             id="run_etl_pipe",
            #             variant="outline",
            #             size="lg",
            #         ),
            #     ],
            # ),
            # dmc.Space(h=20),
            dmc.SimpleGrid(
                cols=3,
                children=[
                    html.Div(
                        [
                            dmc.Text("Catalogs", align="center", weight=550),
                            dmc.Text(id="catalog_selection_output1", align="center"),
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
            dmc.Space(h=20),
            dmc.Text(id="container-button-timestamp", align="center"),
            dcc.Store(id="table_selection_store1"),
            dcc.Store(id="table_selection_store_now"),
            dcc.Store(id="schema_selection_store1"),
            dcc.Store(id="catalog_selection_store1"),
            html.Div(id="table_selection_output_now"),
        ]
    )


@callback(
    Output("group-members-dropdown-step2", "options"),
    Input("refresh-button-step2", "n_clicks"),
)
def refresh_group_members(n_clicks):
    if n_clicks:
        group_name = "admins"
        command = ["databricks", "groups", "list-members", "--group-name", group_name]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            output_json = json.loads(result.stdout)
            members = [member["user_name"] for member in output_json["members"]]
            member_options = [{"label": member, "value": member} for member in members]
            return member_options

    return []


@callback(
    Output("cluster-dropdown-step2", "options"),
    Input("choose-cluster-button", "n_clicks"),
)
def populate_cluster_dropdown(n_clicks):
    if n_clicks is None:
        return []

    # Databricks CLI command to retrieve all cluster statuses
    cluster_command = ["databricks", "clusters", "list", "--output", "json"]

    # Run the Databricks CLI command to retrieve cluster statuses
    cluster_result = subprocess.run(cluster_command, capture_output=True, text=True)

    # Parse the JSON output of the cluster command
    cluster_data = json.loads(cluster_result.stdout)

    # Extract the cluster IDs and labels
    cluster_options = [
        {"label": cluster["cluster_name"], "value": cluster["cluster_id"]}
        for cluster in cluster_data["clusters"]
    ]

    return cluster_options


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
    # [State("table_selection_output_now", "value")],
    prevent_initial_call=True,
)
def create_ag_grid(selected_table):
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
    Input("scheduler-grid", "selectedRows"),
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
    Input("scheduler-grid", "selectedRows"),
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
    Input("scheduler-grid", "selectedRows"),
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
        Input("schema_selection_output1", "value"),
    ],
    State("cluster-dropdown-step2", "value"),
    State("group-members-dropdown-step2", "value"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(n_clicks, selected_schema, selected_cluster, user_name):
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
    headers_auth2 = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/create"
    endp_resp2 = requests.post(uri2, data=job_json2, headers=headers_auth2).json()
    # Run Job
    optimize_job_two = endp_resp2["job_id"]
    run_now_uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/run-now"
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
    Input("checksql", "n_clicks"),
    Input("schedule", "value"),
    Input("schema_selection_output1", "value"),
    State("cluster-dropdown-step2", "value"),
    State("general-store", "data"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer_schedule(
    n_clicks, schedule, selected_cluster, selected_schema
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
                    "notebook_path": "/Repos/sach@streamtostream.com/edw-best-practices/Delta Optimizer/Step 2_ Strategy Runner",
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
    headers_auth2 = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/create"
    endp_resp2 = requests.post(uri2, data=job_json2, headers=headers_auth2).json()
    # Run Job
    optimize_job_two_schedule = endp_resp2["job_id"]
    run_now_uri2 = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/run-now"
    job_run_2 = {
        "job_id": {endp_resp2["job_id"]},
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
