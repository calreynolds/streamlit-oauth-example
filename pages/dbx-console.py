import dash
import json
import requests
from dash import html, callback, Input, Output, ctx
import dash_mantine_components as dmc
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/dbx-console", title="Console")


SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"
engine = create_engine(
    f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}/?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
)
tables_stmt = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.TABLES;"
tables_in_db = pd.read_sql_query(tables_stmt, engine)
catalogs_init_statment = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.CATALOGS;"
catalog_list = pd.read_sql_query(catalogs_init_statment, engine)

columnDefs = [
    {
        "headerName": x,
        "field": x,
        "filter": True,
        "floatingFilter": True,
        "filterParams": {"buttons": ["apply", "reset"]},
    }
    for x in tables_in_db.columns
]
rowData = tables_in_db.to_dict("records")


def layout():
    return html.Div(
        children=[
            dag.AgGrid(
                id="downloadable-grid",
                columnDefs=columnDefs,
                rowData=rowData,
                columnSize="autoSize",
                defaultColDef=dict(
                    resizable=True,
                    editable=True,
                    sortable=True,
                    autoHeight=True,
                ),
            ),
            dmc.Space(h=20),
            dag.AgGrid(
                columnDefs=[
                    {"headerName": i, "field": i} for i in catalog_list.columns
                ],
                rowData=catalog_list.to_dict("records"),
                columnSize="sizeToFit",
                defaultColDef=dict(
                    resizable=True,
                ),
            ),
            dmc.Space(h=20),
            dmc.Group(
                grow=True,
                children=[
                    dmc.Button(
                        "Build Database Tables",
                        id="build_db_btn",
                        variant="outline",
                        size="lg",
                    ),
                    dmc.Button(
                        "Drop Database Tables",
                        id="drop_tabes_btn",
                        variant="outline",
                        size="lg",
                    ),
                    dmc.Button(
                        "Get Table List",
                        id="fetch_tables_btn",
                        variant="outline",
                        size="lg",
                    ),
                    dmc.Button(
                        "Run ELT Pipeline",
                        id="run_etl_pipe",
                        variant="outline",
                        size="lg",
                    ),
                ],
            ),
            dmc.Space(h=20),
            dmc.Text(id="container-button-timestamp", align="center"),
        ]
    )


@callback(
    Output("container-button-timestamp", "children"),
    Input("build_db_btn", "n_clicks"),
    Input("drop_tabes_btn", "n_clicks"),
    Input("fetch_tables_btn", "n_clicks"),
    Input("run_etl_pipe", "n_clicks"),
)
def displayClick(btn1, btn2, btn3, btn4):
    if "run_etl_pipe" == ctx.triggered_id:
        # Build and Trigger Databricks Jobs
        job_req = {
            "name": "Plotly_Backend_Pipeline",
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "Plotly_Backend_Pipeline",
                    "sql_task": {
                        "query": {"query_id": "2a7db015-6b16-4d7e-87b8-bbb6a77ea4a0"},
                        "warehouse_id": "07bdd5688d399f3d",
                    },
                    "timeout_seconds": 0,
                    "email_notifications": {},
                }
            ],
            "format": "MULTI_TASK",
        }

        job_json = json.dumps(job_req)
        # Get this from a secret or param
        headers_auth = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        uri = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/create"

        endp_resp = requests.post(uri, data=job_json, headers=headers_auth).json()

        # Run Job
        # job_req = endp_resp["job_id"]

        run_now_uri = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/run-now"

        job_run = {"job_id": 1055779903432172}
        job_run_json = json.dumps(job_run)

        run_resp = requests.post(
            run_now_uri, data=job_run_json, headers=headers_auth
        ).json()

        msg = f"Pipeline Created and Ran with Job Id: {endp_resp['job_id']} \n run message: {run_resp}"
    else:
        msg = "No Database State Yet..."
    return msg
