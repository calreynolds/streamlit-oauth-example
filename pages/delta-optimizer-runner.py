import os
import dash
import json
import requests
from dash import html, callback, Input, Output, ctx, dcc, State
import dash_mantine_components as dmc
import subprocess
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
                    dmc.Button(
                        "Refresh",
                        id="refresh-button-step2",
                        variant="default",
                    ),
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
                            ),
                        ],
                    ),
                ],
            ),
            dmc.Space(h=40),
            html.Div(
                [
                    dmc.Slider(
                        id="slider-callback",
                        value=26,
                        min=2,
                        max=20,
                        marks=[
                            {"value": 4, "label": "4 Workers"},
                            {"value": 10, "label": "10 Workers"},
                            {"value": 18, "label": "18 Workers"},
                        ],
                        mb=35,
                    ),
                    dmc.Text(id="slider-output"),
                ]
            ),
            dmc.Space(h=10),
            dmc.Tabs(
                [
                    dmc.TabsList(
                        [
                            dmc.Tab("Strategy Picker", value="Strategy Picker"),
                            dmc.Tab("Jobs Console", value="Jobs Console"),
                        ]
                    ),
                    dmc.TabsPanel(
                        html.Div(
                            [
                                dmc.Text(id="run-strategy-output"),
                                dmc.Space(h=10),
                                dmc.Text(id="run-strategy-output-schedule"),
                                dmc.Space(h=10),
                                html.Div(id="load-optimizer-grid-step2"),
                                dmc.Space(h=10),
                                html.Div(id="table_selection_output_now"),
                            ]
                        ),
                        value="Strategy Picker",
                    ),
                    dmc.TabsPanel(
                        html.Div(
                            [
                                dmc.Space(h=10),
                                html.Div(id="jobs-grid"),
                                dmc.Space(h=20),
                                dmc.Group(
                                    position="left",
                                    children=[
                                        dmc.Button(
                                            "Delete Selected Jobs", id="delete-button"
                                        ),
                                        dmc.TextInput(
                                            id="new-cron-expression",
                                            type="text",
                                            value="0 0 10 * * ?",
                                        ),
                                        dmc.Button(
                                            "Update Schedule",
                                            id="update-schedule-button",
                                            variant="default",
                                        ),
                                        dmc.Button(
                                            "Pause/Unpase Selected Jobs",
                                            id="pause-button",
                                            variant="outline",
                                        ),
                                        # dmc.Button(
                                        #     "Unpause Selected Jobs",
                                        #     id="unpause-button",
                                        #     variant="outline",
                                        # ),
                                    ],
                                ),
                                html.Div(id="pause-message"),
                                html.Div(id="job_selection_output1"),
                                html.Div(id="schedule-change-message"),
                                html.Div(id="delete-message"),
                                html.Div(id="run-list"),
                            ]
                        ),
                        value="Jobs Console",
                    ),
                ],
                value="Job Runner",
            ),
            dmc.Space(h=10),
            dmc.Space(h=30),
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
            dcc.Store(id="job_selection_store1", storage_type="memory"),
            dcc.Store(id="job_grid_step2", storage_type="memory"),
            dcc.Store(id="slider_store", storage_type="memory"),
            dcc.Interval(id="interval2", interval=86400000, n_intervals=0),
        ]
    )


@callback(
    Output("slider-output", "children"),
    Output("slider_store", "data"),
    Input("slider-callback", "value"),
    State("slider_store", "data"),
)
def update_value(value, slider_data):
    slider_data = int(slider_data) if slider_data else None
    return f"You have selected: {value}", slider_data


@callback(
    Output("run-list", "children"),
    Input("job_selection_store1", "data"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def get_job_runs(job_id, hostname, token):
    if job_id:
        # Make a GET request to the Runs API
        url = f"https://{hostname}/api/2.1/jobs/runs/list"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"job_id": job_id}
        response = requests.get(url, headers=headers, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the run information from the response
            run_list = response.json().get("runs", [])

            # Create a list of HTML elements to display the runs
            run_divs = [
                html.Div(
                    [
                        html.Div(f"Run ID: {run['run_id']}"),
                        html.Div(f"Status: {run['state']['life_cycle_state']}"),
                        html.Div(f"Start Time: {run['start_time']}"),
                        html.Div(f"End Time: {run['end_time']}"),
                        # Add more fields as needed
                    ]
                )
                for run in run_list
            ]

            return run_divs

    return html.Div("No job selected.")


@callback(
    [
        Output("hostname-store2", "data"),
        Output("token-store2", "data"),
        Output("path-store2", "data"),
        # Output("cluster-name-store2", "data"),
        # Output("cluster-id-store2", "data"),
        # Output("user-name-store2", "data"),
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
                # cluster_name = config.get(profile_name, "cluster_name")
                # cluster_id = config.get(profile_name, "cluster_id")
                # user_name = config.get(profile_name, "user_name")
                host = host.replace("https://", "")

                return host, token, path

    return None, None, None


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
            # and config.has_option(section, "cluster_name")
            # and config.has_option(section, "cluster_id")
            # and config.has_option(section, "user_name")
        ):
            options.append({"label": section, "value": section})

    if profile_name:
        (
            host,
            token,
            path,
            # cluster_name,
            # cluster_id,
            # user_name,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            big_engine = create_engine(engine_url)

            tables_stmt = f"SELECT * FROM {CATALOG}.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'optimizer_results';"
            tables_in_db = pd.read_sql_query(tables_stmt, big_engine)

            columnDefs = [
                # {
                #     "headerName": "Table Name",
                #     "field": "table_name",
                #     "checkboxSelection": True,
                #     "headerCheckboxSelection": True,
                #     "filter": True,
                #     "floatingFilter": True,
                #     "filterParams": {
                #         "buttons": ["apply", "reset"],
                #     },
                #     "minWidth": 180,
                # },
                {
                    "headerName": "Strategy Name",
                    "field": "table_schema",
                    "checkboxSelection": True,
                    "headerCheckboxSelection": True,
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                # {
                #     "headerName": "Catalog Name",
                #     "field": "table_catalog",
                #     "filter": True,
                #     "floatingFilter": True,
                #     "filterParams": {"buttons": ["apply", "reset"]},
                # },
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
def test_engine_connection(profile_name, host, path, token):
    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            # Modify the path to remove "/sql/1.0/warehouses/"
            # sql_warehouse = path.replace("/sql/1.0/warehouses/", "")
            engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
            engine = create_engine(engine_url)

            try:
                # Test the engine connection by executing a sample query
                with engine.connect() as connection:
                    result = connection.execute("SELECT 1")
                    test_value = result.scalar()

                    if test_value == 1:
                        return dmc.LoadingOverlay(
                            dmc.Badge(
                                id="engine-connection-badge",
                                variant="dot",
                                color="green",
                                size="lg",
                                children=[
                                    html.Span(f"Connected to Workspace: {host} ")
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
                        id="engine-connection-badge",
                        variant="dot",
                        color="red",
                        size="lg",
                        children=[html.Span(f"Engine Connection failed: {str(e)}")],
                    ),
                    loaderProps={
                        "variant": "dots",
                        "color": "orange",
                        "size": "xl",
                    },
                )

    return html.Div("Please select a profile.", style={"color": "orange"})


@callback(
    Output("job_selection_output1", "children"),
    Output("job_selection_store1", "data"),
    Input("job-grid-step2", "selectedRows"),
)
def display_selected_jobs(selected_rows):
    if selected_rows:
        selected_jobs = [selected_rows[i]["job_id"] for i in range(len(selected_rows))]
        selected_jobs_list = [html.Li(str(job_id)) for job_id in selected_jobs]
        return selected_jobs_list, selected_jobs

    return html.Div("No selections"), []


import subprocess


@callback(
    Output("delete-message", "children"),
    Input("delete-button", "n_clicks"),
    State("job-grid-step2", "selectedRows"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def delete_selected_jobs(n_clicks, selected_rows, hostname, token):
    if n_clicks is not None and n_clicks > 0:
        if not selected_rows:
            return html.Div("Please select jobs to delete.")

        # Get the selected job IDs
        selected_job_ids = [row["job_id"] for row in selected_rows]

        # Delete each selected job using CLI command
        delete_count = 0

        for job_id in selected_job_ids:
            cli_command = f"databricks jobs delete --job-id {job_id}"
            process = subprocess.Popen(cli_command, shell=True)
            process.wait()

            if process.returncode == 0:
                delete_count += 1

        if delete_count > 0:
            return html.Div(f"{delete_count} jobs deleted successfully.")
        else:
            return html.Div("Error deleting jobs.")

    return html.Div()


@callback(
    Output("pause-message", "children"),
    Input("pause-button", "n_clicks"),
    State("job-grid-step2", "selectedRows"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def toggle_pause_selected_jobs(n_clicks, selected_rows, hostname, token):
    if n_clicks is not None and n_clicks > 0:
        if not selected_rows:
            return html.Div("Please select jobs to toggle pause status.")

        # Get the selected job IDs
        selected_job_ids = [row["job_id"] for row in selected_rows]

        # Toggle pause/unpause status for each selected job
        toggle_count = 0

        for job_id in selected_job_ids:
            # Make a GET request to the Jobs API to retrieve the current job details
            url = f"https://{hostname}/api/2.1/jobs/get"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"job_id": job_id}
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                job_details = response.json()
                job_details["new_settings"] = job_details.pop("settings")

                current_pause_status = job_details["new_settings"]["schedule"].get(
                    "pause_status"
                )
                new_pause_status = (
                    "PAUSED" if current_pause_status == "UNPAUSED" else "UNPAUSED"
                )

                # Update the payload with the new pause status
                payload = {
                    **job_details,
                    "new_settings": {
                        **job_details["new_settings"],
                        "schedule": {
                            **job_details["new_settings"]["schedule"],
                            "pause_status": new_pause_status,
                        },
                    },
                }

                # Send the payload to the Jobs API to update the job
                update_url = f"https://{hostname}/api/2.1/jobs/reset"
                headers = {"Authorization": f"Bearer {token}"}
                response_new = requests.post(update_url, headers=headers, json=payload)
                print(payload)
                print(response_new.status_code)

                if response_new.status_code == 200:
                    toggle_count += 1

        if toggle_count > 0:
            return html.Div(f"{toggle_count} jobs have been toggled successfully.")
        else:
            return html.Div("Error toggling pause status for jobs.")

    return html.Div()


import subprocess
import json


@callback(
    Output("schedule-change-message", "children"),
    Input("update-schedule-button", "n_clicks"),
    State("job-grid-step2", "selectedRows"),
    State("new-cron-expression", "value"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def change_schedule(n_clicks, selected_rows, new_schedule, hostname, token):
    if n_clicks is not None and n_clicks > 0:
        if not selected_rows:
            return html.Div("Please select a job to update its schedule.")

        job_id = selected_rows[0]["job_id"]  # Assuming only one job is selected

        # Prepare the payload with the updated schedule
        payload = {
            "job_id": job_id,
            "new_settings": {
                "schedule": {
                    "quartz_cron_expression": new_schedule,
                    "timezone_id": "US/Pacific",  # Set the desired timezone ID
                }
            },
        }

        # Make a POST request to update the job schedule
        url = f"https://{hostname}/api/2.1/jobs/update"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            return html.Div("Schedule updated successfully.")
        else:
            return html.Div(f"Error updating schedule: {response.text}")

    return html.Div()


@callback(
    Output("jobs-grid", "children"),
    Input("refresh-button-step2", "n_clicks"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
)
def get_job_list(n_clicks, workspace_url, access_token):
    if n_clicks is not None and n_clicks > 0:
        # Make a GET request to the Jobs API
        response = requests.get(
            f"https://{workspace_url}/api/2.1/jobs/list",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the job information from the response
            job_list = response.json().get("jobs", [])

            # Convert the job list to a Pandas DataFrame
            df = pd.DataFrame(job_list)

            # Select columns to display in the table

            columnDefs = [
                {
                    "headerName": "Job ID",
                    "field": "job_id",
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
                    "headerName": "Job Name",
                    "field": "settings.name",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Schedule",
                    "field": "settings.schedule.quartz_cron_expression",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                },
                {
                    "headerName": "Timezone",
                    "field": "settings.schedule.timezone_id",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Pause Status",
                    "field": "settings.schedule.pause_status",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Creator",
                    "field": "creator_user_name",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
                {
                    "headerName": "Created On",
                    "field": "created_time",
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 180,
                },
            ]

            rowData = df.to_dict("records")

            jobs_grid = [
                dag.AgGrid(
                    id="job-grid-step2",
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
            ]

            # Return the jobs_grid
            return jobs_grid

    # Return an empty list if the button hasn't been clicked yet or the request was not successful
    return []

    # Return an empty DataFram

    # Return an empty div if the button hasn't been clicked yet
    return html.Div()


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
            # and config.has_option(section, "cluster_name")
            # and config.has_option(section, "cluster_id")
            # and config.has_option(section, "user_name")
        ):
            options.append({"label": section, "value": section})

    if profile_name:
        (
            host,
            token,
            path,
            # cluster_name,
            # cluster_id,
            # user_name,
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
    State("slider-callback", "value"),
    State("schema_selection_store1", "data"),
    # State("cluster-id-store2", "data"),
    # State("user-name-store2", "data"),
    State("hostname-store2", "data"),
    State("token-store2", "data"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(n_clicks, slider, selected_schema, hostname, token):
    optimize_job_two = {
        "name": f"{selected_schema} Optimizer Run",
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
                    "notebook_path": "Delta Optimizer/Step 2_ Strategy Runner",
                    "source": "GIT",
                    "notebook_params": {
                        "Optimizer Output Database:": selected_schema,
                        "exclude_list(csv)": "",
                        "include_list(csv)": "",
                        "table_mode": "include_all_tables",
                    },
                },
                "new_cluster": {
                    "node_type_id": "i3.xlarge",
                    "spark_version": "7.3.x-scala2.12",
                    "num_workers": slider,
                    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                },
                "aws_attributes": {
                    "availability": "ON_DEMAND",
                    # "zone_id": "us-west-2a",
                },
                "libraries": [
                    {
                        "whl": "dbfs:/FileStore/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"
                    },
                ],
                # "existing_cluster_id": selected_cluster,
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
            }
        ],
        "git_source": {
            "git_url": "https://github.com/CodyAustinDavis/edw-best-practices.git",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
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
        State("slider-callback", "value"),
        State("schema_selection_store1", "data"),
        # State("cluster-id-store2", "data"),
        # State("user-name-store2", "data"),
        State("hostname-store2", "data"),
        State("token-store2", "data"),
    ],
    prevent_initial_call=True,
)
def delta_step_2_optimizer_schedule(
    n_clicks, schedule, slider, selected_schema, hostname, token
):
    optimize_job_two_schedule = {
        "name": f"{selected_schema} Optimizer Run",
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
                    "notebook_path": "Delta Optimizer/Step 2_ Strategy Runner",
                    "source": "GIT",
                    "notebook_params": {
                        "Optimizer Output Database:": selected_schema,
                        "exclude_list(csv)": "",
                        "include_list(csv)": "",
                        "table_mode": "include_all_tables",
                    },
                },
                "new_cluster": {
                    "node_type_id": "i3.xlarge",
                    "spark_version": "7.3.x-scala2.12",
                    "num_workers": slider,
                    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                },
                "aws_attributes": {
                    "availability": "ON_DEMAND",
                    # "zone_id": "us-west-2a",
                },
                "libraries": [
                    {
                        "whl": "dbfs:/FileStore/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"
                    },
                ],
                # "existing_cluster_id": selected_cluster,
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": False,
                    "no_alert_for_canceled_runs": False,
                    "alert_on_last_attempt": False,
                },
            }
        ],
        "git_source": {
            "git_url": "https://github.com/CodyAustinDavis/edw-best-practices.git",
            "git_provider": "gitHub",
            "git_branch": "main",
        },
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
