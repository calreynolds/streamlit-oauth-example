import os
import dash
import json
import requests
from dash import html, callback, Input, Output, ctx, dcc, State
import dash_mantine_components as dmc
import subprocess
import sqlalchemy.exc
from dash_iconify import DashIconify
from configparser import ConfigParser
import components as comp
from dash.exceptions import PreventUpdate
from components import GitSource, Schedule, Library, NewCluster, NotebookTask
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# import dash_dangerously_set_inner_html as ddsih
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine

dash.register_page(__name__, path="/delta-optimizer/optimizer-runner", title="Runner")
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
    return dmc.MantineProvider(
        children=dmc.NotificationsProvider(
            [
                html.Div(
                    id="engine-test-result-step2",
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
                        html.Div(id="cluster-loading-notification-step2"),
                        html.Div(id="cluster-loaded-notification-step2"),
                        html.Div(id="run-strategy-notification"),
                        html.Div(id="run-strategy-notification-schedule"),
                        html.Div(id="schedule-notification"),
                        html.Div(id="delete-notification"),
                        html.Div(id="pause-notification"),
                        dmc.Title(
                            "Run+Schedule Optimizer Strategy",
                            style={
                                "fontSize": "24px",
                                "marginTop": "5px",  # Adjust the font size as needed
                            },
                        ),
                        dmc.Space(h=10),
                        dmc.Divider(variant="solid"),
                        dmc.Space(h=20),
                        dmc.Group(
                            position="left",
                            children=[
                                dmc.Button(
                                    "Run Strategy",
                                    id="run-strategy-now-button",
                                    variant="outline",
                                ),
                                dmc.Button(
                                    "Schedule",
                                    id="run-strategy-button",
                                    variant="outline",
                                ),
                                dmc.TextInput(
                                    id="schedule", type="text", value="0 0 10 * * ?"
                                ),
                                dmc.Button(
                                    "Refresh",
                                    id="refresh-button-step2",
                                    variant="default",
                                ),
                            ],
                        ),
                        dmc.Space(h=40),
                        html.Div(
                            [
                                dmc.Slider(
                                    id="slider-callback",
                                    value=8,
                                    min=2,
                                    max=12,
                                    marks=[
                                        {"value": 3, "label": "3 Workers"},
                                        {"value": 5, "label": "5 Workers"},
                                        {"value": 8, "label": "8 Workers"},
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
                                    position="left",
                                    grow=True,
                                    children=[
                                        dmc.Tab(
                                            "Strategy Picker",
                                            icon=DashIconify(icon="tabler:message"),
                                            value="Strategy Picker",
                                        ),
                                        dmc.Tab(
                                            "Jobs Console",
                                            icon=DashIconify(icon="tabler:settings"),
                                            value="Jobs Console",
                                        ),
                                    ],
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
                                                        "Delete Selected Jobs",
                                                        id="delete-button",
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
                                            dmc.Space(h=20),
                                            html.Div(
                                                id="pause-message",
                                                style={"display": "none"},
                                            ),
                                            html.Div(
                                                id="job_selection_output1",
                                                style={"display": "none"},
                                            ),
                                            html.Div(
                                                id="schedule-change-message",
                                                style={"display": "none"},
                                            ),
                                            html.Div(
                                                id="delete-message",
                                                style={"display": "none"},
                                            ),
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
                        dcc.Store(id="profile-store", storage_type="local"),
                        dcc.Interval(id="interval2", interval=86400000, n_intervals=0),
                    ]
                ),
            ]
        )
    )


@callback(
    [
        Output("cluster-loading-notification-step2", "children"),
        Output("cluster-loaded-notification-step2", "children"),
        Output("engine-test-result-step2", "children"),
    ],
    [
        Input("refresh-button-step2", "n_clicks"),
    ],
    [
        State("profile-store", "data"),
        State("hostname-store2", "data"),
        State("path-store2", "data"),
        State("token-store2", "data"),
    ],
)
def get_cluster_state(n_clicks, profile_name, host, path, token):
    if n_clicks or profile_name:
        config = ConfigParser()
        file_path = os.path.expanduser("./.databrickscfg")
        # print(profile_name)
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
                host = config.get(profile_name, "host")
                path = config.get(profile_name, "path")
                token = config.get(profile_name, "token")
                host = host.replace("https://", "")
            if host and token and path:
                sqlwarehouse = path.replace("/sql/1.0/warehouses/", "")
                # print(sqlwarehouse)
                # print(host)
                # print(token)
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
                                    variant="gradient",
                                    gradient={"from": "yellow", "to": "orange"},
                                    color="yellow",
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


@callback(
    [
        Output("hostname-store2", "data"),
        Output("token-store2", "data"),
        Output("path-store2", "data"),
    ],
    [Input("profile-store", "data")],
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

                print(host)

                return host, token, path

    return (
        None,
        None,
        None,
    )


@callback(
    # Output("slider-output", "children"),
    Output("slider_store", "data"),
    Input("slider-callback", "value"),
    State("slider_store", "data"),
)
def update_value(value, slider_data):
    slider_data = int(slider_data) if slider_data else None
    return slider_data


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
    Output("load-optimizer-grid-step2", "children"),
    [
        # Input("profile-store", "value"),
        Input("refresh-button-step2", "n_clicks"),
    ],
    [
        State("profile-store", "data"),
        # State("hostname-store2", "data"),
        # State("token-store2", "data"),
        # State("path-store2", "data"),
    ],
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
                host = config.get(profile_name, "host")
                path = config.get(profile_name, "path")
                token = config.get(profile_name, "token")
                host = host.replace("https://", "")
                print(host)
                print(token)
                print(path)
                if host and token and path:
                    engine_url = f"databricks://token:{token}@{host}/?http_path={path}&catalog=main&schema=information_schema"
                    bigger_engine = create_engine(engine_url)
                    tables_stmt = f"SELECT * FROM system.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'optimizer_results';"
                    tables_in_db = pd.read_sql_query(tables_stmt, bigger_engine)

            columnDefs = [
                {
                    "headerName": "Strategy Name",
                    "field": "table_schema",
                    "checkboxSelection": True,
                    "headerCheckboxSelection": True,
                    "filter": True,
                    "floatingFilter": True,
                    "filterParams": {"buttons": ["apply", "reset"]},
                    "minWidth": 220,
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
                    dashGridOptions={"rowSelection": "multiple"},
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
                # dmc.SimpleGrid(
                #     cols=3,
                #     children=[
                #         html.Div(
                #             [
                #                 dmc.Text("Catalogs", align="center", weight=550),
                #                 dmc.Text(
                #                     id="catalog_selection_output1", align="center"
                #                 ),
                #             ]
                #         ),
                #         html.Div(
                #             [
                #                 dmc.Text("DBs", align="center", weight=550),
                #                 dmc.Text(id="schema_selection_output1", align="center"),
                #             ]
                #         ),
                #         html.Div(
                #             [
                #                 dmc.Text("Tables", align="center", weight=550),
                #                 dmc.Text(id="table_selection_output1", align="center"),
                #             ]
                #         ),
                #     ],
                # ),
            ]

            return optimizer_grid

    return []


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
    Output("delete-notification", "children"),
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
            return html.Div(
                f"{delete_count} jobs deleted successfully."
            ), comp.notification_delete(f"{delete_count} jobs deleted successfully.")

        else:
            return html.Div("Error deleting jobs."), comp.notification_job1_error(
                "Error deleting jobs."
            )

    return html.Div()


@callback(
    Output("pause-message", "children"),
    Output("pause-notification", "children"),
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
                # print(payload)
                # print(response_new.status_code)

                if response_new.status_code == 200:
                    toggle_count += 1

        if toggle_count > 0:
            return html.Div(
                f"{toggle_count} jobs have been toggled successfully."
            ), comp.notification_update_pause(
                f"{toggle_count} jobs have been toggled successfully."
            )
        else:
            return html.Div(
                "Error toggling pause status for jobs."
            ), comp.notification_job1_error("Error toggling pause status for jobs.")

    return html.Div()


import subprocess
import json


@callback(
    Output("schedule-change-message", "children"),
    Output("schedule-notification", "children"),
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
            return html.Div(
                "Schedule updated successfully."
            ), comp.notification_update_schedule("Schedule updated successfully")
        else:
            return html.Div(
                f"Error updating schedule: {response.text}"
            ), comp.notification_job1_error(f"Error updating schedule: {response.text}")

    return html.Div()


@callback(
    Output("jobs-grid", "children"),
    [
        # Input("profile-dropdown-step2", "value"),
        Input("refresh-button-step2", "n_clicks"),
    ],
    [
        State("hostname-store2", "data"),
        State("token-store2", "data"),
    ],
    prevent_initial_call=True,
)
def get_job_list(n_clicks, host, access_token):
    if n_clicks is not None and n_clicks > 0:
        if host is None or access_token is None:
            raise PreventUpdate

        # Make a GET request to the Jobs API
        response = requests.get(
            f"https://{host}/api/2.1/jobs/list",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the job information from the response
            job_list = response.json().get("jobs", [])

            # Create an empty list to store the filtered jobs
            filtered_jobs = []

            # Iterate over the job list and filter jobs with "Optimizer" in the name or settings.name
            for job in job_list:
                name = job.get("name")
                settings_name = job.get("settings", {}).get("name")
                if name and ("Optimizer" in name or "Strategy" in name):
                    filtered_jobs.append(job)
                elif settings_name and (
                    "Optimizer" in settings_name or "Strategy" in settings_name
                ):
                    filtered_jobs.append(job)

            # Convert the filtered jobs list to a Pandas DataFrame
            df = pd.DataFrame(filtered_jobs)

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
                    dashGridOptions={"rowSelection": "multiple"},
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
    State("profile-store", "data"),
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
        ):
            options.append({"label": section, "value": section})

    if profile_name:
        (
            host,
            token,
            path,
        ) = parse_databricks_config(profile_name)
        if host and token and path:
            engine_url = (
                f"databricks://token:{token}@{host}/?"
                f"http_path={path}&catalog=main&schema=information_schema"
            )
            engine = create_engine(engine_url)
            stmt = f"select * from {selected_table[0].strip('[]')}"
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

    return [], []  # Default return if no conditions above are met


@callback(
    # Output("catalog_selection_output1", "children"),
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
    # Output("schema_selection_output1", "children"),
    Output("schema_selection_store1", "data"),
    Input("optimizer-grid-step2", "selectedRows"),
)
def schema(selected):
    if selected:
        # Create a list of strings in the format "table_catalog.table_schema"
        formatted_selections = [
            f"{s['table_catalog']}.{s['table_schema']}" for s in selected
        ]

        # Join the list into a single string separated by commas
        final_string = ", ".join(formatted_selections)

        # Return the formatted string
        return final_string
    return "No selections", dash.no_update


@callback(
    # Output("table_selection_output1", "children"),
    Output("table_selection_store1", "data"),
    Input("optimizer-grid-step2", "selectedRows"),
)
def tables(selected):
    if selected:
        selected_catalog = [s["table_catalog"] for s in selected]
        selected_tables = [s["table_name"] for s in selected]
        selected_schema = [s["table_schema"] for s in selected]
        final = [
            f"{catalog}.{schema}.{table}"
            for catalog, schema, table in zip(
                selected_catalog, selected_schema, selected_tables
            )
        ]
        final_string = ", ".join(final)
        return final_string, final
    return "No selections", dash.no_update


@callback(
    Output("run-strategy-output", "children"),
    Output("run-strategy-notification", "children"),
    [
        Input("run-strategy-now-button", "n_clicks"),
    ],
    State("slider-callback", "value"),
    State("schema_selection_store1", "data"),
    State("profile-store", "data"),
    prevent_initial_call=True,
)
def delta_step_2_optimizer(n_clicks, slider, selected_schema, profile_name):
    if not n_clicks:
        raise PreventUpdate
    if n_clicks and profile_name:
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

                w = WorkspaceClient()

    if any(
        field is None or field == "" for field in [host, token, slider, selected_schema]
    ):
        print(host, token, slider, selected_schema)
        return (
            dash.no_update,
            comp.notification_job1_error("Please fill out all fields."),
        )

    base_parameters = {
        "Optimizer Output Database:": str(selected_schema),
        "exclude_list(csv)": "",
        "include_list(csv)": "",
        "table_mode": "include_all_tables",
    }
    print(selected_schema)

    created_job_step_2 = w.jobs.create(
        name=f"{selected_schema} Optimizer Run",
        git_source=GitSource(
            git_url="https://github.com/CodyAustinDavis/edw-best-practices.git",
            git_branch="main",
            git_provider="GITHUB",
        ),
        max_concurrent_runs=1,
        schedule=Schedule("0 0 10 * * ?", "US/Pacific", "UNPAUSED"),
        tasks=[
            jobs.Task(
                task_key="Delta_Optimizer_Step_2",
                notebook_task=NotebookTask(
                    "Delta Optimizer/Step 2_ Strategy Runner",
                    base_parameters,
                ),
                new_cluster=NewCluster(
                    spark_version="12.1.x-scala2.12",
                    node_type_id="i3.xlarge",
                    num_workers=slider,
                    spark_conf={"spark.databricks.delta.preview.enabled": "true"},
                    spark_env_vars={
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    enable_elastic_disk=True,
                ),
                libraries=[
                    Library("dbfs:/tmp/deltaoptimizer-1.5.0-py3-none-any.whl"),
                ],
                timeout_seconds=0,
            ),
        ],
        timeout_seconds=0,
        webhook_notifications={},
    )
    job_id_step_2 = created_job_step_2.job_id
    print(job_id_step_2)
    run_resp2 = w.jobs.run_now(job_id=job_id_step_2)
    print(run_resp2)
    run_id_step_2 = run_resp2.response.run_id
    print(run_id_step_2)

    #
    if run_id_step_2 is not None:
        return (
            [
                f"Optimizer ran with Job ID: {job_id_step_2}",
            ],
            comp.notification_user_step_1(
                f"Optimizer ran with Job ID: {job_id_step_2}"
            ),
        )
    else:
        return dash.no_update, dash.no_update


@callback(
    Output("run-strategy-output-schedule", "children"),
    Output("run-strategy-notification-schedule", "children"),
    [
        Input("run_strategy_button", "n_clicks"),
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
    if not n_clicks:
        raise PreventUpdate

    if any(
        field is None or field == ""
        for field in [hostname, token, slider, selected_schema]
    ):
        return (
            dash.no_update,
            comp.notification_job1_error("Please fill out all fields."),
        )
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
                    "spark_version": "12.1.x-scala2.12",
                    "num_workers": slider,
                    "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                },
                "aws_attributes": {
                    "availability": "ON_DEMAND",
                },
                "libraries": [
                    {"whl": "dbfs:/tmp/deltaoptimizer-1.5.0-py3-none-any.whl"},
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
    job_id = endp_resp2["job_id"]
    if "run_id" in run_resp2:
        return (
            [
                f"Optimizer ran with Job ID: {job_id}",
            ],
            comp.notification_user_step_1(f"Optimizer ran with Job ID: {job_id}"),
        )
    else:
        return dash.no_update, dash.no_update