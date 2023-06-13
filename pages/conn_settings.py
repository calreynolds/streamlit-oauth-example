import dash
import json
import requests
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
from flask_sqlalchemy import SQLAlchemy
import os
from configparser import ConfigParser

dash.register_page(__name__, path="/connection_settings", title="Connection Settings")

SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
WAREHOUSE_ID = "f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "information_schema"
SOUND = "dbxdashstudio"


from dash import html, dcc, Input, Output, callback
import dash
import pandas as pd
from sqlalchemy import Table, create_engine
import dash_ag_grid as dag
import subprocess

import urllib.request
import subprocess
import json

# # URL of the file to download
# url = "https://github.com/CodyAustinDavis/edw-best-practices/raw/main/Delta%20Optimizer/deltaoptimizer-1.4.1-py3-none-any.whl"

# # Local path to save the downloaded file
# local_path = "/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

# # Download the file using urllib.request
# urllib.request.urlretrieve(url, local_path)

# # Databricks file system path
# dbfs_path = "dbfs:/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

# # Databricks CLI command to check if the file already exists in DBFS
# check_command = ["databricks", "fs", "ls", dbfs_path]

# # Run the Databricks CLI command to check if the file exists
# check_result = subprocess.run(check_command, capture_output=True, text=True)

# # If the file doesn't exist, upload it to DBFS
# if "No such file or directory" in check_result.stderr:
#     # Databricks CLI command to upload the file to DBFS
#     upload_command = ["databricks", "fs", "cp", local_path, dbfs_path]

#     # Run the Databricks CLI command to upload the file
#     upload_result = subprocess.run(upload_command, capture_output=True, text=True)

#     # Check the return code to see if the command executed successfully
#     if upload_result.returncode == 0:
#         print("File upload successful.")
#     else:
#         print("File upload failed.")

#     # Print the output and error messages
#     print("Command output:", upload_result.stdout)
#     print("Command error:", upload_result.stderr)
# else:
#     print("File already exists in DBFS.")


# Databricks CLI command to retrieve library versions for all cluster statuses
# library_command = ["databricks", "libraries", "all-cluster-statuses"]

# # Run the Databricks CLI command to retrieve library versions
# library_result = subprocess.run(library_command, capture_output=True, text=True)

# # Parse the JSON output of the library command
# library_data = json.loads(library_result.stdout)

# # Print the library versions for all cluster statuses
# for status in library_data["statuses"]:
#     print(f"Cluster ID: {status['cluster_id']}")
#     # print("Library Versions:")
#     # for library in status["library_statuses"]:
#     #     print(f"- {library['library']['whl']}")
#     print()


# cluster_id = "0510-131932-sflv6c6d"

# # Path to the library file in DBFS
# dbfs_file_path = "dbfs:/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

# # Databricks CLI command to install the library on the cluster from DBFS
# install_command = [
#     "databricks",
#     "libraries",
#     "install",
#     "--cluster-id",
#     cluster_id,
#     "--whl",
#     dbfs_file_path,
# ]

# # Run the Databricks CLI command to install the library
# result = subprocess.run(install_command, capture_output=True, text=True)

# # Check the return code to see if the command executed successfully
# if result.returncode == 0:
#     print("Library installation successful.")
# else:
#     print("Library installation failed.")

# # Print the output and error messages
# print("Command output:", result.stdout)
# print("Command error:", result.stderr)

# SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
# HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
# ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
# CATALOG = "main"
# SCHEMA = "dbxdashstudio"
# INFORMATION_SCHEMA = "information_schema"

# conn_str = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
# extra_connect_args = {
#     "_tls_verify_hostname": True,
#     "_user_agent_entry": "PySQL Example Script",
# }
# main_engine = create_engine(
#     conn_str,
#     connect_args=extra_connect_args,
# )

# conn_str = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={INFORMATION_SCHEMA}"
# extra_connect_args = {
#     "_tls_verify_hostname": True,
#     "_user_agent_entry": "PySQL Example Script",
# }
# schema_engine = create_engine(
#     conn_str,
#     connect_args=extra_connect_args,
# )

# userstmt = f"Select * FROM main.dbxdashstudio.engines;"
# dataframe = pd.read_sql_query(userstmt, main_engine)

# db = SQLAlchemy()


def layout():
    return (
        # html.Div(
        #     children=[
        #         html.H1("Cluster IDs"),
        #         dmc.Button("Update Cluster IDs", id="update-button"),
        #         dcc.Dropdown(id="cluster-dropdown"),
        #         html.H1("Databricks Jobs"),
        #         dcc.Dropdown(id="job-dropdown"),
        #         html.Div(id="job-details"),
        #         dmc.Button("List Jobs", id="list-jobs-button"),
        #     ]
        # ),
        html.Div(
            [
                html.H1("Create CLI Connection Profile"),
                html.Div(
                    [
                        html.Label("Profile Name"),
                        dmc.TextInput(
                            id="profile-name",
                            type="text",
                            placeholder="Enter profile name",
                        ),
                    ]
                ),
                html.Div(
                    [
                        html.Label("Workspace URL"),
                        dmc.TextInput(
                            id="workspace-url",
                            type="text",
                            placeholder="Enter workspace URL",
                        ),
                    ]
                ),
                html.Label("Token"),
                dmc.TextInput(id="token", type="text", placeholder="Enter token"),
                dmc.Space(h=10),
                dmc.Button("Create Profile", id="generate-button", n_clicks=0),
                html.H1("Profile Selector"),
                dcc.Dropdown(
                    id="profile-dropdown",
                    options=[
                        {"label": profile_name, "value": profile_name}
                        for profile_name in get_profile_names()
                    ],
                    placeholder="Select a profile",
                ),
                dmc.Space(h=10),
                dmc.Button("Activate Profile", id="activate-button"),
                dmc.Space(h=10),
                html.Div(id="selected-profile"),
                dmc.Button("Check DBFS for Library", id="check-library-button"),
                dmc.Space(h=10),
                html.Div(id="library-status"),
                html.Div(
                    children=[
                        html.H1("Install Library on Cluster"),
                        html.Div(id="result-div"),
                        html.Label("Select Cluster:"),
                        dcc.Dropdown(id="cluster-dropdown", options=[], value=None),
                        dmc.Space(h=10),
                        dmc.Button("Install Library", id="install-library-button"),
                        html.H1("Repository Installation"),
                        dmc.Button("Get Users", id="refresh-button"),
                        dmc.Space(h=10),
                        dcc.Dropdown(id="group-members-dropdown"),
                        # dmc.TextInput(
                        #     id="url-input",
                        #     placeholder="Enter repository URL",
                        #     type="text",
                        # ),
                        # dmc.TextInput(
                        #     id="provider-input",
                        #     placeholder="Enter repository provider",
                        #     type="text",
                        # ),
                        # dmc.TextInput(
                        #     id="path-input",
                        #     placeholder="Enter repository path",
                        #     type="text",
                        # ),
                        dmc.Space(h=10),
                        dmc.Button(
                            "Install Repository", id="create-repo-button", n_clicks=0
                        ),
                        html.Div(id="repo-creation-status"),
                        dmc.Space(h=10),
                        html.H1("Repository List"),
                        dmc.Button(
                            "List Repositories", id="list-repos-button", n_clicks=0
                        ),
                        html.Div(id="list-repos-output"),
                    ]
                ),
                dcc.Store(id="selected-cluster-store", storage_type="memory"),
            ]
        ),
    )


def get_profile_names():
    file_path = os.path.expanduser("~/.databrickscfg")
    config = ConfigParser()

    if os.path.exists(file_path):
        config.read(file_path)

    return config.sections()


@callback(
    Output("group-members-dropdown", "options"), Input("refresh-button", "n_clicks")
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
    Output("list-repos-output", "children"), Input("list-repos-button", "n_clicks")
)
def list_repositories(n_clicks):
    if n_clicks is None or n_clicks == 0:
        return ""

    # Databricks CLI command to list repositories
    list_command = ["databricks", "repos", "list"]

    try:
        # Run the Databricks CLI command to list repositories
        result = subprocess.run(list_command, capture_output=True, text=True)

        if result.returncode == 0:
            repos_data = result.stdout.strip()
            return html.Pre(repos_data)
        else:
            return "Error: Failed to list repositories."

    except Exception as e:
        return f"Error: {str(e)}"


@callback(
    Output("repo-creation-status", "children"),
    Input("create-repo-button", "n_clicks"),
    State("group-members-dropdown", "value"),
    prevent_initial_call=True,
)
def create_repo(n_clicks, user_name):
    if n_clicks and user_name:
        # Specify the GitHub URL and repo path
        github_url = "https://github.com/CodyAustinDavis/edw-best-practices.git"
        repo_path = f"/Repos/{user_name}/edw-best-practices-test"
        # Databricks CLI command to create a new repo
        create_repo_command = [
            "databricks",
            "repos",
            "create",
            "--url",
            github_url,
            "--provider",
            "gitHub",
            "--path",
            repo_path,
        ]

        # Run the Databricks CLI command to create the repo
        create_repo_result = subprocess.run(
            create_repo_command, capture_output=True, text=True
        )

        if create_repo_result.returncode == 0:
            return html.Div(
                [
                    html.H3("Repo created successfully."),
                    html.H4("Command Output:"),
                    dcc.Markdown(create_repo_result.stdout),
                    html.H4("Command Error:"),
                    dcc.Markdown(create_repo_result.stderr),
                ]
            )
        else:
            return html.Div(
                [
                    html.H3("Error creating the repo."),
                    html.H4("Command Output:"),
                    dcc.Markdown(create_repo_result.stdout),
                    html.H4("Command Error:"),
                    dcc.Markdown(create_repo_result.stderr),
                ]
            )
    return ""


@callback(
    Output("cluster-dropdown", "options"),
    Output("cluster-dropdown", "value"),
    Output("selected-cluster-store", "data"),
    Input("check-library-button", "n_clicks"),
    State("cluster-dropdown", "value"),
)
def populate_cluster_dropdown(n_clicks, selected_cluster):
    if n_clicks is None:
        return [], None, None

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

    return cluster_options, selected_cluster, selected_cluster


@callback(
    Output("result-div", "children"),
    Input("install-library-button", "n_clicks"),
    Input("cluster-dropdown", "value"),
    State("profile-dropdown", "value"),
)
def install_library(n_clicks, cluster_id, profile_name):
    if n_clicks is None or cluster_id is None or not profile_name:
        return ""

    # Path to the library file in DBFS
    dbfs_file_path = "dbfs:/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

    # Databricks CLI command to install the library on the cluster from DBFS
    install_command = [
        "databricks",
        "libraries",
        "install",
        "--cluster-id",
        cluster_id,
        "--whl",
        dbfs_file_path,
        "--profile",
        profile_name,
    ]

    # Run the Databricks CLI command to install the library
    result = subprocess.run(install_command, capture_output=True, text=True)

    # Check the return code to see if the command executed successfully
    if result.returncode == 0:
        output_message = result.stdout.strip()
        return html.P(
            f"The library was successfully installed on cluster {cluster_id}. Make sure you select this cluster when you run the Delta Optimizer."
        )
    else:
        error_message = result.stderr.strip()
        return html.P(f"Error: {error_message}")


@callback(
    Output("library-status", "children"),
    [Input("check-library-button", "n_clicks")],
    [State("profile-dropdown", "value")],
)
def check_library(n_clicks, profile_name):
    if n_clicks:
        if profile_name:
            url = "https://github.com/CodyAustinDavis/edw-best-practices/raw/main/Delta%20Optimizer/deltaoptimizer-1.4.1-py3-none-any.whl"
            local_path = "/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"
            dbfs_path = "dbfs:/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

            urllib.request.urlretrieve(url, local_path)

            check_command = ["databricks", "fs", "ls", dbfs_path]
            check_result = subprocess.run(check_command, capture_output=True, text=True)

            if "No such file or directory" in check_result.stderr:
                upload_command = ["databricks", "fs", "cp", local_path, dbfs_path]
                upload_result = subprocess.run(
                    upload_command, capture_output=True, text=True
                )

                if upload_result.returncode == 0:
                    return html.Div(
                        [
                            html.H3(f"The library {dbfs_path} was uploaded to DBFS."),
                            html.H4("Command Output:"),
                            dcc.Markdown(upload_result.stdout),
                            html.H4("Command Error:"),
                            dcc.Markdown(upload_result.stderr),
                        ]
                    )
                else:
                    return html.Div(
                        [
                            html.H3("Error uploading the library to DBFS."),
                            html.H4("Command Output:"),
                            dcc.Markdown(upload_result.stdout),
                            html.H4("Command Error:"),
                            dcc.Markdown(upload_result.stderr),
                        ]
                    )
            else:
                return html.Div(
                    [
                        html.H3(f"The library {dbfs_path} already exists in DBFS."),
                        html.H4("Command Output:"),
                        dcc.Markdown(check_result.stdout),
                        html.H4("Command Error:"),
                        dcc.Markdown(check_result.stderr),
                    ]
                )
        else:
            return html.H3("Please select a profile.")
    return ""


@callback(
    Output("selected-profile", "children"),
    [Input("activate-button", "n_clicks")],
    [State("profile-dropdown", "value")],
)
def activate_profile(n_clicks, profile_name):
    if n_clicks:
        if profile_name:
            command = f"databricks fs ls dbfs:/ --profile {profile_name}"
            try:
                result = subprocess.run(
                    command, capture_output=True, text=True, shell=True
                )
                output = result.stdout.strip()

                return html.Div(
                    [
                        html.H3(f"Activated Profile: {profile_name}"),
                        html.H4("Command:"),
                        dcc.Markdown(f"`{command}`"),
                        html.H4("Command Output:"),
                        dcc.Markdown(output),
                    ]
                )
            except subprocess.CalledProcessError as e:
                return html.Div([html.H3("Error executing command"), html.Pre(str(e))])
        else:
            return html.H3("Please select a profile.")
    return ""


@callback(
    Output("generate-button", "n_clicks"),
    [Input("generate-button", "n_clicks")],
    [
        State("profile-name", "value"),
        State("workspace-url", "value"),
        State("token", "value"),
    ],
)
def generate_file(n_clicks, profile_name, workspace_url, token):
    if n_clicks > 0:
        config = ConfigParser()
        file_path = os.path.expanduser("~/.databrickscfg")

        if os.path.exists(file_path):
            config.read(file_path)

        if not config.has_section(profile_name):
            config.add_section(profile_name)

        config.set(profile_name, "host", workspace_url)
        config.set(profile_name, "token", token)

        with open(file_path, "w") as file:
            config.write(file)

        return 0

    return n_clicks


# @callback(Output("job-dropdown", "options"), Input("list-jobs-button", "n_clicks"))
# def list_jobs(n_clicks):
#     if n_clicks is None:
#         return []

#     # Databricks API endpoint to list jobs
#     api_endpoint = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/list"

#     # Databricks personal access token
#     token = ACCESS_TOKEN

#     # Request headers with authorization token
#     headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

#     # Send GET request to list jobs
#     response = requests.get(api_endpoint, headers=headers)

#     if response.status_code == 200:
#         jobs_data = response.json()["jobs"]
#         jobs_list = [
#             {"label": job["job_id"], "value": job["job_id"]} for job in jobs_data
#         ]
#         return jobs_list
#     else:
#         return []


# @callback(Output("job-details", "children"), Input("job-dropdown", "value"))
# def show_job_details(job_id):
#     if job_id is None:
#         return ""

#     # Databricks API endpoint to get job details
#     api_endpoint = f"https://{SERVER_HOSTNAME}/api/2.1/jobs/get?job_id={job_id}"

#     # Databricks personal access token
#     token = ACCESS_TOKEN

#     # Request headers with authorization token
#     headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

#     # Send GET request to get job details
#     response = requests.get(api_endpoint, headers=headers)

#     if response.status_code == 200:
#         job_data = response.json()
#         creator_user_name = job_data["creator_user_name"]
#         created_time = job_data["created_time"]
#         return html.Div(
#             [
#                 html.H3(f"Job ID: {job_id}"),
#                 html.P(f"Creator: {creator_user_name}"),
#                 html.P(f"Created Time: {created_time}"),
#             ]
#         )
#     else:
#         return ""


# @callback(
#     Output("cluster-dropdown", "options"),
#     Input("update-button", "n_clicks"),
#     State("cluster-dropdown", "value"),
# )
# def update_cluster_ids(n_clicks, selected_cluster):
#     if n_clicks is None:
#         # Initial load, do not update
#         return dash.no_update

#     # Databricks CLI command to retrieve cluster IDs
#     cluster_ids_command = ["databricks", "clusters", "list", "--output", "json"]

#     # Run the Databricks CLI command to retrieve cluster IDs
#     result = subprocess.run(cluster_ids_command, capture_output=True, text=True)

#     # Parse the JSON output of the command
#     cluster_data = json.loads(result.stdout)

#     # Extract the cluster IDs
#     cluster_ids = [cluster["cluster_id"] for cluster in cluster_data["clusters"]]

#     # Create the options for the dropdown component
#     dropdown_options = [
#         {"label": cluster_id, "value": cluster_id} for cluster_id in cluster_ids
#     ]

#     return dropdown_options
