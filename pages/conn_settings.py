import os
import json
import requests
from configparser import ConfigParser
import urllib.request
import subprocess

import dash
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
from sqlalchemy.engine import create_engine
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table
import dash_ag_grid as dag


dash.register_page(__name__, path="/connection_settings", title="Connection Settings")


def layout():
    return html.Div(
        [
            dmc.Title("Settings"),
            dmc.Divider(variant="solid"),
            html.H3("Select Profile"),
            dcc.Dropdown(
                id="profile-dropdown",
                options=[],
                placeholder="Select a profile",
            ),
            dmc.Group(
                position="left",
                mt="xl",
                children=[
                    dmc.Button(
                        "Get Profile Names",
                        id="get-profile-names-button",
                    ),
                    dmc.Button(
                        "Activate Profile",
                        rightIcon=DashIconify(
                            icon="codicon:run-above",
                        ),
                        id="activate-button",
                    ),
                    dmc.Button(
                        "Check DBFS for Library",
                        id="check-library-button",
                    ),
                    # dmc.Button(
                    #     id="test-engine-button",
                    #     children="Test Engine",
                    # ),
                ],
            ),
            html.Div(id="selected-profile"),
            html.Div(id="library-status"),
            html.Div(id="engine-test-result"),
            dmc.Space(h=10),
            dmc.Accordion(
                disableChevronRotation=True,
                children=[
                    dmc.AccordionItem(
                        [
                            dmc.AccordionControl(
                                "Create New Profile",
                                icon=DashIconify(
                                    icon="tabler:user",
                                    color=dmc.theme.DEFAULT_COLORS["blue"][6],
                                    width=20,
                                ),
                            ),
                            dmc.AccordionPanel(
                                [
                                    html.Label("Profile Name"),
                                    dmc.TextInput(
                                        id="profile-name",
                                        type="text",
                                        placeholder="Enter profile name",
                                    ),
                                ]
                            ),
                            dmc.AccordionPanel(
                                [
                                    html.Label("Workspace URL"),
                                    dmc.TextInput(
                                        id="workspace-url",
                                        type="text",
                                        placeholder="Enter workspace URL",
                                    ),
                                ]
                            ),
                            dmc.AccordionPanel(
                                [
                                    html.Label("HTTP Path"),
                                    dmc.TextInput(
                                        id="path",
                                        type="text",
                                        placeholder="Enter Http Path",
                                    ),
                                ]
                            ),
                            dmc.AccordionPanel(
                                [
                                    html.Label("Token"),
                                    dmc.TextInput(
                                        id="token",
                                        type="text",
                                        placeholder="Enter token",
                                    ),
                                    dmc.Space(h=10),
                                    dmc.Button(
                                        "Create Profile",
                                        id="generate-button",
                                        n_clicks=0,
                                    ),
                                    html.Div(id="profile-creation-message"),
                                ]
                            ),
                        ],
                        value="profile",
                    ),
                ],
            ),
            # html.H3("Select Engine"),
            # dcc.Dropdown(
            #     id="engine-dropdown",
            #     options=[],
            #     placeholder="Select an engine",
            # ),
            # dmc.Space(h=10),
            # dmc.Group(
            #     position="left",
            #     mt="xl",
            #     children=[
            #         dmc.Button(
            #             id="get-engine-names-button",
            #             children="Get Engine Names",
            #         ),
            #         dmc.Button(
            #             id="load-engine-button",
            #             children="Load Engine",
            #         ),
            #     ],
            # ),
            # html.Div(id="engine-info"),
            # dmc.Space(h=10),
            # dmc.Space(h=10),
            # dmc.Accordion(
            #     disableChevronRotation=True,
            #     children=[
            #         dmc.AccordionItem(
            #             [
            #                 dmc.AccordionControl(
            #                     "Create New SQLAlchemy Engine",
            #                     icon=DashIconify(
            #                         icon="mingcute:engine-line",
            #                         color=dmc.theme.DEFAULT_COLORS["blue"][6],
            #                         width=20,
            #                     ),
            #                 ),
            #                 dmc.AccordionPanel(
            #                     [
            #                         html.Label("Engine Name"),
            #                         dmc.TextInput(
            #                             id="engine-name",
            #                             type="text",
            #                             placeholder="Enter engine name",
            #                         ),
            #                     ]
            #                 ),
            #                 dmc.AccordionPanel(
            #                     [
            #                         html.Label("Workspace URL"),
            #                         dmc.TextInput(
            #                             id="workspace-url-engine",
            #                             type="text",
            #                             placeholder="Enter workspace URL (without https://)",
            #                         ),
            #                     ]
            #                 ),
            #                 dmc.AccordionPanel(
            #                     [
            #                         html.Label("HTTP Path"),
            #                         dmc.TextInput(
            #                             id="path",
            #                             type="text",
            #                             placeholder="Enter Http Path",
            #                         ),
            #                     ]
            #                 ),
            #                 dmc.AccordionPanel(
            #                     [
            #                         html.Label("Token"),
            #                         dmc.TextInput(
            #                             id="token-engine",
            #                             type="text",
            #                             placeholder="Enter token",
            #                         ),
            #                         dmc.Space(h=10),
            #                         dmc.Button(
            #                             "Create Engine",
            #                             id="generate-engine-button",
            #                             n_clicks=0,
            #                         ),
            #                         html.Div(id="engine-message-output"),
            #                     ]
            #                 ),
            #             ],
            #             value="engine",
            #         ),
            #     ],
            # ),
            html.Div(
                children=[
                    html.H3("Install Library"),
                    html.Div(id="result-div"),
                    # html.Label("Select Cluster:"),
                    dcc.Dropdown(id="cluster-dropdown", options=[], value=None),
                    dmc.Space(h=10),
                    dmc.Group(
                        position="left",
                        mt="xl",
                        children=[
                            dmc.Button(
                                "Get Clusters",
                                id="get-clusters-button",
                                n_clicks=0,
                            ),
                            dmc.Button(
                                "Install Library",
                                id="install-library-button",
                                n_clicks=0,
                            ),
                            dmc.Button(
                                "Append Cluster to Profile",
                                id="append-cluster-button",
                                n_clicks=0,
                            ),
                        ],
                    ),
                    html.Div(id="append-cluster-output"),
                    html.Div(id="selected-cluster-output"),
                    html.H3("Install Repository"),
                    dcc.Dropdown(id="group-members-dropdown"),
                    dmc.Group(
                        position="left",
                        mt="xl",
                        children=[
                            dmc.Button("Get Users", id="refresh-button"),
                            dmc.Button(
                                "Install Repository",
                                id="create-repo-button",
                                leftIcon=DashIconify(icon="ion:logo-github"),
                                n_clicks=0,
                            ),
                        ],
                    ),
                    html.Div(id="repo-creation-status"),
                    dmc.Space(h=10),
                ]
            ),
            dcc.Store(id="selected-cluster-store", storage_type="session"),
            dcc.Store(id="selected-profile-store", storage_type="memory"),
            dcc.Store(id="engine-store", storage_type="memory"),
            # dcc.Store(id="hostname-store", storage_type="memory"),
            # dcc.Store(id="path-store", storage_type="memory"),
            # dcc.Store(id="token-store", storage_type="memory"),
            dcc.Store(id="cluster-options-store", storage_type="memory"),
            # dcc.Store(id="cluster-name-store", storage_type="memory"),
            # dcc.Store(id="cluster-id-store", storage_type="memory"),
        ]
    )


@callback(
    Output("generate-button", "n_clicks"),
    Output("profile-creation-message", "children"),
    [Input("generate-button", "n_clicks")],
    [
        State("profile-name", "value"),
        State("workspace-url", "value"),
        State("token", "value"),
        State("path", "value"),
    ],
)
def generate_file(n_clicks, profile_name, workspace_url, token, path):
    if n_clicks is not None and n_clicks > 0:
        config = ConfigParser()
        file_path = os.path.expanduser("~/.databrickscfg")

        if os.path.exists(file_path):
            config.read(file_path)

        if not config.has_section(profile_name):
            config.add_section(profile_name)

        config.set(profile_name, "host", workspace_url)
        config.set(profile_name, "token", token)
        config.set(profile_name, "path", path)

        with open(file_path, "w") as file:
            config.write(file)

        message = f"Profile '{profile_name}' created successfully."
        return 0, message

    return n_clicks, ""


# Function to get the profile names from the databricks config file
def get_profile_names():
    file_path = os.path.expanduser("~/.databrickscfg")
    config = ConfigParser()

    if os.path.exists(file_path):
        config.read(file_path)

    return config.sections()


@callback(
    Output("profile-dropdown", "options"),
    [Input("get-profile-names-button", "n_clicks")],
)
def update_profile_dropdown(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        profile_names = get_profile_names()
        options = [{"label": name, "value": name} for name in profile_names]
        return options
    return []


# Callback to activate the selected profile
@callback(
    Output("selected-profile", "children"),
    Output("selected-profile-store", "data"),
    [Input("activate-button", "n_clicks")],
    [State("profile-dropdown", "value")],
)
def activate_profile(n_clicks, profile_name):
    if n_clicks and profile_name:
        command = f"databricks fs ls dbfs:/ --profile {profile_name}"
        try:
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            output = result.stdout.strip()

            return [
                html.Div(
                    [
                        html.H3(f"Activated Profile: {profile_name}"),
                        html.H4("Command:"),
                        dcc.Markdown(f"`{command}`"),
                        html.H4("Command Output:"),
                        dcc.Markdown(output),
                    ]
                ),
                profile_name,
            ]
        except subprocess.CalledProcessError as e:
            return [
                html.Div([html.H3("Error executing command"), html.Pre(str(e))]),
                None,
            ]
    return ["", None]


# Callback to check if the library is in DBFS
@callback(
    Output("library-status", "children"),
    [Input("check-library-button", "n_clicks")],
    [State("profile-dropdown", "value")],
    prevent_initial_call=True,
)
def check_library(n_clicks, profile_name):
    if n_clicks:
        if profile_name:
            url = "https://github.com/CodyAustinDavis/edw-best-practices/raw/main/Delta%20Optimizer/deltaoptimizer-1.4.1-py3-none-any.whl"
            local_path = "/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"
            dbfs_path = "dbfs:/tmp/deltaoptimizer-1.4.1-py3-none-any.whl"

            urllib.request.urlretrieve(url, local_path)

            check_command = [
                "databricks",
                "fs",
                "ls",
                dbfs_path,
                "--profile",
                profile_name,
            ]
            check_result = subprocess.run(check_command, capture_output=True, text=True)

            if "No such file or directory" in check_result.stderr:
                upload_command = [
                    "databricks",
                    "fs",
                    "cp",
                    local_path,
                    dbfs_path,
                    "--profile",
                    profile_name,
                ]
                upload_result = subprocess.run(
                    upload_command, capture_output=True, text=True
                )

                if upload_result.returncode == 0:
                    return html.Div(
                        [
                            html.H3(f"The library {dbfs_path} was uploaded to DBFS."),
                            html.H4("Command Output:"),
                            dcc.Markdown(upload_result.stdout),
                            # html.H4("Command Error:"),
                            # dcc.Markdown(upload_result.stderr),
                        ]
                    )
                else:
                    return html.Div(
                        [
                            html.H3("Error uploading the library to DBFS."),
                            html.H4("Command Output:"),
                            dcc.Markdown(upload_result.stdout),
                            # html.H4("Command Error:"),
                            # dcc.Markdown(upload_result.stderr),
                        ]
                    )
            else:
                return html.Div(
                    [
                        html.H3(f"The library {dbfs_path} already exists in DBFS."),
                        html.H4("Command Output:"),
                        dcc.Markdown(check_result.stdout),
                        # html.H4("Command Error:"),
                        # dcc.Markdown(check_result.stderr),
                    ]
                )
        else:
            return html.H3("Please select a profile.")
    return ""


# @callback(
#     Output("hostname-store", "data"),
#     Output("path-store", "data"),
#     Output("token-store", "data"),
#     Input("activate-button", "n_clicks"),
#     [State("profile-dropdown", "value")],
# )
# def load_engine_details(n_clicks, profile_name):
#     if profile_name:
#         config = ConfigParser()
#         file_path = os.path.expanduser("~/.databrickscfg")

#         if os.path.exists(file_path):
#             config.read(file_path)

#             if config.has_section(profile_name):
#                 host = config.get(profile_name, "host")
#                 path = config.get(profile_name, "path")
#                 token = config.get(profile_name, "token")
#                 host = host.replace("https://", "")

#                 return host, path, token

#     return None, None, None


# @callback(
#     Output("engine-test-result", "children"),
#     Input("test-engine-button", "n_clicks"),
#     [
#         State("hostname-store", "data"),
#         State("path-store", "data"),
#         State("token-store", "data"),
#     ],
# )
# def test_sqlalchemy_engine(n_clicks, hostname, path, token):
#     if n_clicks is not None and n_clicks > 0 and hostname and path and token:
#         # Construct the SQL Alchemy engine
#         engine_url = f"databricks://token:{token}@{hostname}/?http_path={path}&catalog='main'&schema='information_schema'"
#         engine = create_engine(engine_url)

#         try:
#             # Test the engine connection by executing a sample query
#             with engine.connect() as connection:
#                 result = connection.execute("SELECT 1")
#                 test_value = result.scalar()

#                 if test_value == 1:
#                     return html.Div("Connection successful!")
#         except Exception as e:
#             return html.Div(f"Connection failed: {str(e)}")

#     return ""


# Callback to generate the databricks config file


@callback(
    Output("cluster-dropdown", "options"),
    Output("selected-cluster-output", "children"),
    Output("cluster-options-store", "data"),
    Input("get-clusters-button", "n_clicks"),
    State("selected-profile-store", "data"),
    State("cluster-dropdown", "value"),
    prevent_initial_call=True,
)
def populate_cluster_dropdown(n_clicks, profile_name, selected_cluster_id):
    if not profile_name:
        return [], "Please go to settings and select a profile first.", []

    # Databricks CLI command to retrieve all cluster statuses
    cluster_command = [
        "databricks",
        "clusters",
        "list",
        "--profile",
        profile_name,
        "--output",
        "json",
    ]

    try:
        # Run the Databricks CLI command to retrieve cluster statuses
        cluster_result = subprocess.run(
            cluster_command,
            capture_output=True,
            text=True,
        )

        # Check if there was an error executing the command
        if cluster_result.returncode != 0:
            return [], "Error retrieving cluster options.", []

        # Parse the JSON output of the cluster command
        cluster_data = json.loads(cluster_result.stdout)

        # Extract the cluster IDs and labels
        cluster_options = [
            {"label": cluster["cluster_name"], "value": cluster["cluster_id"]}
            for cluster in cluster_data["clusters"]
        ]

        return (
            cluster_options,
            "Cluster options retrieved successfully.",
            cluster_options,
        )

    except subprocess.CalledProcessError as e:
        return [], f"Error retrieving cluster options: {str(e)}", []


@callback(
    Output("result-div", "children"),
    Input("install-library-button", "n_clicks"),
    Input("cluster-dropdown", "value"),
    State("profile-dropdown", "value"),
    prevent_initial_call=True,
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
    Output("append-cluster-output", "children"),
    Input("append-cluster-button", "n_clicks"),
    State("cluster-dropdown", "value"),
    State("profile-dropdown", "value"),
    State("cluster-options-store", "data"),
    prevent_initial_call=True,
)
def append_cluster_to_profile(
    n_clicks, selected_cluster, profile_name, cluster_options
):
    if not (n_clicks and selected_cluster and profile_name):
        return ""

    config = ConfigParser()
    file_path = os.path.expanduser("~/.databrickscfg")

    if os.path.exists(file_path):
        config.read(file_path)

        if config.has_section(profile_name):
            cluster_name = None
            cluster_id = None

            # Retrieve cluster name and ID from the selected cluster value
            for option in cluster_options:
                if option["value"] == selected_cluster:
                    cluster_name = option["label"]
                    cluster_id = selected_cluster
                    break

            if cluster_name and cluster_id:
                # Append cluster name and ID to the profile in the .databrickscfg file
                config.set(profile_name, "cluster_name", cluster_name)
                config.set(profile_name, "cluster_id", cluster_id)

                with open(file_path, "w") as f:
                    config.write(f)

                return f"Cluster '{cluster_name}' with ID '{cluster_id}' appended to profile '{profile_name}'."

    return ""


@callback(
    Output("group-members-dropdown", "options"),
    Input("refresh-button", "n_clicks"),
    State("selected-profile-store", "data"),
    prevent_initial_call=True,
)
def refresh_group_members(n_clicks, profile_name):
    if n_clicks:
        group_name = "admins"
        command = [
            "databricks",
            "groups",
            "list-members",
            "--group-name",
            group_name,
            "--profile",
            profile_name,
        ]
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            output_json = json.loads(result.stdout)
            members = [member["user_name"] for member in output_json["members"]]
            member_options = [{"label": member, "value": member} for member in members]
            return member_options

    return []


@callback(
    Output("repo-creation-status", "children"),
    Input("create-repo-button", "n_clicks"),
    State("group-members-dropdown", "value"),
    State("selected-profile-store", "data"),
    prevent_initial_call=True,
)
def create_repo(n_clicks, user_name, profile_name):
    if n_clicks and user_name and profile_name:
        # Specify the GitHub URL and repo path
        github_url = "https://github.com/CodyAustinDavis/edw-best-practices.git"
        repo_path = f"/Repos/{user_name}/edw-best-practices"
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
            "--profile",
            profile_name,
        ]

        # Run the Databricks CLI command to create the repo
        create_repo_result = subprocess.run(
            create_repo_command, capture_output=True, text=True
        )

        if create_repo_result.returncode == 0:
            # Add the user_name to the .databrickscfg file under the associated profile name
            config = ConfigParser()
            file_path = os.path.expanduser("~/.databrickscfg")

            if os.path.exists(file_path):
                config.read(file_path)

                if config.has_section(profile_name):
                    config.set(profile_name, "user_name", user_name)

                    with open(file_path, "w") as f:
                        config.write(f)

            return html.Div(
                [
                    html.H3("Repo created successfully."),
                    html.H4("Command Output:"),
                    dcc.Markdown(create_repo_result.stdout),
                    # html.H4("Command Error:"),
                    # dcc.Markdown(create_repo_result.stderr),
                ]
            )
        else:
            return html.Div(
                [
                    html.H3("Error creating the repo."),
                    html.H4("Command Output:"),
                    dcc.Markdown(create_repo_result.stdout),
                    # html.H4("Command Error:"),
                    # dcc.Markdown(create_repo_result.stderr),
                ]
            )
    return ""
