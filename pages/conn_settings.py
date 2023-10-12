import os
from configparser import ConfigParser
import urllib.request

import dash
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from sqlalchemy.engine import create_engine
from databricks.sdk.oauth import OAuthClient, Consent


import components as comp
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import SessionCredentials
from flask import redirect, session, url_for, request

from dotenv import load_dotenv

load_dotenv()


# Fetch variables from the .env file
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")
DATABRICKS_APP_URL = os.environ.get("DATABRICKS_APP_URL")

oauth_client = OAuthClient(
    host="https://plotly-customer-success.cloud.databricks.com",
    client_id=DATABICKS_CLIENT_ID,
    client_secret=DATABRICKS_CLIENT_SECRET,
    redirect_url=DATABRICKS_APP_URL,
    scopes=["all-apis"]
)



w = WorkspaceClient(host=oauth_client.host)


dash.register_page(__name__, path="/delta-optimizer/connection_settings", title="Connection Settings")


def layout():
    return dmc.MantineProvider(
        children=dmc.NotificationsProvider(
            [
                html.Div(
                    [
                        html.Div(id="notifications-container"),
                        html.Div(id="notifications-container-profile"),
                        dmc.Title("Settings"),
                        dmc.Divider(variant="solid"),
                        dmc.Space(h=20),
                        dmc.Space(h=10),
                        dmc.Space(h=10),
                        html.Div(id="library-status"),
                        html.Div(id="engine-test-result"),
                        dmc.Space(h=10),
                        dmc.TextInput(
                            id="profile-name",
                            type="text",
                            placeholder="Enter profile name",
                            size="md",
                            className="input-field",
                        ),
                        dmc.Space(h=10),
                        dmc.TextInput(
                            id="workspace-url",
                            type="text",
                            placeholder="Enter workspace URL",
                            size="md",
                            className="input-field",
                        ),
                        dmc.Space(h=10),
                        dmc.TextInput(
                            id="path",
                            type="text",
                            placeholder="Enter SQL Warehouse HTTP Path",
                            size="md",
                            className="input-field",
                        ),
                        dmc.Space(h=10),
                        dmc.TextInput(
                            id="token",
                            type="text",
                            placeholder="Enter token",
                            size="md",
                            className="input-field",
                        ),
                        dmc.Group(
                            position="left",
                            mt="xl",
                            children=[
                                dmc.Button(
                                    "Save Profile",
                                    rightIcon=DashIconify(
                                        icon="codicon:run-above",
                                    ),
                                    id="activate-button",
                                    size="md",
                                ),
                                dmc.Button(
                                    id="reset-button",
                                    size="md",
                                    children="Create New Profile",
                                    rightIcon=DashIconify(
                                        icon="codicon:run-above",
                                    ),
                                ),
                                dmc.Button(
                                    id="switch-button",
                                    size="md",
                                    children="Switch Profile",
                                    rightIcon=DashIconify(
                                        icon="codicon:run-above",
                                    ),
                                ),
                                html.Div(id="selected-profile"),
                            ],
                        ),
                        dmc.Space(h=10),
                        dmc.Space(h=10),
                        dcc.Store(id="selected-profile-store", storage_type="memory"),
                        dcc.Store(id="engine-store", storage_type="memory"),
                        dcc.Store(id="profile-store", storage_type="local"),
                        html.Div(id="dummy", style={"display": "none"}),
                        dcc.Store(id="cluster-options-store", storage_type="memory"),
                        html.Div(
                            id="success-message", children="", style={"display": "none"}
                        ),  # Hidden success message
                        dmc.Modal(
                            id="user-modal",
                            title="Select Profile",
                            children=[
                                dmc.Select(
                                    data=[],
                                    id="profile-radio",
                                ),
                                dmc.Space(h=20),
                                dmc.Group(
                                    position="right",
                                    children=[
                                        dmc.Button(
                                            id="switch-profile-button",
                                            children="Switch Profile",
                                            size="md",
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ]
        )
    )


@callback(
    Output("user-modal", "opened"),
    Input("switch-button", "n_clicks"),
    Input("switch-profile-button", "n_clicks"),
    State("user-modal", "opened"),
)
def modal_demo(n_clicks, is_open, switch):
    if n_clicks is not None:
        return not is_open
    if switch is not None:
        return not is_open

    return is_open


def get_most_recent_profile():
    # Retrieve the most recent profile name from the configuration file
    config = ConfigParser()
    file_path = "./.databrickscfg"  # Relative path in the root directory

    if os.path.exists(file_path):
        config.read(file_path)
        profiles = config.sections()

        if profiles:
            most_recent_profile = profiles[
                -1
            ]  # Retrieve the last (most recent) profile
            print(
                "Most recent profile:", most_recent_profile
            )  # Add this print statement
            return most_recent_profile

    return None


@callback(
    [
        Output("profile-name", "value"),
        Output("workspace-url", "value"),
        Output("path", "value"),
        Output("token", "value"),
        Output("success-message", "children"),
        Output("profile-store", "data"),
    ],
    [
        Input("dummy", "children"),
        Input("reset-button", "n_clicks"),
        Input("switch-profile-button", "n_clicks"),
    ],
    State("profile-radio", "value"),
)
def load_most_recent_profile(_, reset, switch, radio):
    # Get the most recently created profile name
    most_recent_profile = get_most_recent_profile()
    print(most_recent_profile)
    if reset:
        return (
            "",
            "",
            "",
            "",  # Hide the token value by returning an empty string
            "Success: Profile reset successfully.",
            "",
        )
    if switch:
        config = ConfigParser()
        file_path = "./.databrickscfg"  # Relative path in the root directory

        if os.path.exists(file_path):
            config.read(file_path)

        if config.has_section(radio):
            workspace_url = config.get(radio, "host")
            path = config.get(radio, "path")
            token = config.get(radio, "token")

            return (
                radio,
                workspace_url,
                path,
                "****************************",  # Hide the token value by returning an empty string
                "Success: Profile loaded successfully.",
                radio,
            )
    if most_recent_profile:
        # Load the stored state values as the initial values for the text input fields
        config = ConfigParser()
        file_path = "./.databrickscfg"  # Relative path in the root directory

        if os.path.exists(file_path):
            config.read(file_path)

        if config.has_section(most_recent_profile):
            workspace_url = config.get(most_recent_profile, "host")
            path = config.get(most_recent_profile, "path")
            token = config.get(most_recent_profile, "token")

            return (
                most_recent_profile,
                workspace_url,
                path,
                "****************************",  # Hide the token value by returning an empty string
                "Success: Profile loaded successfully.",
                most_recent_profile,
            )

    # If no most recent profile or stored state, return empty values
    return (
        "",
        "",
        "",
        "",
        "Error: Unable to load profile.",
        "",
    )


def is_valid_workspace_url(url):
    valid_schemes = ["http://", "https://"]
    valid_suffixes = [
        ".com",
        ".org",
        ".net",
        ".gov",
        ".edu",
    ]  # Add other valid suffixes if needed
    return (
        any(url.startswith(scheme) for scheme in valid_schemes)
        and not url.endswith("/")
        and any(url.endswith(suffix) for suffix in valid_suffixes)
    )


@callback(
    [
        Output("notifications-container-profile", "children"),
        Output("profile-name", "disabled"),
        Output("workspace-url", "disabled"),
        Output("path", "disabled"),
        Output("token", "disabled"),
    ],
    [
        Input("activate-button", "n_clicks"),
        Input("success-message", "children"),
        Input("reset-button", "n_clicks"),
    ],
    [
        State("profile-name", "value"),
        State("workspace-url", "value"),
        State("path", "value"),
        State("token", "value"),
        # State("success-message", "children"),
    ],
)
def generate_file(
    n_clicks, success_message, reset, profile_name, workspace_url, path, token
):
    if reset:
        return (
            None,
            False,
            False,
            False,
            False,
        )
    if success_message:
        return (
            None,
            True,
            True,
            True,
            True,
        )
    if n_clicks is not None and n_clicks > 0:
        if not profile_name or not workspace_url or not token or not path:
            return (
                "Please fill in all the fields.",
                False,
                False,
                False,
                False,
            )

        if not is_valid_workspace_url(workspace_url):
            return (
                "Invalid workspace URL. The URL must start with 'http://' or 'https://' and should not end with a slash.",
                False,
                False,
                False,
                False,
            )

        config = ConfigParser()
        file_path = "./.databrickscfg"  # Relative path in the root directory

        if os.path.exists(file_path):
            config.read(file_path)

        if not config.has_section(profile_name):
            config.add_section(profile_name)

        config.set(profile_name, "host", workspace_url)
        config.set(profile_name, "path", path)
        config.set(profile_name, "token", token)

        with open(file_path, "w") as file:
            config.write(file)

        message = f"Profile '{profile_name}' created successfully."

        # Disable text inputs when the profile is successfully created
        return comp.notification_user(message), True, True, True, True

    return None, False, False, False, False


# Function to get the profile names from the databricks config file
def get_profile_names():
    file_path = os.path.expanduser("./.databrickscfg")
    config = ConfigParser()

    if os.path.exists(file_path):
        config.read(file_path)

    return config.sections()


@callback(
    Output("profile-radio", "data"),
    Input("dummy", "children"),
)
def update_profile_dropdown(dummy):
    profile_names = get_profile_names()
    options = [{"label": name, "value": name} for name in profile_names]
    return options


# Callback to activate the selected profile
@callback(
    Output("selected-profile", "children"),
    Output("selected-profile-store", "data"),
    Output("notifications-container", "children"),
    [Input("activate-button", "n_clicks")],
    [State("profile-radio", "value")],
)
def activate_profile(n_clicks, profile_name):
    if n_clicks and profile_name:
        try:
            dbfs_path = f"dbfs:/tmp/{profile_name}"

            # List files in dbfs path (you might want to refine this part)
            files_in_dbfs = w.dbfs.list(dbfs_path)
            output = [file.path for file in files_in_dbfs]
            print(output)

            url = "https://github.com/CodyAustinDavis/edw-best-practices/raw/main/Delta%20Optimizer/deltaoptimizer-1.5.0-py3-none-any.whl"
            local_path = "./deltaoptimizer-1.5.0-py3-none-any.whl"
            dbfs_path_whl = f"{dbfs_path}/deltaoptimizer-1.5.0-py3-none-any.whl"

            # Download the file from URL
            urllib.request.urlretrieve(url, local_path)

            # Upload the file to DBFS using Databricks SDK
            with open(local_path, "rb") as f:
                w.dbfs.upload(dbfs_path_whl, f)

            activated_prof_noti = "File uploaded successfully."
            notification = f"Profile '{profile_name}' activated successfully. {activated_prof_noti}"

            return [
                html.Div(
                    [
                        dmc.Text(f"Activated Profile: {profile_name}. "),
                    ]
                ),
                profile_name,
                comp.notification_user(notification),
            ]
        except Exception as e:
            error_message = str(e)
            notification = f"Error executing command: {error_message}"

            return [
                html.Div([html.H3(notification)]),
                None,
                comp.notification_user(error_message),
            ]
    return ["", None, None]
