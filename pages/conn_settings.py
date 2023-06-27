import os
from configparser import ConfigParser
import urllib.request
import subprocess
import dash
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
from sqlalchemy.engine import create_engine
import components as comp


dash.register_page(__name__, path="/connection_settings", title="Connection Settings")


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
                        dmc.Select(
                            id="profile-dropdown",
                            data=[],
                            placeholder="Select a profile",
                        ),
                        dmc.Group(
                            position="left",
                            mt="xl",
                            children=[
                                dmc.Button(
                                    "Activate Profile",
                                    rightIcon=DashIconify(
                                        icon="codicon:run-above",
                                    ),
                                    id="activate-button",
                                ),
                                html.Div(id="selected-profile"),
                            ],
                        ),
                        dmc.Space(h=10),
                        dmc.Space(h=10),
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
                                                color=dmc.theme.DEFAULT_COLORS["blue"][
                                                    6
                                                ],
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
                                                html.Label("SQL Warehouse HTTP Path"),
                                                dmc.TextInput(
                                                    id="path",
                                                    type="text",
                                                    placeholder="Enter SQL Warehouse HTTP Path",
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
                        dcc.Store(id="selected-cluster-store", storage_type="session"),
                        dcc.Store(id="selected-profile-store", storage_type="memory"),
                        dcc.Store(id="engine-store", storage_type="memory"),
                        html.Div(id="dummy", style={"display": "none"}),
                        dcc.Store(id="cluster-options-store", storage_type="memory"),
                    ],
                ),
            ]
        )
    )


@callback(
    Output("generate-button", "n_clicks"),
    Output("notifications-container-profile", "children"),
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
        if not profile_name or not workspace_url or not token or not path:
            return None, comp.notification_user("Please fill in all the fields.")

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
        return 0, comp.notification_user(message)

    return n_clicks, None


# Function to get the profile names from the databricks config file
def get_profile_names():
    file_path = os.path.expanduser("~/.databrickscfg")
    config = ConfigParser()

    if os.path.exists(file_path):
        config.read(file_path)

    return config.sections()


@callback(
    Output("profile-dropdown", "data"),
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
    [State("profile-dropdown", "value")],
)
def activate_profile(n_clicks, profile_name):
    if n_clicks and profile_name:
        command = f"databricks fs ls dbfs:/ --profile {profile_name}"
        try:
            result = subprocess.run(command, capture_output=True, text=True, shell=True)
            output = result.stdout.strip()

            url = "https://github.com/CodyAustinDavis/edw-best-practices/raw/main/Delta%20Optimizer/deltaoptimizer-1.5.0-py3-none-any.whl"
            local_path = "/tmp/deltaoptimizer-1.5.0-py3-none-any.whl"
            dbfs_path = "dbfs:/tmp/deltaoptimizer-1.5.0-py3-none-any.whl"

            test = urllib.request.urlretrieve(url, local_path)
            print(test)

            upload_command = [
                f"databricks fs cp {local_path} {dbfs_path} --profile {profile_name}"
            ]
            upload = subprocess.run(
                upload_command, capture_output=True, text=True, shell=True
            )
            activated_prof_noti = upload.stdout.strip()

            if "RESOURCE_ALREADY_EXISTS" in activated_prof_noti:
                notification = f"Profile '{profile_name}' already activated."
            else:
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
        except subprocess.CalledProcessError as e:
            error_message = str(e)
            notification = f"Error executing command: {error_message}"

            return [
                html.Div([html.H3(notification)]),
                None,
                comp.notification_user(error_message),
            ]
    return ["", None, None]
