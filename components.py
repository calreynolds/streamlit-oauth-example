import dash_mantine_components as dmc
import dash
from dash import dcc, html
from dash_iconify import DashIconify
import dash_bootstrap_components as dbc


from dash import html, dcc
import dash_mantine_components as dmc


# the style arguments for the sidebar. We use position:fixed and a fixed width


LEFT_SIDEBAR = dmc.Stack(
    className="sidebar",
    style={
        "backgroundColor": "#0F1D22",
    },
    mt=20,
    mb=20,
    ml=20,
    children=[
        html.A(
            [
                html.Img(
                    src=dash.get_asset_url("dbxdashlogo2.png"),
                    style={
                        "height": "100%",
                        "width": "100%",
                        "float": "center",
                        "position": "relative",
                        "padding-top": 25,
                        "padding-right": 25,
                        "padding-left": 0,
                    },
                )
            ],
            href="https://databricks-dash.aws.plotly.host/databrickslakeside/dbx-console",
        ),
        dmc.NavLink(
            label="Data Explorer",
            href=dash.get_relative_path("/explorer"),
            variant="subtle",
            icon=DashIconify(icon="ri:pie-chart-fill", width=20, color="#FFFFFF"),
            className="nav-link-component",
        ),
        dmc.NavLink(
            label="Optimizer",
            icon=DashIconify(icon="tabler:file-delta", width=20, color="#FFFFFF"),
            childrenOffset=20,
            children=[
                dmc.NavLink(
                    label="Build Strategy",
                    href=dash.get_relative_path("/build-strategy"),
                    variant="subtle",
                    icon=DashIconify(icon="mdi:brain", width=20, color="#FFFFFF"),
                    className="nav-link-component",
                ),
                dmc.NavLink(
                    label="Schedule + Run",
                    href=dash.get_relative_path("/optimizer-runner"),
                    variant="subtle",
                    icon=DashIconify(icon="carbon:run", width=20, color="#FFFFFF"),
                    className="nav-link-component",
                ),
                dmc.NavLink(
                    label="Results",
                    href=dash.get_relative_path("/optimizer-results"),
                    variant="subtle",
                    icon=DashIconify(
                        icon="mingcute:presentation-2-fill", width=20, color="#FFFFFF"
                    ),
                    className="nav-link-component",
                ),
            ],
            className="nav-link-component",
        ),
        dmc.NavLink(
            label="Settings",
            href=dash.get_relative_path("/connection_settings"),
            icon=DashIconify(
                icon="material-symbols:settings", width=20, color="#FFFFFF"
            ),
            className="nav-link-component",
        ),
    ],
)


def notification_user(text):
    return dmc.Notification(
        id="notify-user",
        title="Activation Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        icon=[DashIconify(icon="mdi:account-check", width=128)],
        action="show",
        autoClose=False,
    )


def notification_job1_error(text):
    return dmc.Notification(
        id="notify-user",
        title="Activation Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        color="red",
        icon=[DashIconify(icon="material-symbols:error-outline", width=128)],
        action="show",
        autoClose=False,
    )


def notification_delete(text):
    return dmc.Notification(
        id="notify-user",
        title="Deletion Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        color="white",
        icon=[DashIconify(icon="typcn:delete-outline", width=128)],
        action="show",
        autoClose=False,
    )


def notification_update_schedule(text):
    return dmc.Notification(
        id="notify-user",
        title="Schedule Update Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        color="black",
        icon=[DashIconify(icon="line-md:calendar", width=128)],
        action="show",
        autoClose=False,
    )


def notification_update_pause(text):
    return dmc.Notification(
        id="notify-user",
        title="Pause Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        color="black",
        icon=[DashIconify(icon="zondicons:pause-outline", width=128)],
        action="show",
        autoClose=False,
    )


def notification_user_step_1(text):
    return dmc.Notification(
        id="notify-user-step-1",
        title="Job Status",
        message=[text],
        disallowClose=False,
        radius="xl",
        icon=[DashIconify(icon="material-symbols:build-circle-outline", width=128)],
        action="show",
        autoClose=False,
    )


def cluster_loading(text):
    return dmc.Notification(
        id="cluster-loading",
        title="Process initiated",
        message=[text],
        loading=True,
        radius="xl",
        color="orange",
        action="show",
        autoClose=False,
        disallowClose=False,
    )


def cluster_loaded(text):
    return dmc.Notification(
        id="cluster-loaded",
        title="Data loaded",
        message=[text],
        radius="xl",
        color="green",
        action="show",
        icon=DashIconify(icon="akar-icons:circle-check"),
    )


FOOTER = html.Footer(
    className="footer",
    children=[
        html.Div(
            className="footer-content",
            children=[
                html.A(
                    "© 2023-Plotly Inc.", href="https://plotly.com/", target="_blank"
                ),
                html.Div(
                    className="footer-links",
                    children=[
                        html.A(
                            "About",
                            href="https://www.databricks.com/company/about-us",
                            target="_blank",
                        ),
                        html.A(
                            "Databricks+Dash",
                            href="https://dash-demo.plotly.host/plotly-dash-500/snapshot-1684467228-670d42dd",
                            target="_blank",
                        ),
                        html.A(
                            "Blog Posts",
                            href="https://medium.com/plotly/build-real-time-production-data-apps-with-databricks-plotly-dash-269cb64b7575",
                            target="_blank",
                        ),
                        html.A(
                            "Contact",
                            href="https://www.databricks.com/company/contact",
                            target="_blank",
                        ),
                    ],
                ),
            ],
        ),
    ],
)


from dash import html, dcc

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "22rem",
    "padding": "1rem 1rem",
    "background-color": "#f8f9fa",
}


submenu_1 = [
    html.Li(
        # use Row and Col components to position the chevrons
        dbc.Row(
            [
                dbc.Col("Menu 1"),
                dbc.Col(
                    html.I(className="fas fa-chevron-right me-3"),
                    width="auto",
                ),
            ],
            className="my-1",
        ),
        style={"cursor": "pointer"},
        id="submenu-1",
    ),
    # we use the Collapse component to hide and reveal the navigation links
    dbc.Collapse(
        [
            dbc.NavLink(
                "Stategy Builder",
                href=dash.get_relative_path("/build-strategy"),
            ),
            dbc.NavLink(
                "Schedule+Run", href=dash.get_relative_path("/optimizer-runner")
            ),
            dbc.NavLink("Results", href=dash.get_relative_path("/optimizer-results")),
        ],
        id="submenu-1-collapse",
    ),
]


def footer():
    return html.Footer(
        className="footer",
        children=[
            html.Div(
                className="footer-content",
                children=[
                    html.A(
                        "© 2023-Plotly Inc.",
                        href="https://plotly.com/",
                        target="_blank",
                    ),
                    html.Div(
                        className="footer-links",
                        children=[
                            html.A(
                                "About",
                                href="https://www.databricks.com/company/about-us",
                                target="_blank",
                            ),
                            html.A(
                                "Databricks+Dash",
                                href="https://dash-demo.plotly.host/plotly-dash-500/snapshot-1684467228-670d42dd",
                                target="_blank",
                            ),
                            html.A(
                                "Blog Posts",
                                href="https://medium.com/plotly/build-real-time-production-data-apps-with-databricks-plotly-dash-269cb64b7575",
                                target="_blank",
                            ),
                            html.A(
                                "Contact",
                                href="https://www.databricks.com/company/contact",
                                target="_blank",
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
