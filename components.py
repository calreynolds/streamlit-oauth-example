import dash_mantine_components as dmc
import dash
from dash import dcc, html
from dash_iconify import DashIconify


from dash import html, dcc
import dash_mantine_components as dmc


# the style arguments for the sidebar. We use position:fixed and a fixed width


LEFT_SIDEBAR = dmc.Stack(
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
        # "databricks",
        # href="https://www.databricks.com/",
        # target="_blank",
        # leftIcon=DashIconify(
        #     icon="simple-icons:databricks", width=40, color="#FF3621"
        # ),
        # variant="subtle",
        # className="nav-link-component",
        # m=20,
        dmc.NavLink(
            label="Console",
            href=dash.get_relative_path("/dbx-console"),
            variant="subtle",
            icon=DashIconify(icon="ri:pie-chart-fill", width=20, color="#FFFFFF"),
            className="nav-link-component",
        ),
        dmc.NavLink(
            label="Delta Optimizer",
            icon=DashIconify(icon="tabler:file-delta", width=20, color="#FFFFFF"),
            childrenOffset=28,
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
            variant="subtle",
            active=True,
            icon=DashIconify(
                icon="material-symbols:settings", width=20, color="#FFFFFF"
            ),
            className="nav-link-component",
        ),
    ],
)
FOOTER = dmc.Footer(
    height=50,
    fixed=True,
    className="footer",
    children=[
        dmc.Group(
            position="apart",
            mt=10,
            children=[
                html.A(
                    "© 2023-Plotly Inc.", href="https://plotly.com/", target="_blank"
                ),
                dmc.Group(
                    position="right",
                    spacing="xl",
                    align="center",
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
        )
    ],
)
