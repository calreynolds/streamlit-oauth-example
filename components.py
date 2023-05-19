import dash_mantine_components as dmc
import dash
from dash import dcc, html
from dash_iconify import DashIconify

LEFT_SIDEBAR = dmc.Stack(
    style={
        "backgroundColor": "#303F47",
    },
    mt=20,
    mb=20,
    ml=20,
    children=[
        dmc.Button(
            html.A(
                "databricks",
                href="https://www.databricks.com/",
                target="_blank",
            ),
            leftIcon=DashIconify(
                icon="simple-icons:databricks", width=40, color="#FF3621"
            ),
            variant="subtle",
            className="nav-link-component",
            m=20,
        ),
        dmc.NavLink(
            label="Console",
            href=dash.get_relative_path("/dbx-console"),
            variant="subtle",
            icon=DashIconify(icon="ri:pie-chart-fill", width=20, color="#9ca3af"),
            className="nav-link-component",
        ),
        dmc.NavLink(
            label="Delta Optimizer",
            childrenOffset=28,
            children=[
                dmc.NavLink(
                    label="Config",
                    href=dash.get_relative_path("/optimizer"),
                    variant="subtle",
                    icon=DashIconify(
                        icon="mingcute:presentation-2-fill", width=20, color="#9ca3af"
                    ),
                    className="nav-link-component",
                ),
                dmc.NavLink(
                    label="Results",
                    href=dash.get_relative_path("/optimizer-results"),
                    variant="subtle",
                    icon=DashIconify(
                        icon="mingcute:presentation-2-fill", width=20, color="#9ca3af"
                    ),
                    className="nav-link-component",
                ),
            ],
            className="nav-link-component",
        ),
        dmc.NavLink(
            label="Admin Settings",
            href=dash.get_relative_path("/settings"),
            variant="subtle",
            icon=DashIconify(
                icon="material-symbols:settings", width=20, color="#9ca3af"
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
                            href="https://themesberg.com/about",
                            target="_blank",
                        ),
                        html.A(
                            "Databricsks+Dash",
                            href="https://themesberg.com/themes",
                            target="_blank",
                        ),
                        html.A(
                            "Blog Posts",
                            href="https://themesberg.com/blog",
                            target="_blank",
                        ),
                        html.A(
                            "Contact",
                            href="https://themesberg.com/contact",
                            target="_blank",
                        ),
                    ],
                ),
            ],
        )
    ],
)
