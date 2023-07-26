from dash import Dash, dcc
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
import dash


app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    # url_base_pathname=os.environ.get("DASH_URL_BASE_PATHNAME", "/"),
)
server = app.server

from components import (
    LEFT_SIDEBAR,
    FOOTER_FIXED,
)  # noqa: E402 isort:skip - must be imported after app is defined

app.layout = dmc.MantineProvider(
    withGlobalStyles=True,
    theme={
        "primaryColor": "dbx-orange",
        "colors": {
            "dbx-orange": [
                "#FFB4AC",
                "#FFB4AC",
                "#FFB4AC",
                "#FFB4AC",
                "#FF9B90",
                "#FF8174",
                "#FF6859",
                "#FF4F3D",
                "#FF3621",
            ]
        },
    },
    children=[
        dmc.Grid(
            m=0,
            children=[
                dmc.Col(
                    LEFT_SIDEBAR,
                    span=2,
                    style={"backgroundColor": "#0F1D22"},
                    p=0,
                ),
                dmc.Col(
                    dash.page_container,
                    className="page",
                    span=10,
                    p=20,
                ),
            ],
        ),
        FOOTER_FIXED,
        dcc.Store(
            id="general-store", data={"outputdpdn2": "main.delta_optimizer_mercury"}
        ),
    ],
)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
