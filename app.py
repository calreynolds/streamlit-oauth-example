from dash import Dash, dcc, html
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
    TOP_NAVBAR,
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
        html.Script(
            """
        document.addEventListener('mousemove', function(e) {
            let x = e.clientX;
            let y = e.clientY;
            let windowWidth = window.innerWidth;
            let windowHeight = window.innerHeight;

            let percentageX = (x / windowWidth) * 100;
            let percentageY = (y / windowHeight) * 100;

            let element = document.querySelector('.my-animation');
            if (element) {
                element.style.background = `linear-gradient(${percentageX}deg, red, blue), linear-gradient(${percentageY}deg, yellow, green)`;
            }
        });
    """
        ),
        TOP_NAVBAR,
        LEFT_SIDEBAR,
        dmc.Container(
            className="background-container",
        ),
        dmc.Container(
            dash.page_container,
            className="page",
        ),
        # dmc.Grid(
        #     m=0,
        #     children=[
        #         dmc.Col(
        #             LEFT_SIDEBAR,
        #             span=2,
        #             style={"backgroundColor": "#0F1D22"},
        #             p=0,
        #         ),
        #         dmc.Col(
        #             dash.page_container,
        #             className="page",
        #             span="auto",
        #             p=20,
        #         ),
        #     ],
        # ),
        # FOOTER_FIXED,
        dcc.Store(
            id="general-store", data={"outputdpdn2": "main.delta_optimizer_mercury"}
        ),
    ],
)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
