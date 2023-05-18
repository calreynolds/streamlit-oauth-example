from dash import Dash, dcc, html
import dash_design_kit as ddk
import dash_mantine_components as dmc
import dash
from dash_iconify import DashIconify

app = Dash(__name__, use_pages=True)
server = app.server  # expose server variable for Procfile

LEFT_SIDEBAR = dmc.Stack(
    styles={"root": {"backgroundColor": dmc.theme.DEFAULT_COLORS["yellow"][1]}},
    children=[
        dmc.Button(
            "databricks",
            leftIcon=DashIconify(
                icon="simple-icons:databricks", width=40, color="orange"
            ),
            variant="subtle",
            size="xl",
        ),
        dmc.NavLink(
            label="Console",
            href=dash.get_relative_path("/dbx-console"),
            variant="subtle",
            rightSection=DashIconify(icon="ri:pie-chart-fill", width=20),
        ),
        dmc.NavLink(
            label="Delta Optimizer",
            icon=DashIconify(icon="tabler:gauge", height=16),
            childrenOffset=28,
            children=[
                dmc.NavLink(
                    label="Config",
                    href=dash.get_relative_path("/optimizer"),
                    variant="subtle",
                    rightSection=DashIconify(
                        icon="mingcute:presentation-2-fill", width=20
                    ),
                ),
                dmc.NavLink(
                    label="Results",
                    href=dash.get_relative_path("/optimizer-results"),
                    variant="subtle",
                    rightSection=DashIconify(
                        icon="mingcute:presentation-2-fill", width=20
                    ),
                ),
            ],
        ),
        dmc.NavLink(
            label="Admin Settings",
            href=dash.get_relative_path("/settings"),
            variant="subtle",
            rightSection=DashIconify(icon="material-symbols:settings", width=20),
        ),
    ],
)
FOOTER = dmc.Footer(height=50, fixed=True, children=[dmc.Text("© 2023-Plotly Inc.")])

app.layout = dmc.MantineProvider(
    withGlobalStyles=True,
    #  theme={"colorScheme": "dark"},
    children=[
        dmc.Grid(
            children=[
                dmc.Col(html.Div(LEFT_SIDEBAR), span=2),
                dmc.Col(
                    dmc.Stack(align="stretch", children=[dash.page_container, FOOTER]),
                    span=10,
                ),
            ]
        )
    ],
)


from flask import request, Response, stream_with_context


@app.server.route("/dbx-stream", methods=["POST"])
def streaming_chat():
    prompt = request.json["question"]

    import time  # TODO: remove this

    def send_messages(prompt):
        return "hello"  # TODO: add streaming chat from Dolly here
        # return openai.ChatCompletion.create(
        #     model="gpt-3.5-turbo",
        #     messages=prompt,
        #     stream=True,
        #     max_tokens=2024,
        #     temperature=0,
        # )

    def generate_content():
        response = "SELECT payment_desc, COUNT(*) FROM rawyellowtrips JOIN paymenttype ON paymenttype.payment_type = rawyellowtrips.payment_type GROUP BY paymenttype.payment_type, payment_desc"
        for i in response:
            yield i
            time.sleep(0.01)

    def response_stream():
        yield from (
            line.choices[0].delta.get("content", "") for line in send_messages(prompt)
        )

    content_generator = generate_content()
    return Response(stream_with_context(content_generator), mimetype="text/plain")


if __name__ == "__main__":
    app.run(debug=True)
