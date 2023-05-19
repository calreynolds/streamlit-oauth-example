import dash

dash.register_page(__name__, path="/", title="Welcome")


def layout():
    return dash.html.Div(
        "Welcome to the Databricks Delta Optimizer",
        style={"textAlign": "center", "fontSize": 30},
    )
