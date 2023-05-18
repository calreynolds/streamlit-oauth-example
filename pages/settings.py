import dash
import json
import requests
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import pandas as pd
import dash_ag_grid as dag
from sqlalchemy.engine import create_engine
from flask_sqlalchemy import SQLAlchemy

dash.register_page(__name__, path="/settings", title="Settings")

from dash import html, dcc, Input, Output, callback
import dash
import pandas as pd
from sqlalchemy import Table, create_engine
import dash_ag_grid as dag


SERVER_HOSTNAME = "plotly-customer-success.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f08f0b85ddba8d2e"
ACCESS_TOKEN = "dapia86bd9f9bc3504ca74a4966c0e669002"
CATALOG = "main"
SCHEMA = "dbxdashstudio"
INFORMATION_SCHEMA = "information_schema"

conn_str = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}"
extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL Example Script",
}
main_engine = create_engine(
    conn_str,
    connect_args=extra_connect_args,
)

conn_str = f"databricks://token:{ACCESS_TOKEN}@{SERVER_HOSTNAME}?http_path={HTTP_PATH}&catalog={CATALOG}&schema={INFORMATION_SCHEMA}"
extra_connect_args = {
    "_tls_verify_hostname": True,
    "_user_agent_entry": "PySQL Example Script",
}
schema_engine = create_engine(
    conn_str,
    connect_args=extra_connect_args,
)

userstmt = f"Select * FROM main.dbxdashstudio.engines;"
dataframe = pd.read_sql_query(userstmt, main_engine)

db = SQLAlchemy()


class Engines(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(15), unique=True, nullable=False)
    engine_name = db.Column(db.String(50), unique=True)


engines_tbl = Table("engines", Engines.metadata)


def layout():
    return html.Div(
        [
            dmc.Accordion(
                children=[
                    dmc.AccordionItem(
                        [
                            dmc.AccordionControl("Create a new User"),
                            dmc.AccordionPanel(
                                [
                                    dmc.TextInput(
                                        id="hostname",
                                        label="Enter Server Hostname:",
                                        placeholder="dbc-a2c61234-1234.cloud.databricks.com",
                                    ),
                                    dmc.TextInput(
                                        id="path",
                                        label="Enter HTTP Path:",
                                        placeholder="sql/protocolv1/o/4337956624071234/1234-123456-pane123",
                                    ),
                                    dmc.TextInput(
                                        id="token",
                                        label="Enter Access Token:",
                                        placeholder="token",
                                    ),
                                    dmc.TextInput(
                                        id="catalog",
                                        label="Enter Catalog:",
                                        placeholder="catalog",
                                    ),
                                    dmc.TextInput(
                                        id="schema",
                                        label="Enter Database:",
                                        placeholder="database schema",
                                    ),
                                    dmc.TextInput(
                                        id="username",
                                        label="Enter Username:",
                                        placeholder="This can be whatever your want",
                                    ),
                                    dmc.Space(h=20),
                                    dmc.Text(id="intermediate_engine", align="center"),
                                    dmc.Space(h=20),
                                    dmc.Group(
                                        grow=True,
                                        children=[
                                            dmc.Button(
                                                "Add User Engine to Session",
                                                id="createconn",
                                            ),
                                            dmc.Button(
                                                "Update the List of Users",
                                                id="userlist",
                                                n_clicks=0,
                                            ),
                                        ],
                                    ),
                                ]
                            ),
                        ],
                        value="create-new-user",
                    ),
                ],
            ),
            dmc.Space(h=20),
            dmc.Select(
                label="Listed Users:",
                placeholder="Select user to test connection",
                id="username-dpdn",
                searchable=True,
                style={"width": 200, "marginBottom": 10},
            ),
            dmc.LoadingOverlay(
                overlayOpacity=0.95,
                loaderProps=dict(color="orange", variant="bars"),
                children=dmc.RadioGroup(
                    id="radio-group-engine-dpdn",
                    label="Connected Engines:",
                    size="md",
                    mt=10,
                ),
            ),
            dmc.Text("Tables in Selected Engine:", weight=500, mt=10),
            dmc.LoadingOverlay(
                overlayOpacity=0.95,
                loaderProps=dict(color="orange", variant="bars"),
                children=dmc.List(
                    id="user-tables",
                    size="md",
                    mt=10,
                ),
            ),
        ]
    )


@callback(
    Output("intermediate_engine", "children"),
    Input("createconn", "n_clicks"),
    State("token", "value"),
    State("hostname", "value"),
    State("path", "value"),
    State("catalog", "value"),
    State("schema", "value"),
    State("username", "value"),
    prevent_initial_call=True,
)
def create_connection(n_clicks, token, hostname, path, catalog, schema, username):
    conn_str = f"databricks://token:{token}@{hostname}?http_path={path}&catalog={catalog}&schema={schema}"
    extra_connect_args = {
        "_tls_verify_hostname": True,
        "_user_agent_entry": "PySQL Example Script",
    }
    engine = create_engine(
        conn_str,
        connect_args=extra_connect_args,
    )
    if username is not None:
        ins = engines_tbl.insert().values(
            username={username}, engine_name=f"{engine.url}"
        )
        conn = main_engine.connect()
        conn.execute(ins)
        conn.close()
    if not n_clicks:
        return "Action failed, please check your inputs and try again."
    return f"Successfully added user and engine to session!"


@callback(
    Output("username-dpdn", "data"),
    Input("userlist", "n_clicks"),
)
def getuserlist(n_clicks):
    userstmt = f"Select * FROM main.dbxdashstudio.engines;"
    df = pd.read_sql_query(userstmt, main_engine)
    return [{"label": c, "value": c} for c in sorted(df.username.unique())]


@callback(
    Output("radio-group-engine-dpdn", "children"),
    Input("username-dpdn", "value"),
    prevent_initial_call=True,
)
def set_engine_value(available_options):
    dff = pd.read_sql_query(
        "Select engine_name FROM main.dbxdashstudio.engines WHERE username = '{}'".format(
            available_options
        ),
        main_engine,
    )
    return [dmc.Radio(name, value=name) for name in sorted(dff.engine_name.unique())]


@callback(
    Output("user-tables", "children"),
    Input("radio-group-engine-dpdn", "value"),
    prevent_initial_call=True,
)
def checkfortables(available_options):
    tableoption_init_statement = (
        f"SELECT * FROM main.INFORMATION_SCHEMA.tables LIMIT 1;"
    )
    try:
        tablecheck_list = pd.read_sql_query(
            tableoption_init_statement, available_options
        )
        return [
            dmc.ListItem(name) for name in sorted(tablecheck_list.table_name.unique())
        ]
    except:
        return dmc.Text("No Tables Found")
