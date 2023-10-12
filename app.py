#!env python3
import os
import logging
import secrets
import dash
import sys
from dash import html, dcc, Output, Input
from dotenv import load_dotenv
from flask import redirect, session, url_for, request
from databricks.sdk.oauth import OAuthClient, Consent
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import SessionCredentials
from dash import Dash, dcc, html
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
import dash


load_dotenv()

# Fetch variables from the .env file
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")
DATABRICKS_APP_URL = os.environ.get("DATABRICKS_APP_URL")

APP_NAME = "delta_optimizer"


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    use_pages=True,
    routes_pathname_prefix="/delta-optimizer/")

server = app.server
server.secret_key = secrets.token_urlsafe(32)

oauth_client = OAuthClient(
    host=DATABRICKS_HOST,
    client_id=DATABICKS_CLIENT_ID,
    client_secret=DATABRICKS_CLIENT_SECRET,
    redirect_url=DATABRICKS_APP_URL,
    scopes=["all-apis"]
)

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
        TOP_NAVBAR,
        LEFT_SIDEBAR,
        dmc.Container(
            className="background-container",
        ),
        dcc.Location(id='url', refresh=False),
        dmc.Container(
           dash.page_container,
            className="page",
        ),
    ],
)   

# # Assuming you've defined a div with the id 'page-content' in your main layout:
# @app.callback(Output('page-content', 'children'),
#               [Input('url', 'pathname')])
# def display_page(pathname):
#     if pathname == '/delta-optimizer/build-strategy':
#         return build_strategy.layout
#     elif pathname == '/delta-optimizer/conn-settings':
#         return conn_settings.layout
#     elif pathname == '/delta-optimizer/dbx-console':
#         return dbx_console.layout
#     elif pathname == '/delta-optimizer/main':
#         return main.layout
#     elif pathname == '/delta-optimizer/results':
#         return results.layout
#     elif pathname == '/delta-optimizer/run-strategy':
#         return run_strategy.layout
#     else:
#         # This is if no match is found. You can redirect to a default page or display a 404 message.
#         return "404 Page not found"



# 1. Before request hook to check for authentication
@server.before_request
def check_authentication():
    print("Current endpoint:", request.endpoint)
    
    if "creds" not in session and request.endpoint not in ["delta-optimizer.login", "delta-optimizer.callback"]:
        logging.debug("No creds in session. Redirecting to login.")
        return redirect(url_for('login'))

# 2. Separate login route to initiate the OAuth process
@server.route('/delta-optimizer/login')
def login():
    consent = oauth_client.initiate_consent()
    session["consent"] = consent.as_dict()
    return redirect(consent.auth_url)

# 3. Your callback remains the same
@server.route("/delta-optimizer/callback")
def callback():
    logging.debug(f"Callback accessed with arguments: {request.args}")
    
    try:
        if "consent" in session:
            logging.debug("Consent found in session.")
            consent = Consent.from_dict(oauth_client, session["consent"])
            session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
        else:
            logging.warning("No consent found in session during callback.")
    except Exception as e:
        logging.error(f"Error processing callback: {e}")
    
    logging.debug("Redirecting to the default delta-optimizer page.")
    return redirect('/delta-optimizer/build_strategy')  # Redirect to the main app page

# 4. Dash callback to display the page content (simplified without the creds check)
@app.callback(
    Output(dash.page_container, 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    logging.debug(f"===== Display page accessed with pathname: {pathname} =====")
    
    return dmc.MantineProvider(
        withGlobalStyles=True,
        theme={
            "primaryColor": "dbx-orange",
            "colors": {
                "dbx-orange": [
                    "#FFB4AC", "#FFB4AC", "#FFB4AC", "#FFB4AC",
                    "#FF9B90", "#FF8174", "#FF6859", "#FF4F3D", "#FF3621"
                ]
            },
        },
        children=[
            TOP_NAVBAR,
            LEFT_SIDEBAR,
            dmc.Container(
                className="background-container",
            ),
            dmc.Container(
                dash.page_container,
                className="page",
            ),
        ],
    )



if __name__ == "__main__":
    app.run(debug=True, port=8050)
