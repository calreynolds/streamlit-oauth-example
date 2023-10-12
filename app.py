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



APP_NAME = "delta_optimizer_latest"


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    use_pages=True,
    suppress_callback_exceptions=True,
    routes_pathname_prefix='/delta-optimizer/'
    )

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


# 1. Before request hook to check for authentication
@server.before_request
def check_authentication():
    if request.endpoint not in ['login', 'callback'] and "creds" not in session:
        return redirect(url_for('login'))



# 2. Separate login route to initiate the OAuth process
@server.route('/delta-optimizer/login')
def login():
    consent = oauth_client.initiate_consent()
    session["consent"] = consent.as_dict()
    return redirect(consent.auth_url)


# 3. Your callback remains the same but adjusted for dash-pages
@server.route("/delta-optimizer/callback")
def callback():
    if "consent" in session:
        consent = Consent.from_dict(oauth_client, session["consent"])
        session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()

    # Redirect to the main app page
    return redirect('/delta-optimizer/build-strategy')



if __name__ == "__main__":
    app.run(debug=True, port=8050)
