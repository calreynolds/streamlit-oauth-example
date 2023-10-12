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
    suppress_callback_exceptions=True
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

import logging
logging.basicConfig(level=logging.DEBUG)


# 1. Before request hook to check for authentication
@server.before_request
def check_authentication():
    logging.debug(f"Checking authentication for endpoint: {request.endpoint}")
    
    if request.endpoint not in ['login', 'callback', 'static']:
        if "creds" not in session:
            logging.warning("No creds found in session. Redirecting to login.")
            return redirect(url_for('login'))
        else:
            logging.info("Creds found in session. Continuing with the request.")

# 2. Separate login route to initiate the OAuth process
@server.route('/delta-optimizer/login')
def login():
    if "creds" in session:
        logging.info("User is already authenticated. Redirecting to main app page.")
        return redirect('/delta-optimizer/build-strategy')

    logging.info("Initiating OAuth flow.")
    consent = oauth_client.initiate_consent()
    session["consent"] = consent.as_dict()
    logging.debug(f"Consent stored in session: {session['consent']}")
    return redirect(consent.auth_url)

# 3. Your callback remains the same but adjusted for dash-pages
@server.route("/delta-optimizer/callback")
def callback():
    logging.debug(f"===== Callback accessed with arguments: {request.args} =====")
    
    try:
        if "consent" in session:
            logging.debug("Step 7: Consent found in session.")
            consent = Consent.from_dict(oauth_client, session["consent"])
            session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
            logging.debug("Step 8: Credentials successfully obtained and stored in session.")
        else:
            logging.warning("Step 9: No consent found in session during callback.")
    except Exception as e:
        logging.error(f"Step 10: Error processing callback: {e}")
    
    logging.debug("Step 11: Redirecting to the clusters page.")
    return redirect('/delta-optimizer/build-strategy')


if __name__ == "__main__":
    app.run(debug=True, port=8050)
