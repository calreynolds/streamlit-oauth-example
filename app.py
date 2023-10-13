#!env python3
import os
import logging
import secrets
import dash
import sys
from dash import html, dcc, Output, Input
from dotenv import load_dotenv
from flask import redirect, session, url_for, request
from flask import Flask, session, redirect, request, make_response

from databricks.sdk.oauth import OAuthClient, Consent
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import SessionCredentials
from dash import Dash, dcc, html
from flask_session import Session
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
import dash


load_dotenv()

# Fetch variables from the .env file
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET")
DATABRICKS_APP_URL = os.environ.get("DATABRICKS_APP_URL")



APP_NAME = "delta_optimizer_latest_one"


app = dash.Dash(__name__, use_pages=True, suppress_callback_exceptions=True, routes_pathname_prefix="/delta-optimizer/")
server = app.server
server.secret_key = secrets.token_urlsafe(32)


# Session(app)
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

import logging
from dash import dcc, html, Output, Input
import dash_mantine_components as dmc
from datetime import timedelta

logging.basicConfig(level=logging.DEBUG)

@app.callback(Output("auth-action", "children"), [Input("url", "pathname")])
def display_page(pathname):
    logging.debug(f"===== Display page accessed with pathname: {pathname} =====")
    
    if "creds" not in session:
        logging.debug("Step 1: No creds found in session, initiating consent.")
        consent = oauth_client.initiate_consent()
        session["consent"] = consent.as_dict()
        session["requested_path"] = pathname  # Store the current pathname
        logging.debug(f"Step 2: Consent URL generated: {consent.auth_url}")
        logging.debug("Step 3: Prompting user to click authentication link.")
        return html.A("Click here to authenticate", href=consent.auth_url)
    else:
        # If the user is authenticated, redirect them to the originally requested page
        requested_path = session.get("requested_path", "/delta-optimizer/build-strategy")
        return dcc.Location(pathname=requested_path, id='redirect')

 
@server.route("/delta-optimizer/callback")
def callback():
    logging.debug("Step 5: Callback received. Fetching credentials.")

    try:
        if "consent" in session:
            logging.debug("Step 7: Consent found in session.")
            consent = Consent.from_dict(oauth_client, session["consent"])
            session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
            logging.debug("Step 8: Credentials successfully obtained and stored in session.")
    except Exception as e:
        logging.error(f"Step 10: Error processing callback: {e}")

    logging.debug("Step 11: Redirecting to the originally requested page.")
    requested_path = session.get("requested_path", "/delta-optimizer/build-strategy")
    return redirect(requested_path)



# Define your app's layout
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
        dcc.Location(id='url', refresh=False),
        TOP_NAVBAR,
        LEFT_SIDEBAR,
        dmc.Container(
        className="background-container"
    ),
    dmc.Container(
        className="page",
        children=[
            dash.page_container,
            html.Div(id='auth-action')  # Placeholder for the authentication link
        ]
    )

    ],
)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
