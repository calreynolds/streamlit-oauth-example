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

import logging
from dash import dcc, html
import dash_mantine_components as dmc
from datetime import timedelta

# logging.basicConfig(level=logging.DEBUG)



# # Setting session to be permanent and define its lifetime
# server.config['SESSION_PERMANENT'] = True
# server.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=5)

# @server.before_request
# def check_authentication():
#     log_prefix = "[Before Request]"

#     # If credentials are already present in the session, simply return
#     if "creds" in session:
#         logging.debug(f"{log_prefix} Creds found in session. Skipping authentication checks.")
#         return

#     logging.debug(f"{log_prefix} Checking authentication for endpoint: {request.endpoint}")

#     # Exclude some endpoints from the authentication check
#     excluded_endpoints = ['login', 'callback', 'static']

#     if request.endpoint and request.endpoint not in excluded_endpoints:
#         logging.warning(f"{log_prefix} No creds found in session. Redirecting to login. Session State: {session}")
#         return redirect(url_for('login'))

# @server.route('/delta-optimizer/login')
# def login():
#     log_prefix = "[Login Route]"
    
#     # If creds are found in session, redirect to the main app page
#     if "creds" in session:
#         logging.info(f"{log_prefix} User is already authenticated. Redirecting to main app page. Session State: {session}")
#         return redirect('/delta-optimizer/build-strategy')
    
#     # If no creds are in session and user is at the login route, present them with an authentication link
#     consent = oauth_client.initiate_consent()
#     session["consent"] = consent.as_dict()

#     # Instead of redirecting, return a button or link for the user to click
#     auth_link = f'<a href="{consent.auth_url}">Click here to authenticate</a>'
#     return auth_link


# @server.route("/delta-optimizer/callback")
# def callback():
#     log_prefix = "[Callback Route]"
    
#     logging.debug(f"{log_prefix} Callback accessed with arguments: {request.args}")
    
#     try:
#         if "consent" in session:
#             logging.debug(f"{log_prefix} Consent found in session. Session State: {session}")
#             consent = Consent.from_dict(oauth_client, session["consent"])
#             session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
#             logging.debug(f"{log_prefix} Credentials successfully obtained and stored in session. Session State: {session}")
#         else:
#             logging.warning(f"{log_prefix} No consent found in session during callback. Session State: {session}")
#     except Exception as e:
#         logging.error(f"{log_prefix} Error processing callback: {e}")
    
#     logging.debug(f"{log_prefix} Redirecting to the main content page.")
#     return redirect('/delta-optimizer/build-strategy')

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
        
        TOP_NAVBAR,
        LEFT_SIDEBAR,
        dmc.Container(
        className="background-container"
    ),
    dmc.Container(
        className="page",
        children=[
            dash.page_container,
            # html.Div(id='auth-action')  # Placeholder for the authentication link
        ]
    )

    ],
)


if __name__ == "__main__":
    app.run(debug=True, port=8050)
