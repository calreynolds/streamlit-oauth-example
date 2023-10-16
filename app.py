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
SECRET_KEY = os.environ.get("SECRET_KEY")



APP_NAME = "delta_optimizer_latest_one"


app = dash.Dash(__name__, use_pages=True, suppress_callback_exceptions=True, routes_pathname_prefix="/delta-optimizer/")
server = app.server
server.secret_key = SECRET_KEY

print(server.config)


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

from flask import Flask, session, redirect, request, jsonify
import logging


# Assuming you've already initialized your server and oauth_client elsewhere in your code.

# 1. Improve Session Management
server.config['SESSION_PERMANENT'] = True
# Optionally set a specific lifetime: server.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)

# Session Cookie Secure Flag (only if using HTTPS)
server.config['SESSION_COOKIE_SECURE'] = False

server.config['SESSION_COOKIE_SAMESITE'] = 'None'


# 1. Enhanced Logging Configuration
logging.basicConfig(level=logging.DEBUG,
                   format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
                   handlers=[logging.StreamHandler(), logging.FileHandler("app_debug.log")])

# 2. More Debugging Information in Session Management
@server.after_request
def log_session_cookie(response):
    logging.debug(f"Set-Cookie Header: {response.headers.get('Set-Cookie')}")
    return response

# 3. Enhanced Logging in Authentication Check
@server.before_request
def check_authentication():
    if "creds" not in session:
        if request.endpoint in ["login", "callback"]:
            pass  # Allow unauthenticated access to these routes.
        else:
            logging.debug(f"Unauthenticated access attempt to endpoint: {request.endpoint}")
            logging.debug(f"Headers: {request.headers}")
            logging.debug(f"Remote Address: {request.remote_addr}")
            logging.debug(f"Session data: {session}")
            return redirect("/delta-optimizer/login")

# 3. Separate login route to initiate the OAuth process
@server.route('/delta-optimizer/login')
def login():
    consent = oauth_client.initiate_consent()
    logging.debug(f"Initiated consent: {consent.as_dict()}")
    session["consent"] = consent.as_dict()
    return redirect(consent.auth_url)

# 4. Your callback remains the same with enhanced handling and logging
@server.route("/delta-optimizer/callback")
def callback():
    logging.debug(f"Callback accessed. Full request: {request}")
    
    try:
        if "consent" in session:
            logging.debug("Consent found in session.")
            consent = Consent.from_dict(oauth_client, session["consent"])
            session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
        else:
            logging.warning("No consent found in session during callback.")
            logging.debug(f"Session data: {session}")
    except Exception as e:
        logging.error(f"Error processing callback: {e}")
        # Clearing the session in case of errors
        session.clear()
        # session.pop("consent", None)

        return "Error processing authentication. Please try again later.", 500
    
    logging.debug("Redirecting to the default delta-optimizer page.")
    return redirect('/delta-optimizer')  # Redirect to the main app page

# 5. Periodic Session Debugging
@server.route('/debug/session')
def debug_session():
    return jsonify(session)

# 6. Error Handling
@server.errorhandler(Exception)
def handle_unexpected_error(error):
    logging.exception("An unexpected error occurred.")
    return "An unexpected error occurred.", 500

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
