""" Dash with OAuth:

This application provides end-to-end demonstration of Databricks SDK for Python
capabilities of OAuth Authorization Code flow with PKCE security enabled. This
can help you build a hosted app with every user using their own identity to
access Databricks resources.

If you have already Custom App:

./dash_app_with_oauth.py <databricks workspace url> \
    --client_id <app-client-id> \
    --client_secret <app-secret> \
    --port 5001

If you want this script to register Custom App and redirect URL for you:

./dash_app_with_oauth.py <databricks workspace url>

You'll get prompted for Databricks Account username and password for
script to enroll your account into OAuth and create a custom app with
http://localhost:5003/callback as the redirect callback. Client and
secret credentials for this OAuth app will be printed to the console,
so that you could resume testing this app at a later stage.

Once started, please open http://localhost:5003 in your browser and
go through SSO flow to get a list of clusters on <databricks workspace url>.
"""

# Import required Dash modules
import dash
import dash_html_components as html
import argparse
import logging
import sys
from databricks.sdk.oauth import OAuthClient


APP_NAME = "dash_app_with_oauth"

def create_dash_app(oauth_client: OAuthClient, port: int):
    import secrets
    from flask import request, session, redirect, url_for

    # Create Dash app with Flask server
    app = dash.Dash(__name__)
    server = app.server
    server.secret_key = secrets.token_urlsafe(32)

    # Define the Dash layout
    app.layout = html.Div([
        html.Div(id='page-content'),
        html.Div(id='redirect-url', style={'display': 'none'})
    ])

    @server.route("/callback")
    def callback():
        from databricks.sdk.oauth import Consent
        consent = Consent.from_dict(oauth_client, session["consent"])
        session["creds"] = consent.exchange_callback_parameters(request.args).as_dict()
        return redirect('/index')

    @server.route("/index")
    def dash_index():
        if "creds" not in session:
            consent = oauth_client.initiate_consent()
            session["consent"] = consent.as_dict()
            return redirect(consent.auth_url)

        # For Dash, instead of directly returning a layout from Flask's view function,
        # set the layout in the Dash app
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.oauth import SessionCredentials

        credentials_provider = SessionCredentials.from_dict(oauth_client, session["creds"])
        workspace_client = WorkspaceClient(
            host=oauth_client.host,
            product=APP_NAME,
            credentials_provider=credentials_provider
        )

        clusters = workspace_client.clusters.list()
        cluster_list = [
            html.Li(
                html.A(cluster.cluster_name, 
                       href=f"{workspace_client.config.host}/#setting/clusters/{cluster.cluster_id}/configuration", 
                       target="_blank")
            ) 
            for cluster in clusters
        ]

        app.layout = html.Div([
            html.H1("Databricks Clusters"), 
            html.Ul(cluster_list)
        ])
        return redirect('/')  # Redirect to the root URL of Dash to display the updated layout
 

    return app

# Function to register custom app
def register_custom_app(
    oauth_client: OAuthClient, args: argparse.Namespace
) -> tuple[str, str]:
    if not oauth_client.is_aws:
        logging.error("Not supported for other clouds than AWS")
        sys.exit(2)

    logging.info("No OAuth custom app client/secret provided, creating new app")

    import getpass
    from databricks.sdk import AccountClient

    account_client = AccountClient(
        host="https://accounts.cloud.databricks.com",
        account_id=input("Databricks Account ID: "),
        username=input("Username: "),
        password=getpass.getpass("Password: "),
    )

    logging.info("Enrolling all published apps...")
    account_client.o_auth_enrollment.create(enable_all_published_apps=True)

    status = account_client.o_auth_enrollment.get()
    logging.info(f"Enrolled all published apps: {status}")

    custom_app = account_client.custom_app_integration.create(
        name=APP_NAME,
        redirect_urls=[f"http://localhost:{args.port}/callback"],
        confidential=True,
        scopes=["all-apis"],
    )
    logging.info(
        f"Created new custom app: "
        f"--client_id {custom_app.client_id} "
        f"--client_secret {custom_app.client_secret}"
    )

    return custom_app.client_id, custom_app.client_secret


def init_oauth_config(args) -> OAuthClient:
    """Creates Databricks SDK configuration for OAuth"""
    oauth_client = OAuthClient(
        host=args.host,
        client_id=args.client_id,
        client_secret=args.client_secret,
        redirect_url=f"http://localhost:{args.port}/callback",
        scopes=["all-apis"],
    )
    if not oauth_client.client_id:
        client_id, client_secret = register_custom_app(oauth_client, args)
        oauth_client.client_id = client_id
        oauth_client.client_secret = client_secret

    return oauth_client


def parse_arguments() -> argparse.Namespace:
    """Parses arguments for this demo"""
    parser = argparse.ArgumentParser(prog=APP_NAME, description=__doc__.strip())
    parser.add_argument("host")
    for flag in ["client_id", "client_secret"]:
        parser.add_argument(f"--{flag}")
    parser.add_argument("--port", default=5003, type=int)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    oauth_cfg = init_oauth_config(args)
    app_dash = create_dash_app(
        oauth_cfg, args.port
    )  # This modifies the global Dash app

    app_dash.run_server(host="localhost", port=args.port, debug=True)
