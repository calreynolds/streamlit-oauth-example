import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import (
    OAuthClient,
    SessionCredentials,
    Consent,
    retrieve_token,
)
import logging

from tornado.web import RequestHandler
from injectApi import (
    CustomRule,
    init_global_tornado_hook,
    uninitialize_global_tornado_hook,
)


# OAuth settings
OAUTH_HOST = "https://plotly-customer-success.cloud.databricks.com"  # Replace with your Databricks workspace URL
CLIENT_ID = "793dbcac-9b34-409a-a234-7ad1d4de71a7"
# CLIENT_SECRET = "dosef002c7610adc771eb8a0786903eb1fc7"
REDIRECT_URI = "http://localhost:8501/callback"

# Session(app)
oauth_client = OAuthClient(
    host=OAUTH_HOST,
    client_id=CLIENT_ID,
    # client_secret=CLIENT_SECRET,
    redirect_url=REDIRECT_URI,
    scopes=["offline_access", "all-apis"],
)

# 1. Enhanced Logging Configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app_debug.log")],
)


def streamlit_app():
    if "consent" not in st.session_state:
        logging.debug("streamlit_app: Initiating OAuth flow")
        consent = oauth_client.initiate_consent()
        st.session_state["consent"] = consent
        logging.debug(f"streamlit_app: Consent set in session state: {consent}")
    else:
        logging.debug("streamlit_app: Consent already in session state")
    st.write(
        f"Please click [here]({st.session_state['consent'].auth_url}) to authenticate."
    )


streamlit_app()


class CallbackHandler(RequestHandler):
    def get(self):
        logging.debug(f"CallbackHandler: Session State: {st.session_state}")
        if "consent" not in st.session_state:
            logging.warning("CallbackHandler: 'consent' not in session state")
            self.write("OAuth consent not found. Please initiate the OAuth flow.")
            return

        consent = Consent.from_dict(oauth_client, st.session_state["consent"])
        st.session_state["creds"] = consent.exchange_callback_parameters(
            st.session_state
        ).as_dict()
        self.write(st.session_state["creds"])


init_global_tornado_hook([CustomRule("/callback", CallbackHandler)])


# st.title("Streamlit App with Databricks OAuth PKCE Flow")

# if __name__ == "__main__":


# print(
#     retrieve_token(
#         "793dbcac-9b34-409a-a234-7ad1d4de71a7",
#         "dosef002c7610adc771eb8a0786903eb1fc7",
#         "https://plotly-customer-success.cloud.databricks.com/oidc/v1/token",
#         {},
#     )
# )
