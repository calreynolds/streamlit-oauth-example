#!env python3

import logging, sys

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(name)s][%(levelname)s] %(message)s",
)
logging.getLogger("databricks.sdk").setLevel(logging.DEBUG)

import getpass
from databricks.sdk import AccountClient

account_client = AccountClient(
    host="https://accounts.cloud.databricks.com",
    account_id="5f8e7e8c-28bb-44d1-8c24-74401ef4f45b",
    client_id="c3368a22-7015-484d-a68a-874264413f50",
    client_secret="dose3aa9ebd2008a4519971fcbdcf6b7d6c2",
    debug_headers=True,
)

custom_app = account_client.custom_app_integration.create(
    name="streamlit-app",
    redirect_urls=[f"http://localhost:8501/callback"],
    # confidential=True,
    scopes=["all-apis", "offline_access", "openid"],
)
print(
    f"Created new custom app: "
    f"--client_id {custom_app.client_id} "
    f"--client_secret {custom_app.client_secret}"
)
