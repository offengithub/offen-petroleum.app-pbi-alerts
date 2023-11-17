"""
Template Component main class.

"""
import json
import os 
import logging
import requests
import xml.etree.ElementTree as ET
from keboola.component.exceptions import UserException
from keboola.component.base import ComponentBase
import json 
from datetime import date,datetime, timezone, timedelta
import datetime
import snowflake.connector
import pandas as pd
import requests
import json



# configuration variables
KEY_SNOWFLAKE_ACCOUNT = 'account'
KEY_SNOWFLAKE_USER = 'username'
KEY_SNOWFLAKE_PASSWORD = 'password'
KEY_SNOWFLAKE_WAREHOUSE= 'warehouse'
KEY_SNOWFLAKE_DATABASE = 'database'
KEY_SNOWFLAKE_SCHEMA = 'schema'
KEY_TABLE_NAME='table_name'
KEY_WEBHOOK_URL='webhook_url'


# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_SNOWFLAKE_ACCOUNT,KEY_SNOWFLAKE_DATABASE,KEY_SNOWFLAKE_WAREHOUSE,KEY_WEBHOOK_URL,KEY_SNOWFLAKE_SCHEMA,
                       KEY_SNOWFLAKE_PASSWORD,KEY_SNOWFLAKE_USER, KEY_TABLE_NAME]
REQUIRED_IMAGE_PARS = []




logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)


def post_to_teams(webhook_url, message):
    """
    Post a message to Microsoft Teams.
    :param webhook_url: The webhook URL provided by Microsoft Teams.
    :param message: The message to be posted.
    """
    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        'text': message
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        raise ValueError(f"Request to Teams returned an error {response.status_code}, the response is:\n{response.text}")


def query_snowflake(account,user, password,warehouse,database, schema, table):
    conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
    )
    # Query to check for records from yesterday
    today = datetime.datetime.now().date()
    query = f"""
    SELECT *
    FROM {table}
    WHERE alert_date = '{today}'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df



class Component(ComponentBase):

    def __init__(self):
        super().__init__()

    def run(self) -> None:

        """Runs the component.
        Validates the configuration parameters and triggers a Boomi job.
        """

       # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        account = self.configuration.parameters.get(KEY_SNOWFLAKE_ACCOUNT)
        username = self.configuration.parameters.get(KEY_SNOWFLAKE_USER)
        password = self.configuration.parameters.get(KEY_SNOWFLAKE_PASSWORD)
        database = self.configuration.parameters.get(KEY_SNOWFLAKE_DATABASE)
        schema = self.configuration.parameters.get(KEY_SNOWFLAKE_SCHEMA)
        webhook_url=self.configuration.parameters.get(KEY_WEBHOOK_URL)
        table_name=self.configuration.parameters.get(KEY_TABLE_NAME)
        warehouse=self.configuration.parameters.get(KEY_SNOWFLAKE_WAREHOUSE)

        # Check if there are records and send an email
        try:
            df = query_snowflake(account, username, password, warehouse, database, schema, table_name)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
        else:
            logging.info("Successfully connected to the Snowflake database and executed the query.")
        finally:
            logging.info("Finished attempting to connect and query the Snowflake database.")

        today=date.today()

        if not df.empty:
            for index, row in df.iterrows():
                # Getting date and measure to send to notification channel
                alert_type = row['MEASURE']
                message_body = f"This is to notify you that, {alert_type} occured today {today}"
                logging.info(message_body)
                post_to_teams(webhook_url, message_body)
         


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        #logging.info("Component started")
        comp = Component()
    
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception("User configuration error: %s", exc)
        exit(1)
    except Exception as exc:
        logging.exception("Unexpected error: %s", exc)
        exit(2)
