import boto3
import json
import requests
import traceback
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TokenManager:
    """Class to handle token management for Salesforce"""

    def __init__(self, session_object):
        self.dynamodb = boto3.resource("dynamodb")
        self.token_table_name = session_object['token_table_name']
        self.token_table = self.dynamodb.Table(
            session_object['token_table_name'])
        self.url = session_object['host'] + session_object['auth_path']
        self.diff_time = int(session_object['diff_time'])
        self.contact_center_username = session_object['contact_center_username']
        self.region_name = session_object['region_name']
        self.secret_name = session_object['secret_name']
        # self.db_manager = DBManager(session_object)
        # self.secret_manager = SecretManager(self.secret_name, self.region_name)
        self.service_name = 'secretsmanager'

    def get_secret(self):
        ''' 
        Function to fetch seccrets from secret manager.

        Returns:
            string: value of the secret
        '''
        try:

            # Create a Secrets Manager client
            session = boto3.Session()
            client = session.client(
                service_name=self.service_name,
                region_name=self.region_name
            )

            get_secret_value_response = client.get_secret_value(
                SecretId=self.secret_name
            )

            secret = get_secret_value_response['SecretString']
            return secret
        except Exception as e:
            logger.error(f'Error getting secret: {traceback.format_exc()}')
            raise e

    def fetch_token(self):
        """Function to fetch token from DynamoDB

        Returns:
            JSON: fetched token and issued timestamp
        """
        response = self.token_table.get_item(
            Key={
                'username': self.contact_center_username
            }
        )
        token = ""
        issued_at = ""
        if "Item" in response and "token" in response["Item"]:
            logger.info("Token exists, checking expiration")
            token = response["Item"]["token"]
            issued_at = int(response["Item"]["issued_at"])/1000

        return {
            "token": token,
            "issued_at": issued_at
        }

    def update_token(self, token, issued_at):
        """Function to update DynamoDB with given token

        Args:
            token (string): New token to be updated 
            issued_at (string): timestamp of the new token was issued

        Returns:
            string: updated token in DynamoDB
        """
        self.token_table.put_item(
            Item={
                "username": self.contact_center_username,
                "token": token,
                "issued_at": issued_at
            }
        )
        return token

    def update_sf_token(self):
        """Function to update the newly generated token in db

        Returns:
            string: newly updated token
        """
        try:
            salesforce_response = self.get_token()

            token = salesforce_response["access_token"]
            issued_at = salesforce_response["issued_at"]

            self.update_token(token, issued_at)
            logger.info("[update_token] updated token in db")
            return token
        except Exception:
            logger.error("Error: {}".format(traceback.format_exc()))

    def get_token(self):
        """Function to retrieve token from Salesforce

        Returns:
            string: newly generated token
        """
        try:
            payload = json.loads(self.get_secret())
            files = []
            response = requests.request(
                "POST", self.url, data=payload, files=files)

            token = json.loads(response.text)

            return token

        except Exception:
            logger.error(f"[get_token] response: {traceback.format_exc()}")

    def get_access_token(self):
        '''
        Function to fetch access token .

        Returns:
            string: access token
        '''
        try:
            response = self.fetch_token()

            token = response['token']
            issued_at = response['issued_at']

            if token != "" and issued_at != "":
                current_timestamp = int(datetime.now(timezone.utc).timestamp())

                if current_timestamp-issued_at > self.diff_time:
                    logger.info("Token expired, generating new token...")
                    token = self.update_sf_token()

                return token

            logger.info("No token found, generating new token...")
            token = self.update_sf_token()
            return token

        except Exception as e:
            logger.error("Error fetching access token from salesforce : %s" %
                         (traceback.format_exc()))
            raise e
