import boto3
import time


class CognitoAuth:
    def __init__(self, client_id, user_pool_id, region):
        self.client_id = client_id
        self.user_pool_id = user_pool_id
        self.client = boto3.client("cognito-idp", region_name=region)
        self.tokens = None

    def login(self, username, password):
        """Login with username/password and store tokens"""
        response = self.client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": username,
                "PASSWORD": password,
            },
            ClientId=self.client_id,
        )

        self.tokens = {
            "id_token": response["AuthenticationResult"]["IdToken"],
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "refresh_token": response["AuthenticationResult"]["RefreshToken"],
            "expires_in": response["AuthenticationResult"]["ExpiresIn"],  # seconds
            "issued_at": int(time.time())
        }
        return self.tokens

        client = boto3.client("cognito-idp", region_name="us-east-2")

        # Start authentication
        response = client.initiate_auth(
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters={
                "USERNAME": username,
                "PASSWORD": password
            },
            ClientId=CLIENT_ID,
        )

        tokens = {
            "id_token": response["AuthenticationResult"]["IdToken"],
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "refresh_token": response["AuthenticationResult"]["RefreshToken"],
        }




    def is_access_token_valid(self):
        """Check if the access token is still valid"""
        if not self.tokens:
            return False
        exp_time = self.tokens["issued_at"] + self.tokens["expires_in"]
        return int(time.time()) < exp_time

    def refresh_tokens(self):
        """Use the refresh token to get new tokens"""
        if not self.tokens or "refresh_token" not in self.tokens:
            raise Exception("No refresh token available")

        response = self.client.initiate_auth(
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={
                "REFRESH_TOKEN": self.tokens["refresh_token"]
            },
            ClientId=self.client_id,
        )

        # Cognito does not always return a new refresh_token
        self.tokens.update({
            "id_token": response["AuthenticationResult"]["IdToken"],
            "access_token": response["AuthenticationResult"]["AccessToken"],
            "expires_in": response["AuthenticationResult"]["ExpiresIn"],
            "issued_at": int(time.time())
        })
        return self.tokens

    def get_access_token(self):
        """Return a valid access token, refreshing if needed"""
        if not self.is_access_token_valid():
            print("🔄 Access token expired — refreshing...")
            self.refresh_tokens()
        return self.tokens["access_token"]


    def get_id_token(self):
        """Return a valid access token, refreshing if needed"""
        if not self.is_access_token_valid():
            print("🔄 Access token expired — refreshing...")
            self.refresh_tokens()
        return self.tokens["id_token"]

