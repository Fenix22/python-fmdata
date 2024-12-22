from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Dict

from fmdata.fmclient import FMClient, SessionProvider, DataSourceProvider
from fmdata.results import LoginResult


def get_token_or_raise_exception(result: LoginResult) -> str:
    result.raise_exception_if_has_error()

    return result.response.token


@dataclass
class UsernamePasswordDataSourceProvider(DataSourceProvider):
    database: str
    username: str
    password: str

    def provide(self, **kwargs) -> Dict:
        return {
            "database": self.database,
            "username": self.username,
            "password": self.password
        }


@dataclass
class OAuthDataSourceProvider(DataSourceProvider):
    database: str
    oauth_request_id: str
    oauth_identifier: str

    def provide(self, **kwargs) -> Dict:
        return {
            "database": self.database,
            "oAuthRequestId": self.oauth_request_id,
            "oAuthIdentifier": self.oauth_identifier
        }


@dataclass
class UsernamePasswordSessionProvider(SessionProvider):
    username: str
    password: str
    data_sources: Optional[List[DataSourceProvider]] = None

    def login(self, fm_client: FMClient, **kwargs) -> str:
        result: LoginResult = fm_client.raw_login_username_password(
            username=self.username,
            password=self.password,
            data_sources=self.data_sources
        )

        return get_token_or_raise_exception(result)


@dataclass
class OAuthSessionProvider(SessionProvider):
    oauth_request_id: str
    oauth_identifier: str
    data_sources: Optional[List[DataSourceProvider]] = None

    def login(self, fm_client: FMClient, **kwargs) -> str:
        result: LoginResult = fm_client.raw_login_oauth(
            oauth_request_id=self.oauth_request_id,
            oauth_identifier=self.oauth_identifier,
            data_sources=self.data_sources
        )

        return get_token_or_raise_exception(result)


@dataclass
class ClarisCloudSessionProvider(SessionProvider):
    cognito_userpool_id: str = 'us-west-2_NqkuZcXQY'
    cognito_client_id: str = '4l9rvl4mv5es1eep1qe97cautn'
    claris_id_name: str = None
    clarid_id_password: str = None
    data_sources: Optional[List[DataSourceProvider]] = None

    try:
        import pycognito
        _has_pycognito = True
    except ImportError:
        _has_pycognito = False

    if not _has_pycognito:
        raise ImportError(
            'Please install pycognito for Claris Cloud support. '
        )

    def _get_cognito_token(self) -> str:
        """Use Pycognito library to authenticate with Amazon Cognito and retrieve FMID token."""

        import pycognito
        user = pycognito.Cognito(user_pool_id=self.cognito_userpool_id,
                                 client_id=self.cognito_client_id,
                                 username=self.claris_id_name)

        user.authenticate(self.clarid_id_password)
        return user.id_token

    def login(self, fm_client: FMClient, **kwargs) -> Optional[str]:
        fmid_token = self._get_cognito_token()
        result: LoginResult = fm_client.raw_login_claris_cloud(
            fmid_token=fmid_token,
            data_sources=self.data_sources
        )

        return get_token_or_raise_exception(result)
