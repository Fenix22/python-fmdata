from __future__ import annotations

import json
import logging
import threading
import time
from functools import wraps
from typing import List, Dict, Optional, Any, IO, Union

import requests

from statelessfm.fmclient.const import FMErrorEnum, APIPath
from statelessfm.fmclient.inputs import ScriptsInput, OptionsInput, _scripts_to_dict, \
    _portals_to_params, _sort_to_params, _date_formats_to_value, PortalsInput, \
    SortInput, QueryInput, DateFormats
from statelessfm.fmclient.results import \
    FileMakerErrorException, LogoutResult, CreateRecordResult, EditRecordResult, DeleteRecordResult, \
    GetRecordResult, ScriptResult, BaseResult, Message, LoginResult, UploadContainerResult, GetRecordsResult, \
    FindResult, SetGlobalResult, GetProductInfoResult, GetDatabasesResult, GetLayoutsResult, GetLayoutResult, \
    GetScriptsResult
from statelessfm.fmclient.utils import clean_none


class LoginRetriedTooFastException(Exception):

    def __init__(self, msg) -> None:
        super().__init__(msg)

class SessionProvider:

    def login(self, fm_client: FMClient, **kwargs) -> str:
        raise NotImplementedError

class DataSourceProvider:
    def provide(self, **kwargs) -> Dict:
        pass

def fm_data_source_from_providers(providers: List[DataSourceProvider]) -> Optional[List[Dict]]:
    if providers is None:
        return None

    return [provider.provide() for provider in providers]

def _auto_manage_session(f):
    @wraps(f)
    def wrapper(self: FMClient, *args, **kwargs):
        if not self.auto_manage_session:
            if self._session_invalid:
                raise Exception("Session is invalid. Please call login first.")
            return f(self, *args, **kwargs)

        invalid_token_error: Optional[Message] = None

        for _ in range(2):
            self.safe_login_if_not()

            result: BaseResult = f(self, *args, **kwargs)
            invalid_token_error = next(result.get_errors(include_codes=[FMErrorEnum.INVALID_FILEMAKER_DATA_API_TOKEN]), None)

            # If not invalid token error, return result immediately
            if not invalid_token_error:
                return result

            # If invalid token, invalidate session and try once more
            self._session_invalid = True

        # If we reached here, login attempts failed twice
        raise FileMakerErrorException.from_response_message(invalid_token_error)

    return wrapper


class FMClient(object):
    def __init__(self,
                 url: str,
                 database: str,
                 login_provider: SessionProvider,
                 api_version: str = "v1",
                 connection_timeout: float = 10,
                 read_timeout: float = 30,
                 too_fast_login_retry_timeout: Optional[float] = 1,
                 http_client_extra_params: Dict = None,
                 verify_ssl: Union[bool, str] = True,
                 auto_manage_session: bool = True) -> None:

        self.url: str = url
        self.database: str = database
        self.login_provider: SessionProvider = login_provider
        self.api_version: str = api_version
        self.connection_timeout: float = connection_timeout
        self.read_timeout: float = read_timeout
        self.too_fast_login_retry_timeout: Optional[float] = too_fast_login_retry_timeout
        self.http_client_extra_params: Dict = http_client_extra_params or {}
        self.verify_ssl: Union[bool, str] = verify_ssl
        self.auto_manage_session: bool = auto_manage_session

        self._token: Optional[str] = None
        self._session_invalid: bool = True
        self._session_last_login_retry: Optional[float] = None
        self._session_lock = threading.RLock()

    def on_new_session(self, **kwargs):
        pass

    def login(self) -> None:
        """
        Attempts to create a new session token using the configured login_provider.
        Raises an exception if login fails.
        """
        if not self.login_provider:
            raise ValueError("LoginProvider is not set.")

        logging.debug("Logging in to FileMaker Data API")

        try:
            self._token = self.login_provider.login(fm_client=self)
            self._session_invalid = False
            self.on_new_session()

        except Exception:
            self._token = None
            self._session_invalid = True

            raise
        finally:
            self._session_last_login_retry = time.time()

    def safe_login_if_not(self, exception_if_too_fast: bool = True) -> None:
        """
        Thread-safe login method that only logs in if no active session is present.
        Raises LoginRetriedTooFastException if a login attempt was too recent.
        """
        if self._session_invalid:
            with self._session_lock:
                if self._session_invalid:
                    if exception_if_too_fast:
                        self._raise_exception_if_too_fast()
                    self.login()


    def logout(self, api_version: Optional[str] = None) -> Optional[LogoutResult]:
        """
        Explicitly logs out of the current session.
        """
        if self._session_invalid:
            return None

        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            token=self._token
        )

        return LogoutResult(original_response=self.call_filemaker(method='DELETE', path=path, use_session_token=False))

    @_auto_manage_session
    def create_record(self,
                      layout: str,
                      field_data: Dict[str, Any],
                      portal_data: Optional[Dict[str, Any]] = None,
                      scripts: Optional[ScriptsInput] = None,
                      options: Optional[OptionsInput] = None,
                      date_formats: Optional[DateFormats] = None,
                      api_version: Optional[str] = None
                      ) -> CreateRecordResult:

        path = APIPath.RECORDS.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
        )

        request_data = clean_none({
            'fieldData': field_data,
            'portalData': portal_data,
            'options': options,
            'date_formats': _date_formats_to_value(date_formats),
            **_scripts_to_dict(scripts),
        })

        return CreateRecordResult(self.call_filemaker(method='POST', path=path, data=request_data))

    @_auto_manage_session
    def edit_record(self,
                    layout: str,
                    record_id: str,
                    field_data: Dict[str, Any],
                    mod_id: Optional[str] = None,
                    portals: Optional[Dict[str, Any]] = None,
                    scripts: Optional[ScriptsInput] = None,
                    options: Optional[OptionsInput] = None,
                    date_formats: Optional[DateFormats] = None,
                    api_version: Optional[str] = None
                    ) -> EditRecordResult:
        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        request_data = clean_none({
            'fieldData': field_data,
            'modId': mod_id,
            'portalData': portals,
            'options': options,
            'date_formats': _date_formats_to_value(date_formats),
            **_scripts_to_dict(scripts),
        })

        return EditRecordResult(self.call_filemaker(method='PATCH', data=request_data, path=path))

    @_auto_manage_session
    def delete_record(self,
                      layout: str,
                      record_id: int,
                      scripts: Optional[ScriptsInput] = None,
                      api_version: Optional[str] = None
                      ) -> DeleteRecordResult:

        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        params = clean_none({
            **_scripts_to_dict(scripts),
        })

        return DeleteRecordResult(self.call_filemaker(method='DELETE', params=params, path=path))

    @_auto_manage_session
    def get_record(self,
                   layout: str,
                   record_id: int,
                   response_layout: Optional[str] = None,
                   portals: Optional[PortalsInput] = None,
                   scripts: Optional[ScriptsInput] = None,
                   api_version: Optional[str] = None
                   ) -> GetRecordResult:

        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        params = clean_none({
            "layout.response": response_layout,
            **_portals_to_params(portals, names_as_string=True),
            **_scripts_to_dict(scripts),
        })

        return GetRecordResult(self.call_filemaker(method='GET', path=path, params=params))

    @_auto_manage_session
    def perform_script(self,
                       layout: str,
                       name: str,
                       param: Optional[str] = None,
                       api_version: Optional[str] = None
                       ) -> ScriptResult:

        path = APIPath.SCRIPT.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
            script_name=name
        )

        return ScriptResult(self.call_filemaker(method='GET', path=path, params={'script.param': param}))

    @_auto_manage_session
    def upload_container(self,
                         layout: str,
                         record_id: int,
                         field_name: str,
                         file: IO,
                         field_repetition: int = 1,
                         api_version: Optional[str] = None
                         ) -> UploadContainerResult:

        path = APIPath.UPLOAD_CONTAINER.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout,
            record_id=record_id,
            field_name=field_name,
            field_repetition=field_repetition
        )

        # Let requests handle multipart/form-data
        return UploadContainerResult(self.call_filemaker('POST', path, files={'upload': file}, content_type=None))

    @_auto_manage_session
    def get_records(self,
                    layout: str,
                    offset: int = 1,
                    limit: int = 100,
                    response_layout: Optional[str] = None,
                    sort: Optional[SortInput] = None,
                    portals: Optional[PortalsInput] = None,
                    scripts: Optional[ScriptsInput] = None,
                    date_formats: Optional[DateFormats] = None,
                    api_version: Optional[str] = None
                    ) -> GetRecordsResult:

        path = APIPath.RECORDS.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout
        )

        params = clean_none({
            '_offset': offset,
            '_limit': limit,
            'layout.response': response_layout,
            'date_formats': _date_formats_to_value(date_formats),
            '_sort': _sort_to_params(sort),
            **_portals_to_params(portals, names_as_string=True),
            **_scripts_to_dict(scripts),
        })

        return GetRecordsResult(self.call_filemaker(method='GET', path=path, params=params))

    @_auto_manage_session
    def find(self,
             layout: str,
             query: QueryInput,
             sort: Optional[SortInput] = None,
             offset: int = 1,
             limit: int = 100,
             portals: Optional[PortalsInput] = None,
             scripts: Optional[ScriptsInput] = None,
             date_formats: Optional[DateFormats] = None,
             response_layout: Optional[str] = None,
             api_version: Optional[str] = None
             ) -> FindResult:

        path = APIPath.FIND.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout
        )

        data = clean_none({
            'query': query,
            'sort': _sort_to_params(sort),
            'offset': str(offset),
            'limit': str(limit),
            'layout.response': response_layout,
            'date_formats': _date_formats_to_value(date_formats),
            **_portals_to_params(portals, names_as_string=False),
            **_scripts_to_dict(scripts),
        })

        return FindResult(self.call_filemaker(method='POST', path=path, data=data))

    @_auto_manage_session
    def set_globals(self, global_fields: Dict[str, Any], api_version: Optional[str] = None) -> SetGlobalResult:
        path = APIPath.GLOBALS.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database
        )

        data = {'globalFields': global_fields}
        return SetGlobalResult(self.call_filemaker(method='PATCH', path=path, data=data))

    def get_product_info(self, api_version: Optional[str] = None) -> GetProductInfoResult:
        path = APIPath.META_PRODUCT.value.format(
            api_version=self._get_api_version(api_version)
        )

        return GetProductInfoResult(self.call_filemaker(method='GET', path=path, use_session_token=False))

    def get_databases(self,
                      username: Optional[str] = None,
                      password: Optional[str] = None,
                      api_version: Optional[str] = None) -> GetDatabasesResult:
        path = APIPath.META_DATABASES.value.format(
            api_version=self._get_api_version(api_version)
        )

        auth = (username, password) if (username and password) else None
        return GetDatabasesResult(self.call_filemaker(method='GET', path=path, auth=auth, use_session_token=False))

    @_auto_manage_session
    def get_layouts(self, api_version: Optional[str] = None) -> GetLayoutsResult:
        path = APIPath.META_LAYOUTS.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database
        )

        return GetLayoutsResult(self.call_filemaker(method='GET', path=path))

    @_auto_manage_session
    def get_layout(self, layout: Optional[str] = None,
                   api_version: Optional[str] = None) -> GetLayoutResult:

        path = APIPath.META_LAYOUT.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            layout=layout
        )

        return GetLayoutResult(self.call_filemaker(method='GET', path=path))

    @_auto_manage_session
    def get_scripts(self, api_version: Optional[str] = None) -> GetScriptsResult:
        path = APIPath.META_SCRIPTS.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database
        )

        return GetScriptsResult(self.call_filemaker(method='GET', path=path))


    def raw_login_username_password(self, username: str,
                                    password: str,
                                    data_sources: Optional[List[DataSourceProvider]] = None,
                                    api_version: Optional[str] = None,
                                    **kwargs
                                    ) -> LoginResult:

        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            token=''
        )

        data = clean_none({
            'fmDataSource': fm_data_source_from_providers(data_sources)
        })

        return LoginResult(self.call_filemaker(method='POST', path=path, data=data, auth=(username, password)))

    def raw_login_oauth(self, oauth_request_id: str,
                        oauth_identifier: str,
                        data_sources: Optional[List[DataSourceProvider]],
                        api_version: Optional[str] = None,
                        **kwargs
                        ) -> LoginResult:
        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            token=''
        )

        data = clean_none({
            'fmDataSource': fm_data_source_from_providers(data_sources)
        })

        headers = {
            'X-FM-Data-OAuth-Request-Id': oauth_request_id,
            'X-FM-Data-OAuth-Identifier': oauth_identifier
        }

        return LoginResult(self.call_filemaker(method='POST', path=path, data=data, headers=headers))

    def raw_login_claris_cloud(self, fmid_token: str,
                               data_sources: Optional[List[DataSourceProvider]],
                               api_version: Optional[str] = None,
                               **kwargs
                               ) -> LoginResult:
        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._get_api_version(api_version),
            database=self.database,
            token=''
        )

        data = clean_none({
            'fmDataSource': fm_data_source_from_providers(data_sources)
        })

        headers = {
            'Authorization': f'FMID {fmid_token}'
        }

        return LoginResult(self.call_filemaker(method='POST', path=path, data=data, headers=headers, use_session_token=False))

    def _get_api_version(self, api_version: Optional[str] = None) -> str:
        return api_version if api_version else self.api_version

    def _raise_exception_if_too_fast(self):
        if self.too_fast_login_retry_timeout is None or self._session_last_login_retry is None:
            return

        elapsed = time.time() - self._session_last_login_retry

        if elapsed <= self.too_fast_login_retry_timeout:
            raise LoginRetriedTooFastException(
                f"Last failed login retry was {elapsed * 1000:.0f}ms ago, "
                f"retry timeout is {self.too_fast_login_retry_timeout * 1000:.0f}ms."
            )

    def request(self, *args, **kwargs) -> requests.Response:
        return requests.request(*args, timeout=(self.connection_timeout, self.read_timeout), **kwargs)

    def call_filemaker(self, method: str,
                       path: str,
                       headers: Optional[Dict] = None,
                       data: Optional[Dict] = None,
                       params: Optional[Dict] = None,
                       use_session_token: bool = True,
                       content_type: Optional[str] = 'application/json',
                       **kwargs: Any) -> Dict:

        url = self.url + path
        request_data = json.dumps(data) if data else None

        request_headers = headers if headers else {}
        if content_type:
            request_headers['Content-Type'] = content_type

        if use_session_token:
            request_headers['Authorization'] = f'Bearer {self._token}'

        response = self.request(
            method=method,
            headers=request_headers,
            url=url,
            data=request_data,
            verify=self.verify_ssl,
            params=params,
            **self.http_client_extra_params,
            **kwargs
        )

        return response.json()

    def __repr__(self) -> str:
        return f"<FMClient logged_in={bool(not self._session_invalid)} token={self._token} database={self.database}>"
