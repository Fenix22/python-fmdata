from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from functools import wraps, cached_property
from typing import List, Dict, Optional, Any, IO, Iterator

import requests

from fmdata.cache_iterator import CacheIterator
from fmdata.const import FMErrorEnum, APIPath
from fmdata.inputs import ScriptsInput, OptionsInput, _scripts_to_dict, \
    _portals_to_params, _sort_to_params, _date_formats_to_value, PortalsInput, \
    SortInput, QueryInput, VerifySSL, DateInput
from fmdata.results import \
    FileMakerErrorException, LogoutResult, CreateRecordResult, EditRecordResult, DeleteRecordResult, \
    GetRecordResult, ScriptResult, BaseResult, Message, LoginResult, UploadContainerResult, GetRecordsResult, \
    FindResult, SetGlobalResult, GetProductInfoResult, GetDatabasesResult, GetLayoutsResult, GetLayoutResult, \
    GetScriptsResult, GetRecordsPaginatedResult, FindPaginatedResult, CommonSearchRecordsResult, Page, \
    DuplicateRecordResult
from fmdata.utils import clean_none


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
            invalid_token_error = next(
                result.get_messages_iterator(search_codes=[FMErrorEnum.INVALID_FILEMAKER_DATA_API_TOKEN]), None)

            # If not invalid token error, return result immediately
            if not invalid_token_error:
                return result

            # If invalid token, invalidate session and try once more
            self._session_invalid = True

        # If we reached here, login attempts failed twice
        raise FileMakerErrorException.from_response_message(invalid_token_error)

    return wrapper


class FMClient:
    def __init__(self,
                 url: str,
                 database: str,
                 login_provider: SessionProvider,
                 api_version: str = "v1",
                 connection_timeout: float = 10,
                 read_timeout: float = 30,
                 too_fast_login_retry_timeout: Optional[float] = 1,
                 http_client_extra_params: Dict = None,
                 verify_ssl: VerifySSL = True,
                 auto_manage_session: bool = True) -> None:

        self.url: str = url
        self.database: str = database
        self.login_provider: SessionProvider = login_provider
        self.api_version: str = api_version
        self.connection_timeout: float = connection_timeout
        self.read_timeout: float = read_timeout
        self.too_fast_login_retry_timeout: Optional[float] = too_fast_login_retry_timeout
        self.http_client_extra_params: Dict = http_client_extra_params or {}
        self.verify_ssl: VerifySSL = verify_ssl
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

    def logout(self, **kwargs) -> Optional[LogoutResult]:
        """
        Explicitly logs out of the current session.
        """
        if self._session_invalid:
            return None

        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            token=self._token
        )

        return LogoutResult(
            raw_content=self.call_filemaker(method='DELETE', path=path, use_session_token=False, **kwargs))

    @_auto_manage_session
    def create_record(self,
                      layout: str,
                      field_data: Dict[str, Any],
                      portal_data: Optional[Dict[str, Any]] = None,
                      scripts: Optional[ScriptsInput] = None,
                      options: Optional[OptionsInput] = None,
                      date_formats: Optional[DateInput] = None,
                      **kwargs
                      ) -> CreateRecordResult:

        path = APIPath.RECORDS.value.format(
            api_version=self._pop_api_version(kwargs),
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

        return CreateRecordResult(self.call_filemaker(method='POST', path=path, data=request_data, **kwargs))

    @_auto_manage_session
    def duplicate_record(self,
                         layout: str,
                         record_id: str,
                         scripts: Optional[ScriptsInput] = None,
                         **kwargs
                         ) -> DuplicateRecordResult:

        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        params = clean_none({
            **_scripts_to_dict(scripts),
        })

        return DuplicateRecordResult(self.call_filemaker(method='POST', params=params, path=path, **kwargs))

    @_auto_manage_session
    def edit_record(self,
                    layout: str,
                    record_id: str,
                    field_data: Dict[str, Any],
                    mod_id: Optional[str] = None,
                    portal_data: Optional[Dict[str, Any]] = None,
                    scripts: Optional[ScriptsInput] = None,
                    options: Optional[OptionsInput] = None,
                    date_formats: Optional[DateInput] = None,
                    **kwargs
                    ) -> EditRecordResult:
        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        request_data = clean_none({
            'fieldData': field_data,
            'modId': mod_id,
            'portalData': portal_data,
            'options': options,
            'date_formats': _date_formats_to_value(date_formats),
            **_scripts_to_dict(scripts),
        })

        return EditRecordResult(self.call_filemaker(method='PATCH', data=request_data, path=path, **kwargs))

    @_auto_manage_session
    def delete_record(self,
                      layout: str,
                      record_id: str,
                      scripts: Optional[ScriptsInput] = None,
                      **kwargs
                      ) -> DeleteRecordResult:

        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        params = clean_none({
            **_scripts_to_dict(scripts),
        })

        return DeleteRecordResult(self.call_filemaker(method='DELETE', params=params, path=path, **kwargs))

    @_auto_manage_session
    def get_record(self,
                   layout: str,
                   record_id: str,
                   response_layout: Optional[str] = None,
                   portals: Optional[PortalsInput] = None,
                   scripts: Optional[ScriptsInput] = None,
                   **kwargs
                   ) -> GetRecordResult:

        path = APIPath.RECORD_ACTION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            record_id=record_id
        )

        params = clean_none({
            "layout.response": response_layout,
            **_portals_to_params(portals, names_as_string=True),
            **_scripts_to_dict(scripts),
        })

        return GetRecordResult(raw_content=self.call_filemaker(method='GET', path=path, params=params, **kwargs),
                               client=self,
                               layout=layout)

    @_auto_manage_session
    def perform_script(self,
                       layout: str,
                       name: str,
                       param: Optional[str] = None,
                       **kwargs
                       ) -> ScriptResult:

        path = APIPath.SCRIPT.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            script_name=name
        )

        return ScriptResult(self.call_filemaker(method='GET', path=path, params={'script.param': param}, **kwargs))

    @_auto_manage_session
    def upload_container(self,
                         layout: str,
                         record_id: str,
                         field_name: str,
                         file: IO,
                         field_repetition: int = 1,
                         **kwargs
                         ) -> UploadContainerResult:

        path = APIPath.UPLOAD_CONTAINER.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout,
            record_id=record_id,
            field_name=field_name,
            field_repetition=field_repetition
        )

        # Let requests handle multipart/form-data
        return UploadContainerResult(
            self.call_filemaker('POST', path, files={'upload': file}, content_type=None, **kwargs))

    @_auto_manage_session
    def get_records(self,
                    layout: str,
                    offset: int = 1,
                    limit: int = 100,
                    response_layout: Optional[str] = None,
                    sort: Optional[SortInput] = None,
                    portals: Optional[PortalsInput] = None,
                    scripts: Optional[ScriptsInput] = None,
                    date_formats: Optional[DateInput] = None,
                    **kwargs
                    ) -> GetRecordsResult:

        path = APIPath.RECORDS.value.format(
            api_version=self._pop_api_version(kwargs),
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

        return GetRecordsResult(raw_content=self.call_filemaker(method='GET', path=path, params=params, **kwargs),
                                client=self,
                                layout=layout)

    def get_records_paginated(self,
                              offset: int = 1,
                              page_size: Optional[int] = 100,
                              limit: Optional[int] = 200,
                              **kwargs
                              ) -> GetRecordsPaginatedResult:

        return GetRecordsPaginatedResult(
            pages=cached_page_generator(
                client=self,
                fn_get_response=self.get_records,
                offset=offset,
                page_size=page_size,
                limit=limit,
                **kwargs
            )
        )

    @_auto_manage_session
    def find(self,
             layout: str,
             query: QueryInput,
             sort: Optional[SortInput] = None,
             offset: int = 1,
             limit: int = 100,
             portals: Optional[PortalsInput] = None,
             scripts: Optional[ScriptsInput] = None,
             date_formats: Optional[DateInput] = None,
             response_layout: Optional[str] = None,
             **kwargs
             ) -> FindResult:

        path = APIPath.FIND.value.format(
            api_version=self._pop_api_version(kwargs),
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

        return FindResult(raw_content=self.call_filemaker(method='POST', path=path, data=data, **kwargs),
                          client=self,
                          layout=layout)

    def find_paginated(self,
                       offset: int = 1,
                       page_size: Optional[int] = 100,
                       limit: Optional[int] = 200,
                       **kwargs
                       ) -> FindPaginatedResult:

        return FindPaginatedResult(
            pages=cached_page_generator(
                client=self,
                fn_get_response=self.get_records,
                offset=offset,
                page_size=page_size,
                limit=limit,
                **kwargs
            )
        )

    @_auto_manage_session
    def set_globals(self, global_fields: Dict[str, Any], **kwargs) -> SetGlobalResult:
        path = APIPath.GLOBALS.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database
        )

        data = {'globalFields': global_fields}
        return SetGlobalResult(self.call_filemaker(method='PATCH', path=path, data=data, **kwargs))

    def get_product_info(self, **kwargs) -> GetProductInfoResult:
        path = APIPath.META_PRODUCT.value.format(
            api_version=self._pop_api_version(kwargs)
        )

        return GetProductInfoResult(self.call_filemaker(method='GET', path=path, use_session_token=False, **kwargs))

    def get_databases(self,
                      username: Optional[str] = None,
                      password: Optional[str] = None,
                      **kwargs) -> GetDatabasesResult:
        path = APIPath.META_DATABASES.value.format(
            api_version=self._pop_api_version(kwargs)
        )

        auth = (username, password) if (username and password) else None
        return GetDatabasesResult(
            self.call_filemaker(method='GET', path=path, auth=auth, use_session_token=False, **kwargs))

    @_auto_manage_session
    def get_layouts(self, **kwargs) -> GetLayoutsResult:
        path = APIPath.META_LAYOUTS.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database
        )

        return GetLayoutsResult(self.call_filemaker(method='GET', path=path, **kwargs))

    @_auto_manage_session
    def get_layout(self, layout: Optional[str] = None,
                   **kwargs) -> GetLayoutResult:

        path = APIPath.META_LAYOUT.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            layout=layout
        )

        return GetLayoutResult(self.call_filemaker(method='GET', path=path, **kwargs))

    @_auto_manage_session
    def get_scripts(self, **kwargs) -> GetScriptsResult:
        path = APIPath.META_SCRIPTS.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database
        )

        return GetScriptsResult(self.call_filemaker(method='GET', path=path, **kwargs))

    def raw_login_username_password(self, username: str,
                                    password: str,
                                    data_sources: Optional[List[DataSourceProvider]] = None,
                                    **kwargs,
                                    ) -> LoginResult:

        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            token=''
        )

        data = clean_none({
            'fmDataSource': fm_data_source_from_providers(data_sources)
        })

        return LoginResult(
            self.call_filemaker(method='POST', path=path, data=data, auth=(username, password), **kwargs))

    def raw_login_oauth(self, oauth_request_id: str,
                        oauth_identifier: str,
                        data_sources: Optional[List[DataSourceProvider]],
                        **kwargs,
                        ) -> LoginResult:
        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._pop_api_version(kwargs),
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

        return LoginResult(self.call_filemaker(method='POST', path=path, data=data, headers=headers, **kwargs))

    def raw_login_claris_cloud(self, fmid_token: str,
                               data_sources: Optional[List[DataSourceProvider]],
                               **kwargs,
                               ) -> LoginResult:
        path = APIPath.AUTH_SESSION.value.format(
            api_version=self._pop_api_version(kwargs),
            database=self.database,
            token=''
        )

        data = clean_none({
            'fmDataSource': fm_data_source_from_providers(data_sources)
        })

        headers = {
            'Authorization': f'FMID {fmid_token}'
        }

        return LoginResult(
            self.call_filemaker(method='POST', path=path, data=data, headers=headers, use_session_token=False,
                                **kwargs))

    def _pop_api_version(self, kwargs) -> str:
        value_in_kwargs = kwargs.pop('api_version', None)
        return value_in_kwargs if value_in_kwargs else self.api_version

    def _pop_connection_timeout(self, kwargs) -> float:
        value_in_kwargs = kwargs.pop('connection_timeout', None)
        return value_in_kwargs if value_in_kwargs else self.connection_timeout

    def _pop_read_timeout(self, kwargs) -> float:
        value_in_kwargs = kwargs.pop('read_timeout', None)
        return value_in_kwargs if value_in_kwargs else self.read_timeout

    def _pop_http_client_extra_params(self, kwargs) -> Dict:
        value_in_kwargs = kwargs.pop('http_client_extra_params', None)
        return value_in_kwargs if value_in_kwargs else self.http_client_extra_params

    def _pop_verify_ssl(self, kwargs) -> VerifySSL:
        value_in_kwargs = kwargs.pop('verify_ssl', None)
        return value_in_kwargs if value_in_kwargs else self.verify_ssl

    def _raise_exception_if_too_fast(self):
        if self.too_fast_login_retry_timeout is None or self._session_last_login_retry is None:
            return

        elapsed = time.time() - self._session_last_login_retry

        if elapsed <= self.too_fast_login_retry_timeout:
            raise LoginRetriedTooFastException(
                f"Last failed login retry was {elapsed * 1000:.0f}ms ago, "
                f"retry timeout is {self.too_fast_login_retry_timeout * 1000:.0f}ms."
            )

    def with_layout(self, layout: str) -> FMClient:
        return FMClientProxy(client=self, layout=layout)

    def with_api_version(self, api_version: str) -> FMClient:
        return FMClientProxy(client=self, api_version=api_version)

    def with_connection_timeout(self, connection_timeout: float) -> FMClient:
        return FMClientProxy(client=self, connection_timeout=connection_timeout)

    def with_read_timeout(self, read_timeout: float) -> FMClient:
        return FMClientProxy(client=self, read_timeout=read_timeout)

    def with_http_client_extra_params(self, http_client_extra_params: Dict) -> FMClient:
        return FMClientProxy(client=self, http_client_extra_params=http_client_extra_params)

    def with_verify_ssl(self, verify_ssl: VerifySSL) -> FMClient:
        return FMClientProxy(client=self, verify_ssl=verify_ssl)

    def _request(self, *args, **kwargs) -> requests.Response:
        return requests.request(*args,
                                timeout=(self._pop_connection_timeout(kwargs), self._pop_read_timeout(kwargs)),
                                **kwargs)

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

        response = self._request(
            method=method,
            headers=request_headers,
            url=url,
            data=request_data,
            verify=self._pop_verify_ssl(kwargs),
            params=params,
            **self._pop_http_client_extra_params(kwargs),
            **kwargs
        )

        return response.json()

    def __repr__(self) -> str:
        return f"<FMClient logged_in={bool(not self._session_invalid)} token={self._token} database={self.database}>"


@dataclass(frozen=True)
class FMClientProxy:
    client: FMClient
    layout: Optional[str] = None
    api_version: Optional[str] = None
    connection_timeout: Optional[float] = None
    read_timeout: Optional[float] = None
    http_client_extra_params: Optional[Dict] = None
    verify_ssl: Optional[VerifySSL] = None

    def __getattr__(self, name):
        attr = getattr(self.client, name)
        if callable(attr):
            return lambda *args, **kwargs: attr(*args, **{**self.proxy_data, **kwargs})
        else:
            return attr

    @cached_property
    def proxy_data(self):
        return clean_none({
            'layout': self.layout,
            'api_version': self.api_version,
            'connection_timeout': self.connection_timeout,
            'read_timeout': self.read_timeout,
            'http_client_extra_params': self.http_client_extra_params,
            'verify_ssl': self.verify_ssl,
        })

    def with_layout(self, layout: str) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'layout': layout}
        return FMClientProxy(client=self.client, **new_proxy_data)

    def with_api_version(self, api_version: str) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'api_version': api_version}
        return FMClientProxy(client=self.client, **new_proxy_data)

    def with_connection_timeout(self, connection_timeout: float) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'connection_timeout': connection_timeout}
        return FMClientProxy(client=self.client, **new_proxy_data)

    def with_read_timeout(self, read_timeout: float) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'read_timeout': read_timeout}
        return FMClientProxy(client=self.client, **new_proxy_data)

    def with_http_client_extra_params(self, http_client_extra_params: Dict) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'http_client_extra_params': http_client_extra_params}
        return FMClientProxy(client=self.client, **new_proxy_data)

    def with_verify_ssl(self, verify_ssl: VerifySSL) -> FMClient:
        new_proxy_data = {**self.proxy_data, 'verify_ssl': verify_ssl}
        return FMClientProxy(client=self.client, **new_proxy_data)


def page_generator(
        client: FMClient,
        layout: str,
        fn_get_response: callable = None,
        offset: int = 1,
        page_size: Optional[int] = 100,
        limit: Optional[int] = 200,
        **kwargs
) -> Iterator[Page]:
    if offset < 1:
        raise ValueError("offset must be greater or equal to 1")

    if page_size is None and limit is None:
        raise ValueError("Either page_size or limit must be provided")

    if page_size is not None and page_size <= 0:
        raise ValueError("page_size must be greater than 0 or None")

    if limit is not None and limit <= 0:
        raise ValueError("limit must be greater than 0 or None")

    # At this point we have at least one between "page_size" and "limit" set.
    # We want to read all the records in range [offset, offset+limit-1]
    # If "limit" = None it means that we want to read the full DB
    # If the "page_size" is None it means that we want to read all the records in one go

    if page_size is None or (limit is not None and limit <= page_size):
        page_size = limit

    is_final_page = False
    records_retrieved = 0
    page_number = 0

    while is_final_page is False:
        # Calculate the limit for the next request
        if limit is None:
            # If the global limit is not defined we don't know how many records we have to retrieve
            # so we set the limit for the next request to the page_size and proceed until we get RECORD_IS_MISSING
            limit_for_current_request = page_size
        else:
            remaining = limit - records_retrieved

            if remaining <= page_size:
                # If the remaining records are less than the page_size we are sure that this will be the last page
                is_final_page = True

            assert remaining > 0, "remaining <= 0! This should not happen"

            limit_for_current_request = min(page_size, remaining)

        client_response = fn_get_response(
            layout=layout,
            offset=offset,
            limit=limit_for_current_request,
            **kwargs
        )

        result = CommonSearchRecordsResult(raw_content=client_response.raw_content, client=client,
                                           layout=layout)
        has_messages = any(result.messages_iterator)
        if has_messages:
            message_is_record_is_missing = any(
                result.get_messages_iterator(search_codes=[FMErrorEnum.RECORD_IS_MISSING]))
            if message_is_record_is_missing:
                is_final_page = True
            else:
                result.raise_exception_if_has_error()

        yield Page(result=result)

        # Update offset and retrived for the next page
        records_retrieved += limit_for_current_request
        offset += limit_for_current_request
        page_number += 1


def cached_page_generator(
        **kwargs
) -> CacheIterator[Page]:
    return CacheIterator(page_generator(**kwargs))
