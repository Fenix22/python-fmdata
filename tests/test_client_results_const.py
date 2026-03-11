from __future__ import annotations

import builtins
import importlib.util
import json
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import fmdata
from fmdata.client import (
    Client,
    DataSourceProvider,
    FMVersion,
    IncompatibleVersionException,
    LoginFailedException,
    LoginProvider,
    LoginRetriedTooFastException,
    _auto_manage_session,
    assert_fm_version_gte,
    cached_page_generator,
    fm_data_source_from_providers,
    fm_version_gte,
    map_version_or_raise,
    page_generator,
    portal_page_generator,
)
from fmdata.const import APIPath, FMErrorEnum
from fmdata.results import (
    CommonSearchRecordsResult,
    CreateRecordResult,
    Data,
    DuplicateRecordResult,
    EditRecordResult,
    FileMakerErrorException,
    FindPaginatedResult,
    GetDatabasesResult,
    GetLayoutResult,
    GetLayoutsResult,
    GetProductInfoResult,
    GetRecordResult,
    GetRecordsPaginatedResult,
    GetRecordsResult,
    GetScriptsResult,
    LoginResult,
    Message,
    Page,
    PortalPage,
    ScriptResult,
)


class DummyResponse:
    def __init__(self, response=None, messages=None, raise_error=None):
        self._response = response or {}
        self._messages = messages if messages is not None else [{"code": "0", "message": "OK"}]
        self._raise_error = raise_error
        self._parse_float = str
        self.headers = {"X-Test": "1"}
        self.content = b"response-body"

    def json(self, parse_float=None):
        if parse_float is not None:
            self._parse_float = parse_float
        return {
            "messages": self._messages,
            "response": self._response,
        }

    def raise_for_status(self):
        if self._raise_error is not None:
            raise self._raise_error
        return None


class FakeLoginProvider(LoginProvider):
    def __init__(self, token="token-1", error=None):
        self.token = token
        self.error = error

    def login(self, fm_client: Client, **kwargs) -> str:
        if self.error is not None:
            raise self.error
        return self.token


class FakeDataSourceProvider(DataSourceProvider):
    def __init__(self, payload):
        self.payload = payload

    def provide(self, **kwargs):
        return self.payload


def make_client(auto_manage_session=False, version=FMVersion.V22):
    client = Client(
        url="https://example.com",
        database="db",
        login_provider=FakeLoginProvider(),
        version=version,
        auto_manage_session=auto_manage_session,
    )
    client._session_invalid = False
    client._token = "session-token"
    return client


class ConstCoverageTests(unittest.TestCase):
    CONST_PATH = Path("/Users/desiena/PycharmProjects/python-fmdata/fmdata/const.py")

    def _exec_const_module(self, custom_import):
        source = self.CONST_PATH.read_text()
        builtins_dict = dict(vars(builtins))
        builtins_dict["__import__"] = custom_import
        namespace = {"__builtins__": builtins_dict}
        exec(compile(source, str(self.CONST_PATH), "exec"), namespace)
        return namespace

    def test_const_importlib_metadata_fallback(self):
        fake_module = types.SimpleNamespace(
            version=lambda package_name: "1.2.3",
            PackageNotFoundError=RuntimeError,
        )
        real_import = builtins.__import__

        def custom_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "importlib.metadata":
                raise ImportError("missing stdlib module")
            if name == "importlib_metadata":
                return fake_module
            return real_import(name, globals, locals, fromlist, level)

        namespace = self._exec_const_module(custom_import)
        self.assertEqual(namespace["__version__"], "1.2.3")

    def test_const_pkg_resources_fallback_and_final_import_error(self):
        distribution_error = type("DistributionNotFound", (Exception,), {})
        fake_pkg_resources = types.SimpleNamespace(
            DistributionNotFound=distribution_error,
            get_distribution=lambda package_name: types.SimpleNamespace(version="9.9.9"),
        )
        real_import = builtins.__import__

        def custom_import_pkg(name, globals=None, locals=None, fromlist=(), level=0):
            if name in {"importlib.metadata", "importlib_metadata"}:
                raise ImportError("missing metadata provider")
            if name == "pkg_resources":
                return fake_pkg_resources
            return real_import(name, globals, locals, fromlist, level)

        namespace = self._exec_const_module(custom_import_pkg)
        self.assertEqual(namespace["__version__"], "9.9.9")

        fake_pkg_resources_missing = types.SimpleNamespace(
            DistributionNotFound=distribution_error,
            get_distribution=lambda package_name: (_ for _ in ()).throw(distribution_error()),
        )

        def custom_import_pkg_missing(name, globals=None, locals=None, fromlist=(), level=0):
            if name in {"importlib.metadata", "importlib_metadata"}:
                raise ImportError("missing metadata provider")
            if name == "pkg_resources":
                return fake_pkg_resources_missing
            return real_import(name, globals, locals, fromlist, level)

        namespace = self._exec_const_module(custom_import_pkg_missing)
        self.assertEqual(namespace["__version__"], "0.0.0-dev")

        def custom_import_all_missing(name, globals=None, locals=None, fromlist=(), level=0):
            if name in {"importlib.metadata", "importlib_metadata", "pkg_resources"}:
                raise ImportError("missing everything")
            return real_import(name, globals, locals, fromlist, level)

        with self.assertRaises(ImportError):
            self._exec_const_module(custom_import_all_missing)

    def test_error_enum_string_representation(self):
        self.assertEqual(str(FMErrorEnum.NO_ERROR), "NO_ERROR (0): No error")


class ClientUnitTests(unittest.TestCase):
    def test_data_source_provider_and_version_mapping_helpers(self):
        with self.assertRaises(NotImplementedError):
            LoginProvider().login(make_client())
        self.assertIsNone(DataSourceProvider().provide())
        providers = [FakeDataSourceProvider({"db": "a"}), FakeDataSourceProvider({"db": "b"})]
        self.assertEqual(fm_data_source_from_providers(None), None)
        self.assertEqual(fm_data_source_from_providers(providers), [{"db": "a"}, {"db": "b"}])

        self.assertEqual(map_version_or_raise("22.0.1"), FMVersion.V22)
        self.assertEqual(map_version_or_raise(17), FMVersion.V17)
        self.assertEqual(map_version_or_raise(18), FMVersion.V18)
        self.assertEqual(map_version_or_raise(19), FMVersion.V19)
        self.assertEqual(map_version_or_raise(20), FMVersion.V20)
        self.assertEqual(map_version_or_raise(21), FMVersion.V21)
        self.assertEqual(map_version_or_raise(50), FMVersion.V22)
        self.assertEqual(map_version_or_raise(FMVersion.V22), FMVersion.V22)

        with self.assertRaises(ValueError):
            map_version_or_raise(None)
        with self.assertRaises(ValueError):
            map_version_or_raise(16)
        with self.assertRaises(ValueError):
            map_version_or_raise(101)
        with self.assertRaises(ValueError):
            map_version_or_raise([])
        with self.assertRaises(ValueError):
            map_version_or_raise(170000001)

    def test_client_init_login_logout_and_repr(self):
        with self.assertRaises(ValueError):
            Client(url=None, database="db", login_provider=FakeLoginProvider(), version=22)
        with self.assertRaises(ValueError):
            Client(url="https://example.com", database=None, login_provider=FakeLoginProvider(), version=22)
        with self.assertRaises(ValueError):
            Client(url="https://example.com", database="db", login_provider=None, version=22)

        client = Client(
            url="https://example.com",
            database="db",
            login_provider=FakeLoginProvider("fresh-token"),
            version=22,
        )
        with patch.object(client, "on_new_session") as on_new_session:
            client.login()
        self.assertEqual(client._token, "fresh-token")
        self.assertFalse(client._session_invalid)
        self.assertIsNotNone(client._session_last_login_retry)
        on_new_session.assert_called_once()
        self.assertIn("logged_in=True", repr(client))

        with patch.object(client, "call_filemaker", return_value=DummyResponse()) as call_filemaker:
            result = client.logout()
        self.assertIsNotNone(result)
        self.assertEqual(call_filemaker.call_args.kwargs["method"], "DELETE")

        client._session_invalid = True
        self.assertIsNone(client.logout())
        client.on_new_session()

        failing_client = Client(
            url="https://example.com",
            database="db",
            login_provider=FakeLoginProvider(error=RuntimeError("boom")),
            version=22,
        )
        with self.assertRaises(LoginFailedException):
            failing_client.login()
        self.assertTrue(failing_client._session_invalid)
        self.assertIsNone(failing_client._token)

        client_without_provider = make_client()
        client_without_provider.login_provider = None
        with self.assertRaises(ValueError):
            client_without_provider.login()

    def test_safe_login_and_retry_timing(self):
        client = make_client(auto_manage_session=True)
        client._session_invalid = True
        with patch.object(client, "_raise_exception_if_too_fast") as raise_if_too_fast, patch.object(client, "login") as login:
            client.safe_login_if_not()
        raise_if_too_fast.assert_called_once()
        login.assert_called_once()

        client = make_client(auto_manage_session=True)
        client._session_invalid = True
        with patch.object(client, "login") as login:
            client.safe_login_if_not(exception_if_too_fast=False)
        login.assert_called_once()

        client._session_invalid = False
        with patch.object(client, "login") as login:
            client.safe_login_if_not()
        login.assert_not_called()

        client.too_fast_login_retry_timeout = 1
        client._session_last_login_retry = 100.0
        with patch("fmdata.client.time.time", return_value=100.5):
            with self.assertRaises(LoginRetriedTooFastException):
                client._raise_exception_if_too_fast()

        client.too_fast_login_retry_timeout = None
        client._raise_exception_if_too_fast()

    def test_auto_manage_session_wrapper_paths(self):
        client = make_client(auto_manage_session=False)
        client._session_invalid = True
        with self.assertRaises(ValueError):
            client.create_record(layout="L", field_data={})

        client = make_client(auto_manage_session=True)
        invalid = DummyResponse(messages=[{"code": "952", "message": "Invalid token"}])
        valid = DummyResponse(response={"recordId": "1", "modId": "2"})
        with patch.object(client, "safe_login_if_not") as safe_login, patch.object(
            client, "call_filemaker", side_effect=[invalid, valid]
        ):
            result = client.create_record(layout="L", field_data={})
        self.assertIsInstance(result, CreateRecordResult)
        self.assertEqual(result.response.record_id, "1")
        self.assertEqual(safe_login.call_count, 2)
        self.assertTrue(client._session_invalid)

        client = make_client(auto_manage_session=True)
        with patch.object(client, "safe_login_if_not"), patch.object(
            client, "call_filemaker", side_effect=[invalid, invalid]
        ):
            with self.assertRaises(FileMakerErrorException):
                client.create_record(layout="L", field_data={})

        client = make_client(auto_manage_session=False)

        @_auto_manage_session
        def wrapped(self):
            return "ok"

        client._session_invalid = False
        self.assertEqual(wrapped(client), "ok")

    def test_request_building_wrappers(self):
        client = make_client()
        scripts = {
            "prerequest": {"name": "Pre", "param": "1"},
            "presort": {"name": "Sort", "param": "2"},
            "after": {"name": "After", "param": "3"},
        }
        portals = {"Portal": {"offset": 2, "limit": 5}}

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"recordId": "1", "modId": "2"})) as call:
            client.create_record(layout="L", field_data={"A": 1}, portal_data={"P": []}, scripts=scripts, options={"entrymode": "script"}, date_formats=2)
        self.assertEqual(call.call_args.kwargs["method"], "POST")
        self.assertEqual(call.call_args.kwargs["data"]["fieldData"], {"A": 1})

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"recordId": "2", "modId": "3"})) as call:
            client.duplicate_record(layout="L", record_id="1", scripts=scripts)
        self.assertEqual(call.call_args.kwargs["params"]["script"], "After")

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"modId": "4"})) as call:
            client.edit_record(layout="L", record_id="1", field_data={"A": 2}, mod_id="99", portal_data={"P": []}, scripts=scripts, options={"entrymode": "script"}, date_formats=1)
        self.assertEqual(call.call_args.kwargs["method"], "PATCH")
        self.assertEqual(call.call_args.kwargs["data"]["modId"], "99")

        with patch.object(client, "call_filemaker", return_value=DummyResponse()) as call:
            client.delete_record(layout="L", record_id="1", scripts=scripts)
        self.assertEqual(call.call_args.kwargs["params"]["script.prerequest"], "Pre")

        get_record_response = DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1"}]})
        with patch.object(client, "call_filemaker", return_value=get_record_response) as call:
            result = client.get_record(layout="L", record_id="1", response_layout="Slim", portals=portals, scripts=scripts)
        self.assertIsInstance(result, GetRecordResult)
        self.assertEqual(call.call_args.kwargs["params"]["layout.response"], "Slim")
        self.assertEqual(call.call_args.kwargs["params"]["portal"], "[\"Portal\"]")

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"scriptResult": "ok", "scriptError": "0"})) as call:
            client.perform_script(layout="L", name="Run", param="x")
        self.assertEqual(call.call_args.kwargs["params"], {"script.param": "x"})

        with patch.object(client, "call_filemaker", return_value=DummyResponse()) as call:
            client.upload_container(layout="L", record_id="1", field_name="Photo", field_repetition=2, file=Mock())
        self.assertIsNone(call.call_args.kwargs["content_type"])
        self.assertIn("upload", call.call_args.kwargs["files"])

        get_records_response = DummyResponse(response={"data": [], "dataInfo": {"foundCount": 0}})
        with patch.object(client, "call_filemaker", return_value=get_records_response) as call:
            result = client.get_records(layout="L", offset=3, limit=4, response_layout="Slim", sort=[{"fieldName": "Name", "sortOrder": "ascend"}], portals=portals, scripts=scripts, date_formats=0)
        self.assertIsInstance(result, GetRecordsResult)
        self.assertEqual(call.call_args.kwargs["params"]["_offset"], 3)
        self.assertEqual(call.call_args.kwargs["params"]["_sort"], json.dumps([{"fieldName": "Name", "sortOrder": "ascend"}]))

        with patch.object(client, "get_records", return_value=result):
            paged = client.get_records_paginated(layout="L", offset=2, page_size=3, limit=4)
        self.assertIsInstance(paged, GetRecordsPaginatedResult)

        find_response = DummyResponse(response={"data": [], "dataInfo": {"foundCount": 0}})
        with patch.object(client, "call_filemaker", return_value=find_response) as call:
            result = client.find(layout="L", query=[{"Name": "Alice"}], sort=[{"fieldName": "Name", "sortOrder": "ascend"}], offset=2, limit=4, portals=portals, scripts=scripts, date_formats=2, response_layout="Slim")
        self.assertEqual(call.call_args.kwargs["data"]["offset"], "2")
        self.assertEqual(call.call_args.kwargs["data"]["portal"], ["Portal"])

        with patch.object(client, "find", return_value=result):
            paged = client.find_paginated(layout="L", offset=2, page_size=3, limit=4, query=[{"Name": "Alice"}])
        self.assertIsInstance(paged, FindPaginatedResult)

        with patch.object(client, "call_filemaker", return_value=DummyResponse()) as call:
            client.set_globals({"Globals::A": "1"})
        self.assertEqual(call.call_args.kwargs["data"], {"globalFields": {"Globals::A": "1"}})

    def test_metadata_and_raw_login_requests(self):
        client = make_client(version=FMVersion.V22)

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"name": "Server"})) as call:
            result = client.get_product_info()
        self.assertIsInstance(result, GetProductInfoResult)
        self.assertFalse(call.call_args.kwargs["use_session_token"])

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"databases": [{"name": "db"}]})) as call:
            result = client.get_databases(username="u", password="p")
        self.assertIsInstance(result, GetDatabasesResult)
        self.assertEqual(call.call_args.kwargs["auth"], ("u", "p"))

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"layouts": [{"name": "L", "table": "T"}]})) as call:
            result = client.get_layouts()
        self.assertIsInstance(result, GetLayoutsResult)
        self.assertEqual(call.call_args.kwargs["path"], APIPath.META_LAYOUTS.value.format(api_version="v1", database="db"))

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"fieldMetaData": [], "portalMetaData": {}})) as call:
            result = client.get_layout(layout="L")
        self.assertIsInstance(result, GetLayoutResult)
        self.assertIn("/layouts/L", call.call_args.kwargs["path"])

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"scripts": []})) as call:
            result = client.get_scripts()
        self.assertIsInstance(result, GetScriptsResult)
        self.assertIn("/scripts", call.call_args.kwargs["path"])

        providers = [FakeDataSourceProvider({"database": "db2"})]
        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"token": "t"})) as call:
            result = client.raw_login_username_password(username="u", password="p", data_sources=providers)
        self.assertIsInstance(result, LoginResult)
        self.assertEqual(call.call_args.kwargs["auth"], ("u", "p"))
        self.assertEqual(call.call_args.kwargs["data"], {"fmDataSource": [{"database": "db2"}]})

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"token": "t"})) as call:
            client.raw_login_oauth(oauth_request_id="rid", oauth_identifier="oid", data_sources=None)
        self.assertEqual(call.call_args.kwargs["headers"]["X-FM-Data-OAuth-Request-Id"], "rid")

        with patch.object(client, "call_filemaker", return_value=DummyResponse(response={"token": "t"})) as call:
            client.raw_login_claris_cloud(fmid_token="fmid", data_sources=None)
        self.assertEqual(call.call_args.kwargs["headers"]["Authorization"], "FMID fmid")
        self.assertFalse(call.call_args.kwargs["use_session_token"])

    def test_call_filemaker_timeout_helpers_and_repr(self):
        client = Client(
            url="https://example.com",
            database="db",
            login_provider=FakeLoginProvider(),
            version=22,
            connection_timeout=11,
            read_timeout=12,
            http_client_extra_params={"stream": True},
            verify_ssl="/tmp/cert.pem",
        )
        client._session_invalid = False
        client._token = "token"

        self.assertEqual(client._pop_connection_timeout({}), 11)
        self.assertEqual(client._pop_read_timeout({}), 12)
        self.assertEqual(client._pop_http_client_extra_params({}), {"stream": True})
        self.assertEqual(client._pop_verify_ssl({}), "/tmp/cert.pem")

        with patch("fmdata.client.requests.request", return_value=DummyResponse()) as request, patch(
            "fmdata.client.logger.isEnabledFor", return_value=True
        ), patch("fmdata.client.logger.debug") as debug:
            response = client.call_filemaker(
                method="POST",
                path="/path",
                headers={"X-Test": "1"},
                data={"a": 1},
                params={"p": 2},
                parse_float=float,
                verify_ssl=False,
                connection_timeout=1,
                read_timeout=2,
                http_client_extra_params={"allow_redirects": False},
            )
        self.assertIs(response._parse_float, float)
        self.assertEqual(request.call_args.kwargs["url"], "https://example.com/path")
        self.assertEqual(request.call_args.kwargs["timeout"], (1, 2))
        self.assertFalse(request.call_args.kwargs["verify"])
        self.assertFalse(request.call_args.kwargs["allow_redirects"])
        self.assertEqual(json.loads(request.call_args.kwargs["data"]), {"a": 1})
        self.assertEqual(request.call_args.kwargs["headers"]["Authorization"], "Bearer token")
        self.assertEqual(request.call_args.kwargs["headers"]["Content-Type"], "application/json")
        self.assertGreaterEqual(debug.call_count, 2)

        with patch("fmdata.client.requests.request", return_value=DummyResponse()) as request:
            client.call_filemaker(method="GET", path="/path", content_type=None, use_session_token=False)
        self.assertNotIn("Content-Type", request.call_args.kwargs["headers"])
        self.assertNotIn("Authorization", request.call_args.kwargs["headers"])

    def test_page_generators_and_version_helpers(self):
        client = make_client()

        with self.assertRaises(ValueError):
            list(page_generator(client=client, layout="L", fn_get_response=lambda **kwargs: None, offset=0, page_size=1))
        with self.assertRaises(ValueError):
            list(page_generator(client=client, layout="L", fn_get_response=lambda **kwargs: None))
        with self.assertRaises(ValueError):
            list(page_generator(client=client, layout="L", fn_get_response=lambda **kwargs: None, page_size=0))
        with self.assertRaises(ValueError):
            list(page_generator(client=client, layout="L", fn_get_response=lambda **kwargs: None, page_size=1, limit=0))

        responses = [
            GetRecordsResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1"}]}), layout="L", client=client),
            GetRecordsResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "2"}]}), layout="L", client=client),
        ]
        pages = list(
            page_generator(
                client=client,
                layout="L",
                fn_get_response=Mock(side_effect=responses),
                offset=1,
                page_size=1,
                limit=2,
            )
        )
        self.assertEqual(len(pages), 2)
        self.assertIsInstance(cached_page_generator(client=client, layout="L", fn_get_response=Mock(return_value=responses[0]), page_size=1, limit=1)[0], Page)

        no_match = GetRecordsResult(
            DummyResponse(messages=[{"code": "401", "message": "No records"}], response={"data": []}),
            layout="L",
            client=client,
        )
        self.assertEqual(len(list(page_generator(client=client, layout="L", fn_get_response=Mock(return_value=no_match), page_size=1, limit=1))), 1)
        no_limit_pages = list(
            page_generator(
                client=client,
                layout="L",
                fn_get_response=Mock(side_effect=[
                    GetRecordsResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1"}, {"fieldData": {}, "recordId": "2"}]}), layout="L", client=client),
                    GetRecordsResult(DummyResponse(response={"data": []}), layout="L", client=client),
                ]),
                page_size=2,
                limit=None,
            )
        )
        self.assertEqual(len(no_limit_pages), 2)

        with self.assertRaises(ValueError):
            list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", offset=0, page_size=1))
        with self.assertRaises(ValueError):
            list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P"))
        with self.assertRaises(ValueError):
            list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", page_size=0))
        with self.assertRaises(ValueError):
            list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", page_size=1, limit=0))

        client.get_record = Mock(side_effect=[
            GetRecordResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1", "portalData": {"P": [{"recordId": "10"}]}}]}), layout="L", client=client),
            GetRecordResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1", "portalData": {"P": [{"recordId": "11"}]}}]}), layout="L", client=client),
        ])
        portal_pages = list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", page_size=1, limit=2))
        self.assertEqual(len(portal_pages), 2)

        client.get_record = Mock(return_value=GetRecordResult(DummyResponse(response={"data": []}), layout="L", client=client))
        self.assertEqual(len(list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", page_size=1, limit=1))), 1)
        client.get_record = Mock(side_effect=[
            GetRecordResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1", "portalData": {"P": [{"recordId": "10"}, {"recordId": "11"}]}}]}), layout="L", client=client),
            GetRecordResult(DummyResponse(response={"data": [{"fieldData": {}, "recordId": "1", "portalData": {"P": []}}]}), layout="L", client=client),
        ])
        self.assertEqual(len(list(portal_page_generator(client=client, layout="L", record_id="1", portal_name="P", page_size=2, limit=None))), 2)

        self.assertTrue(fm_version_gte(client, FMVersion.V18))
        assert_fm_version_gte(client, FMVersion.V18)
        older = make_client(version=FMVersion.V17)
        with self.assertRaises(IncompatibleVersionException):
            assert_fm_version_gte(older, FMVersion.V18)


class ResultsUnitTests(unittest.TestCase):
    def make_result_response(self):
        return DummyResponse(
            response={
                "dataInfo": {
                    "database": "db",
                    "layout": "layout",
                    "table": "table",
                    "totalRecordCount": 3,
                    "foundCount": 2,
                    "returnedCount": 1,
                },
                "data": [
                    {
                        "fieldData": {"Name": "Alice"},
                        "recordId": "1",
                        "modId": "2",
                        "portalDataInfo": [{
                            "database": "db",
                            "table": "portal_table",
                            "foundCount": 2,
                            "returnedCount": 1,
                            "portalObjectName": "Portal",
                        }],
                        "portalData": {
                            "Portal": [{"recordId": "10", "modId": "11", "City": "Berlin"}]
                        },
                    }
                ],
                "scriptResult": "after",
                "scriptError": "0",
                "scriptResult.prerequest": "pre",
                "scriptError.prerequest": "1",
                "scriptResult.presort": "sort",
                "scriptError.presort": "2",
            }
        )

    def test_base_result_message_helpers_and_http_proxy(self):
        response = DummyResponse(messages=[{"code": "0", "message": "OK"}, {"code": "401", "message": "No records"}])
        result = CommonSearchRecordsResult(http_response=response, client=Mock(), layout="L")
        self.assertEqual(result.messages[0].code, "0")
        self.assertEqual(result.messages[1].message, "No records")
        self.assertEqual([msg.code for msg in result.get_messages_iterator(search_codes=[FMErrorEnum.NO_RECORDS_MATCH_REQUEST])], ["401"])
        self.assertEqual([msg.code for msg in result.get_messages_iterator(exclude_codes=[FMErrorEnum.NO_ERROR])], ["401"])
        self.assertEqual(result.errors, [])
        self.assertIs(result.raise_exception_if_has_error(), result)
        self.assertIs(result.raise_exception_if_has_message(include_codes=[999]), result)

        error_result = CreateRecordResult(DummyResponse(messages=[{"code": "500", "message": "Failure"}]))
        with self.assertRaises(FileMakerErrorException):
            error_result.raise_exception_if_has_message(include_codes=[500])
        with self.assertRaises(FileMakerErrorException):
            error_result.raise_exception_if_has_error()

        proxy = CommonSearchRecordsResult(http_response=DummyResponse(), client=Mock(), layout="L")
        proxy.ensure_2xx()
        failing = CommonSearchRecordsResult(http_response=DummyResponse(raise_error=RuntimeError("bad status")), client=Mock(), layout="L")
        with self.assertRaises(RuntimeError):
            failing.ensure_2xx()

    def test_search_record_response_proxies(self):
        result = CommonSearchRecordsResult(http_response=self.make_result_response(), client=Mock(), layout="L")
        response = result.response
        self.assertEqual(response.after_script_result, "after")
        self.assertEqual(response.after_script_error, "0")
        self.assertEqual(response.prerequest_script_result, "pre")
        self.assertEqual(response.prerequest_script_error, "1")
        self.assertEqual(response.presort_script_result, "sort")
        self.assertEqual(response.presort_script_error, "2")
        self.assertEqual(response.data_info.database, "db")
        self.assertEqual(response.data_info.layout, "layout")
        self.assertEqual(response.data_info.table, "table")
        self.assertEqual(response.data_info.total_record_count, 3)
        self.assertEqual(response.data_info.found_count, 2)
        self.assertEqual(response.data_info.returned_count, 1)

        data = response.data[0]
        self.assertEqual(data["Name"], "Alice")
        self.assertEqual(data.get("Missing", "fallback"), "fallback")
        self.assertEqual(data.field_data, {"Name": "Alice"})
        self.assertEqual(data.record_id, "1")
        self.assertEqual(data.mod_id, "2")
        self.assertEqual(data.portal_data_info[0].database, "db")
        self.assertEqual(data.portal_data_info[0].table, "portal_table")
        self.assertEqual(data.portal_data_info[0].found_count, 2)
        self.assertEqual(data.portal_data_info[0].returned_count, 1)
        self.assertEqual(data.portal_data_info[0].portal_object_name, "Portal")
        portal_data = data.portal_data
        self.assertEqual(portal_data["Portal"][0].record_id, "10")
        self.assertEqual(portal_data["Portal"][0].mod_id, "11")
        self.assertEqual(portal_data["Portal"][0].fields, {"City": "Berlin"})
        self.assertEqual(portal_data["Portal"][0]["City"], "Berlin")
        self.assertEqual(portal_data["Missing"], None)
        self.assertEqual(portal_data.get("Missing", []), [])
        self.assertEqual(len(list(response.data_iterator)), 1)

        no_portals = Data({"fieldData": {}, "recordId": "1"})
        self.assertIsNone(no_portals.portal_data_info)
        self.assertIsNone(no_portals.portal_data)
        self.assertIsNone(CommonSearchRecordsResult(DummyResponse(response={}), client=Mock(), layout="L").response.data_info)
        self.assertIsNone(CommonSearchRecordsResult(DummyResponse(response={"data": None}), client=Mock(), layout="L").response.data)
        self.assertIsNone(CommonSearchRecordsResult(DummyResponse(response={"data": None}), client=Mock(), layout="L").response.data_iterator)

    def test_result_type_specific_responses(self):
        self.assertEqual(ScriptResult(DummyResponse(response={"scriptResult": "ok", "scriptError": "0"})).response.script_result, "ok")
        self.assertEqual(ScriptResult(DummyResponse(response={"scriptResult": "ok", "scriptError": "0"})).response.script_error, "0")

        create = CreateRecordResult(DummyResponse(response={"recordId": "1", "modId": "2", "newPortalRecordInfo": [{"tableName": "TO", "recordId": "10", "modId": "11"}]}))
        self.assertEqual(create.response.record_id, "1")
        self.assertEqual(create.response.mod_id, "2")
        self.assertEqual(create.response.new_portal_record_info[0].table_name, "TO")
        self.assertEqual(create.response.new_portal_record_info[0].record_id, "10")
        self.assertEqual(create.response.new_portal_record_info[0].mod_id, "11")
        self.assertEqual(len(list(create.response.new_portal_record_info_iterator)), 1)
        self.assertIsNone(CreateRecordResult(DummyResponse(response={"recordId": "1", "modId": "2"})).response.new_portal_record_info)

        duplicate = DuplicateRecordResult(DummyResponse(response={"recordId": "3", "modId": "4"}))
        self.assertEqual(duplicate.response.record_id, "3")
        self.assertEqual(duplicate.response.mod_id, "4")

        edit = EditRecordResult(DummyResponse(response={"modId": "5"}))
        self.assertEqual(edit.response.mod_id, "5")

        login = LoginResult(DummyResponse(response={"token": "abc"}))
        self.assertEqual(login.response.token, "abc")

        product = GetProductInfoResult(DummyResponse(response={
            "name": "Server",
            "buildDate": "2026-01-01",
            "version": "22",
            "dateFormat": "M/d/yyyy",
            "timeFormat": "HH:mm:ss",
            "timeStampFormat": "M/d/yyyy HH:mm:ss",
        }))
        self.assertEqual(product.response.name, "Server")
        self.assertEqual(product.response.build_date, "2026-01-01")
        self.assertEqual(product.response.version, "22")
        self.assertEqual(product.response.date_format, "M/d/yyyy")
        self.assertEqual(product.response.time_format, "HH:mm:ss")
        self.assertEqual(product.response.time_stamp_format, "M/d/yyyy HH:mm:ss")

        databases = GetDatabasesResult(DummyResponse(response={"databases": [{"name": "db"}]}))
        self.assertEqual(databases.response.databases[0].name, "db")
        self.assertEqual(len(list(databases.response.databases_iterator)), 1)
        self.assertIsNone(GetDatabasesResult(DummyResponse(response={})).response.databases)

        layouts = GetLayoutsResult(DummyResponse(response={"layouts": [{"name": "L", "table": "T"}]}))
        self.assertEqual(layouts.response.layouts[0].name, "L")
        self.assertEqual(layouts.response.layouts[0].table, "T")
        self.assertEqual(len(list(layouts.response.layouts_iterator)), 1)
        self.assertIsNone(GetLayoutsResult(DummyResponse(response={})).response.layouts)

        layout = GetLayoutResult(DummyResponse(response={
            "fieldMetaData": [{
                "name": "Field",
                "type": "normal",
                "displayType": "text",
                "result": "text",
                "global": False,
                "autoEnter": True,
                "fourDigitYear": False,
                "maxRepeat": 1,
                "maxCharacters": 50,
                "notEmpty": False,
                "numeric": False,
                "timeOfDay": False,
                "repetitionStart": 1,
                "repetitionEnd": 1,
            }],
            "portalMetaData": {"Portal": [{"name": "Portal::Field"}]},
        }))
        field = layout.response.field_meta_data[0]
        self.assertEqual(field.name, "Field")
        self.assertEqual(field.type, "normal")
        self.assertEqual(field.display_type, "text")
        self.assertEqual(field.result, "text")
        self.assertFalse(field.global_)
        self.assertTrue(field.auto_enter)
        self.assertFalse(field.four_digit_year)
        self.assertEqual(field.max_repeat, 1)
        self.assertEqual(field.max_characters, 50)
        self.assertFalse(field.not_empty)
        self.assertFalse(field.numeric)
        self.assertFalse(field.time_of_day)
        self.assertEqual(field.repetition_start, 1)
        self.assertEqual(field.repetition_end, 1)
        self.assertEqual(len(list(layout.response.field_meta_data_iterator)), 1)
        self.assertEqual(layout.response.portal_meta_data["Portal"][0].name, "Portal::Field")
        self.assertEqual(len(list(layout.response.portal_meta_data_iterator["Portal"])), 1)
        self.assertIsNone(GetLayoutResult(DummyResponse(response={"fieldMetaData": None})).response.field_meta_data)
        self.assertIsNone(GetLayoutResult(DummyResponse(response={})).response.portal_meta_data)
        self.assertIsNone(GetLayoutResult(DummyResponse(response={})).response.portal_meta_data_iterator)

        scripts = GetScriptsResult(DummyResponse(response={"scripts": [{"name": "Folder", "isFolder": True, "folderScriptNames": [{"name": "Child", "isFolder": False}]}]}))
        script = list(scripts.response.scripts)[0]
        self.assertEqual(script.name, "Folder")
        self.assertTrue(script.is_folder)
        self.assertEqual(script.folder_script_names[0].name, "Child")
        self.assertEqual(len(list(script.folder_script_names_iterator)), 1)
        self.assertIsNone(GetScriptsResult(DummyResponse(response={})).response.scripts)

        self.assertEqual(str(FileMakerErrorException.from_response_message(Message({"code": 500, "message": "Boom"}))), "FileMaker Server returned error 500, Boom")
        self.assertIsInstance(Page(result=CommonSearchRecordsResult(DummyResponse(response={"data": []}), client=Mock(), layout="L")), Page)
        self.assertIsInstance(PortalPage(result=GetRecordResult(DummyResponse(response={"data": []}), client=Mock(), layout="L")), PortalPage)
