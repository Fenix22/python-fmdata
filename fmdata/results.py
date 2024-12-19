from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Iterator, Iterable

from statelessfm.fmclient.const import FMErrorEnum


@dataclass(frozen=True)
class BaseProxy:
    original_response: Dict[str, Any]


@dataclass(frozen=True)
class Message(BaseProxy):

    @property
    def code(self) -> Optional[int]:
        return self.original_response.get('code', None)

    @property
    def message(self) -> Optional[str]:
        return self.original_response.get('message', None)


@dataclass(frozen=True)
class BaseResult(BaseProxy):

    @property
    def messages(self) -> Iterator[Message]:
        return (Message(msg) for msg in self.original_response['messages'])

    def get_errors(self,
                   include_codes: Optional[List[FMErrorEnum | int]] = None,
                   exclude_codes: Optional[List[FMErrorEnum | int]] = None
                   ) -> Iterator[Message]:

        if exclude_codes is None:
            exclude_codes = [FMErrorEnum.NO_ERROR]

        return (msg for msg in self.messages
                if int(msg.code) not in exclude_codes and (include_codes is None or (int(msg.code) in include_codes)))

    def raise_exception_if_has_error(self,
                                     include_codes: Optional[List[FMErrorEnum | int]] = None,
                                     exclude_codes: Optional[List[FMErrorEnum | int]] = None
                                     ) -> None:

        error = next(self.get_errors(include_codes=include_codes, exclude_codes=exclude_codes), None)

        if error is not None:
            raise FileMakerErrorException(code=error.code, message=error.message)


@dataclass(frozen=True)
class LogoutResult(BaseResult, BaseProxy):
    pass


@dataclass(frozen=True)
class ScriptResponse(BaseProxy):

    @property
    def after_script_result(self) -> Optional[str]:
        return self.original_response.get('scriptResult', None)

    @property
    def after_script_error(self) -> Optional[str]:
        return self.original_response.get('scriptError', None)

    @property
    def prerequest_script_result(self) -> Optional[str]:
        return self.original_response.get('scriptResult.prerequest', None)

    @property
    def prerequest_script_error(self) -> Optional[str]:
        return self.original_response.get('scriptError.prerequest', None)

    @property
    def presort_script_result(self) -> Optional[str]:
        return self.original_response.get('scriptResult.presort', None)

    @property
    def presort_script_error(self) -> Optional[str]:
        return self.original_response.get('scriptError.presort', None)


@dataclass(frozen=True)
class ScriptResult(BaseResult):

    @property
    def response(self):
        return ScriptResponse(self.original_response['response'])


@dataclass(frozen=True)
class PortalDataInfo(BaseProxy):

    @property
    def database(self) -> Optional[str]:
        return self.original_response.get('database', None)

    @property
    def table(self) -> Optional[str]:
        return self.original_response.get('table', None)

    @property
    def found_count(self) -> Optional[int]:
        return self.original_response.get('foundCount', None)

    @property
    def returned_count(self) -> Optional[int]:
        return self.original_response.get('returnedCount', None)

    @property
    def portal_object_name(self) -> Optional[str]:
        return self.original_response.get('portalObjectName', None)


@dataclass(frozen=True)
class PortalData(BaseProxy):
    table_name: str

    # TODO check

    @property
    def extracted_field_data(self) -> Dict[str, Any]:
        prefix = f"{self.table_name}::"
        return {
            key[len(prefix):]: value
            for key, value in self.original_response.items()
            if key.startswith(prefix)
        }

    @property
    def record_id(self) -> Optional[str]:
        return self.original_response.get('recordId', None)

    @property
    def mod_id(self) -> Optional[str]:
        return self.original_response.get('modId', None)


@dataclass(frozen=True)
class DataInfo(BaseProxy):

    @property
    def database(self) -> Optional[str]:
        return self.original_response.get('database', None)

    @property
    def layout(self) -> Optional[str]:
        return self.original_response.get('layout', None)

    @property
    def table(self) -> Optional[str]:
        return self.original_response.get('table', None)

    @property
    def total_record_count(self) -> Optional[str]:
        return self.original_response.get('totalRecordCount', None)

    @property
    def found_count(self) -> Optional[int]:
        return self.original_response.get('foundCount', None)

    @property
    def returned_count(self) -> Optional[int]:
        return self.original_response.get('returnedCount', None)


@dataclass(frozen=True)
class Data(BaseProxy):

    @property
    def field_data(self) -> Dict[str, Any]:
        return self.original_response['fieldData']

    @property
    def record_id(self) -> Optional[str]:
        return self.original_response.get('recordId', None)

    @property
    def mod_id(self) -> Optional[str]:
        return self.original_response.get('modId', None)

    @property
    def portal_data_info(self) -> Optional[List[PortalDataInfo]]:
        portal_data_info_list: Optional[List[Dict[str, Any]]] = self.original_response.get('portalDataInfo', None)
        return [PortalDataInfo(portal_data_info) for portal_data_info in
                portal_data_info_list] if portal_data_info_list is not None else None

    @property
    def portal_data(self) -> Optional[Dict[str, PortalData]]:
        portal_data: Optional[Dict[str, Any]] = self.original_response.get('portalData', None)
        return {
            key: PortalData(table_name=key, original_response=value)
            for key, value in portal_data.items()
        } if portal_data is not None else None


@dataclass(frozen=True)
class CommonSearchRecordsResponseField(ScriptResponse):

    @property
    def data_info(self) -> Optional[DataInfo]:
        data_info: Optional[Dict[str, Any]] = self.original_response.get('dataInfo', None)
        return DataInfo(data_info) if data_info is not None else None

    @property
    def data(self) -> List[Data]:
        return [Data(record) for record in self.original_response['data']]


@dataclass(frozen=True)
class CommonSearchRecordsResult(BaseResult):

    @property
    def response(self):
        return CommonSearchRecordsResponseField(self.original_response['response'])


@dataclass(frozen=True)
class GetRecordResult(CommonSearchRecordsResult):
    pass


@dataclass(frozen=True)
class GetRecordsResult(CommonSearchRecordsResult):
    pass


@dataclass(frozen=True)
class FindResult(CommonSearchRecordsResult):
    pass


@dataclass(frozen=True)
class CreateRecordResponse(BaseProxy):

    @property
    def mod_id(self) -> str:
        return self.original_response['modId']

    @property
    def record_id(self) -> str:
        return self.original_response['recordId']


@dataclass(frozen=True)
class CreateRecordResult(BaseResult):

    @property
    def response(self):
        return CreateRecordResponse(original_response=self.original_response['response'])


@dataclass(frozen=True)
class EditRecordResponse(BaseProxy):

    @property
    def mod_id(self) -> str:
        return self.original_response['modId']


@dataclass(frozen=True)
class EditRecordResult(BaseResult):

    @property
    def response(self):
        return EditRecordResponse(original_response=self.original_response['response'])


@dataclass(frozen=True)
class DeleteRecordResult(BaseResult):
    pass


@dataclass(frozen=True)
class LoginResponse(BaseProxy):

    @property
    def token(self) -> str:
        return self.original_response['token']


@dataclass(frozen=True)
class LoginResult(BaseResult):

    @property
    def response(self):
        return LoginResponse(self.original_response['response'])


@dataclass(frozen=True)
class UploadContainerResult(BaseResult):
    pass


@dataclass(frozen=True)
class SetGlobalResult(BaseResult):
    pass


@dataclass(frozen=True)
class GetProductInfoResponse(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)

    @property
    def build_date(self) -> Optional[str]:
        return self.original_response.get('buildDate', None)

    @property
    def version(self) -> Optional[str]:
        return self.original_response.get('version', None)

    @property
    def date_format(self) -> Optional[str]:
        return self.original_response.get('dateFormat', None)

    @property
    def time_format(self) -> Optional[str]:
        return self.original_response.get('timeFormat', None)

    @property
    def time_stamp_format(self) -> Optional[str]:
        return self.original_response.get('timeStampFormat', None)


@dataclass(frozen=True)
class GetProductInfoResult(BaseResult):

    @property
    def response(self):
        return GetProductInfoResponse(self.original_response['response'])


@dataclass(frozen=True)
class Database(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)


@dataclass(frozen=True)
class GetDatabasesResponse(BaseProxy):

    @property
    def databases(self) -> Iterator[Database]:
        return (Database(database) for database in self.original_response['databases'])


@dataclass(frozen=True)
class GetDatabasesResult(BaseResult):

    @property
    def response(self):
        return GetDatabasesResponse(self.original_response['response'])


@dataclass(frozen=True)
class GetLayoutsLayout(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)

    @property
    def is_folder(self) -> Optional[bool]:
        return self.original_response.get('isFolder', None)

    @property
    def folder_layout_names(self) -> Optional[Iterator[GetLayoutsLayout]]:
        content: Optional[Iterable] = self.original_response.get('folderLayoutNames', None)
        return (GetLayoutsLayout(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetLayoutsResponse(BaseProxy):

    @property
    def layouts(self) -> Optional[Iterator[GetLayoutsLayout]]:
        content: Optional[Iterable] = self.original_response.get('layouts', None)
        return (GetLayoutsLayout(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetLayoutsResult(BaseResult):

    @property
    def response(self):
        return GetLayoutsResponse(self.original_response['response'])


@dataclass(frozen=True)
class FieldMetaData(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)

    @property
    def type(self) -> Optional[str]:
        return self.original_response.get('type', None)

    @property
    def display_type(self) -> Optional[str]:
        return self.original_response.get('displayType', None)

    @property
    def result(self) -> Optional[str]:
        return self.original_response.get('result', None)

    @property
    def global_(self) -> Optional[bool]:
        return self.original_response.get('global', None)

    @property
    def auto_enter(self) -> Optional[bool]:
        return self.original_response.get('autoEnter', None)

    @property
    def four_digit_year(self) -> Optional[bool]:
        return self.original_response.get('fourDigitYear', None)

    @property
    def max_repeat(self) -> Optional[int]:
        return self.original_response.get('maxRepeat', None)

    @property
    def max_characters(self) -> Optional[int]:
        return self.original_response.get('maxCharacters', None)

    @property
    def not_empty(self) -> Optional[bool]:
        return self.original_response.get('notEmpty', None)

    @property
    def numeric(self) -> Optional[bool]:
        return self.original_response.get('numeric', None)

    @property
    def time_of_day(self) -> Optional[bool]:
        return self.original_response.get('timeOfDay', None)

    @property
    def repetition_start(self) -> Optional[int]:
        return self.original_response.get('repetitionStart', None)

    @property
    def repetition_end(self) -> Optional[int]:
        return self.original_response.get('repetitionEnd', None)


@dataclass(frozen=True)
class PortalMetaDataItem(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)

    @property
    def type(self) -> Optional[str]:
        return self.original_response.get('type', None)

    @property
    def display_type(self) -> Optional[str]:
        return self.original_response.get('displayType', None)

    @property
    def result(self) -> Optional[str]:
        return self.original_response.get('result', None)

    @property
    def global_(self) -> Optional[bool]:
        return self.original_response.get('global', None)

    @property
    def auto_enter(self) -> Optional[bool]:
        return self.original_response.get('autoEnter', None)

    @property
    def four_digit_year(self) -> Optional[bool]:
        return self.original_response.get('fourDigitYear', None)

    @property
    def max_repeat(self) -> Optional[int]:
        return self.original_response.get('maxRepeat', None)

    @property
    def max_characters(self) -> Optional[int]:
        return self.original_response.get('maxCharacters', None)

    @property
    def not_empty(self) -> Optional[bool]:
        return self.original_response.get('notEmpty', None)

    @property
    def numeric(self) -> Optional[bool]:
        return self.original_response.get('numeric', None)

    @property
    def time_of_day(self) -> Optional[bool]:
        return self.original_response.get('timeOfDay', None)

    @property
    def repetition_start(self) -> Optional[int]:
        return self.original_response.get('repetitionStart', None)

    @property
    def repetition_end(self) -> Optional[int]:
        return self.original_response.get('repetitionEnd', None)


@dataclass(frozen=True)
class GetLayoutResponse(BaseProxy):

    @property
    def field_meta_data(self) -> Optional[Iterator[FieldMetaData]]:
        content: Optional[Iterable] = self.original_response['fieldMetaData']
        return (FieldMetaData(entry) for entry in content) if content is not None else None

    @property
    def portal_meta_data(self) -> Optional[Dict[str, Iterator[PortalMetaDataItem]]]:
        content: Optional[dict[str, Any]] = self.original_response.get('portalMetaData', None)
        return {
            key: (PortalMetaDataItem(entry) for entry in value_list)
            for key, value_list in content.items()
        } if content is not None else None


@dataclass(frozen=True)
class GetLayoutResult(BaseResult):

    @property
    def response(self):
        return GetLayoutResponse(self.original_response['response'])


@dataclass(frozen=True)
class GetScriptsScript(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.original_response.get('name', None)

    @property
    def is_folder(self) -> Optional[bool]:
        return self.original_response.get('isFolder', None)

    @property
    def folder_script_names(self) -> Optional[Iterator[GetScriptsScript]]:
        content: Optional[Iterable] = self.original_response.get('folderScriptNames', None)
        return (GetScriptsScript(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetScriptsResponse(BaseProxy):

    @property
    def scripts(self) -> Optional[Iterator[GetScriptsScript]]:
        content: Optional[Iterable] = self.original_response.get('scripts', None)
        return (GetScriptsScript(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetScriptsResult(BaseResult):

    @property
    def response(self):
        return GetScriptsResponse(self.original_response['response'])


class FileMakerErrorException(Exception):

    def __init__(self, code: int, message: str) -> None:
        super().__init__('FileMaker Server returned error {}, {}'.format(code, message))

    @staticmethod
    def from_response_message(error: Message) -> FileMakerErrorException:
        return FileMakerErrorException(code=error.code, message=error.message)
