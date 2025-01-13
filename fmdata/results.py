from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Dict, Any, Iterator, Iterable, Set

from fmdata.cache_iterator import CacheIterator
from fmdata.const import FMErrorEnum


def optional_list(iterator: Optional[Iterator]) -> Optional[List]:
    return list(iterator) if iterator is not None else None


@dataclass(frozen=True)
class BaseProxy:
    raw_content: Dict[str, Any]


@dataclass(frozen=True)
class Message(BaseProxy):

    @property
    def code(self) -> Optional[int]:
        return self.raw_content.get('code', None)

    @property
    def message(self) -> Optional[str]:
        return self.raw_content.get('message', None)


def _get_int(input: FMErrorEnum | int):
    if isinstance(input, FMErrorEnum):
        return input.value
    return input


@dataclass(frozen=True)
class BaseResult(BaseProxy):

    def _message_codes_that_are_not_considered_errors(self) -> List[int]:
        return [FMErrorEnum.NO_ERROR.value]

    @cached_property
    def messages(self) -> List[Message]:
        return list(self.messages_iterator)

    @property
    def messages_iterator(self) -> Iterator[Message]:
        return (Message(msg) for msg in self.raw_content['messages'])

    def get_messages_iterator(self,
                              search_codes: Optional[List[FMErrorEnum | int]] = None,
                              exclude_codes: Optional[List[FMErrorEnum | int]] = None
                              ) -> Iterator[Message]:
        int_search_codes = [_get_int(code) for code in search_codes] if search_codes is not None else None
        int_exclude_codes = [_get_int(code) for code in exclude_codes] if exclude_codes is not None else None

        return (
            msg for msg in self.messages
            if (int_exclude_codes is None or (int(msg.code) not in int_exclude_codes))
               and (
                       int_search_codes is None or (int(msg.code) in int_search_codes)
               )
        )

    def raise_exception_if_has_message(self,
                                       include_codes: Optional[List[FMErrorEnum | int]] = None,
                                       exclude_codes: Optional[List[FMErrorEnum | int]] = None
                                       ) -> None:
        error = next(self.get_messages_iterator(search_codes=include_codes, exclude_codes=exclude_codes), None)

        if error is not None:
            raise FileMakerErrorException(code=error.code, message=error.message)

    @cached_property
    def errors(self) -> List[Message]:
        return list(self.get_messages_iterator(exclude_codes=self._message_codes_that_are_not_considered_errors()))

    def raise_exception_if_has_error(self) -> None:
        self.raise_exception_if_has_message(exclude_codes=self._message_codes_that_are_not_considered_errors())


@dataclass(frozen=True)
class LogoutResult(BaseResult, BaseProxy):
    pass


@dataclass(frozen=True)
class ScriptResponse(BaseProxy):

    @property
    def after_script_result(self) -> Optional[str]:
        return self.raw_content.get('scriptResult', None)

    @property
    def after_script_error(self) -> Optional[str]:
        return self.raw_content.get('scriptError', None)

    @property
    def prerequest_script_result(self) -> Optional[str]:
        return self.raw_content.get('scriptResult.prerequest', None)

    @property
    def prerequest_script_error(self) -> Optional[str]:
        return self.raw_content.get('scriptError.prerequest', None)

    @property
    def presort_script_result(self) -> Optional[str]:
        return self.raw_content.get('scriptResult.presort', None)

    @property
    def presort_script_error(self) -> Optional[str]:
        return self.raw_content.get('scriptError.presort', None)


@dataclass(frozen=True)
class ScriptResult(BaseResult):

    @property
    def response(self):
        return ScriptResponse(self.raw_content['response'])


@dataclass(frozen=True)
class PortalDataInfo(BaseProxy):

    @property
    def database(self) -> Optional[str]:
        return self.raw_content.get('database', None)

    @property
    def table(self) -> Optional[str]:
        return self.raw_content.get('table', None)

    @property
    def found_count(self) -> Optional[int]:
        return self.raw_content.get('foundCount', None)

    @property
    def returned_count(self) -> Optional[int]:
        return self.raw_content.get('returnedCount', None)

    @property
    def portal_object_name(self) -> Optional[str]:
        return self.raw_content.get('portalObjectName', None)


@dataclass(frozen=True)
class PortalData(BaseProxy):
    portal_data_list: PortalDataList

    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key, None)

    def get(self, key: str, default: Optional[str] = None, table_name: Optional[str] = None) -> Optional[str]:
        if table_name is None:
            table_name = self.portal_data_list.table_name

        if table_name is None:
            raise ValueError("Cannot auto detect table name, please provide it .get(key, default, table_name)")

        real_key = table_name + "::" + key

        return self.raw_content.get(real_key, default)

    def calculate_table_name(self) -> Optional[str]:
        for key in self.raw_content:
            if '::' in key:
                return key.split('::', 1)[0]

        return None

    @property
    def record_id(self) -> Optional[str]:
        return self.raw_content.get('recordId', None)

    @property
    def mod_id(self) -> Optional[str]:
        return self.raw_content.get('modId', None)


class PortalDataList(CacheIterator[PortalData]):

    def __init__(self, portal_name: str, iterator: Iterator[Dict[str, Any]]) -> None:
        self.portal_name: str = portal_name
        super().__init__(iterator=(PortalData(raw_content=entry, portal_data_list=self) for entry in iterator))

    def __getitem__(self, index: int) -> PortalData:
        return super().__getitem__(index)

    def __iter__(self) -> Iterator[PortalData]:
        return super().__iter__()

    @cached_property
    def table_name(self):
        first_element = next(self.__iter__(), None)
        if first_element is not None:
            return first_element.calculate_table_name()

        return None


@dataclass(frozen=True)
class DataInfo(BaseProxy):

    @property
    def database(self) -> Optional[str]:
        return self.raw_content.get('database', None)

    @property
    def layout(self) -> Optional[str]:
        return self.raw_content.get('layout', None)

    @property
    def table(self) -> Optional[str]:
        return self.raw_content.get('table', None)

    @property
    def total_record_count(self) -> Optional[str]:
        return self.raw_content.get('totalRecordCount', None)

    @property
    def found_count(self) -> Optional[int]:
        return self.raw_content.get('foundCount', None)

    @property
    def returned_count(self) -> Optional[int]:
        return self.raw_content.get('returnedCount', None)


@dataclass(frozen=True)
class Data(BaseProxy):

    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key, None)

    def get(self, key: str, default: Optional[str] = None) -> str:
        return self.field_data.get(key, default)

    @property
    def field_data(self) -> Dict[str, Any]:
        return self.raw_content['fieldData']

    @property
    def record_id(self) -> str:
        return self.raw_content.get('recordId')

    @property
    def mod_id(self) -> Optional[str]:
        return self.raw_content.get('modId', None)

    @cached_property
    def portal_data_info(self) -> Optional[List[PortalDataInfo]]:
        return optional_list(self.portal_data_info_iterator)

    @property
    def portal_data_info_iterator(self) -> Optional[Iterator[PortalDataInfo]]:
        portal_data_info_list: Optional[List[Dict[str, Any]]] = self.raw_content.get('portalDataInfo', None)
        return (PortalDataInfo(portal_data_info) for portal_data_info in
                portal_data_info_list) if portal_data_info_list is not None else None

    @cached_property
    def portal_data(self) -> Optional[Dict[str, PortalDataList]]:
        portal_data: Optional[Dict[str, Any]] = self.raw_content.get('portalData', None)
        return {
            key: PortalDataList(portal_name=key, iterator=(for _ in value))
            for key, value in portal_data.items()
        } if portal_data is not None else None


@dataclass(frozen=True)
class CommonSearchRecordsResponseField(ScriptResponse):

    @property
    def data_info(self) -> Optional[DataInfo]:
        data_info: Optional[Dict[str, Any]] = self.raw_content.get('dataInfo', None)
        return DataInfo(data_info) if data_info is not None else None

    @cached_property
    def data(self) -> Optional[List[Data]]:
        content: Optional[Iterable] = self.raw_content.get('data', None)
        return [Data(record) for record in content] if content is not None else None

    @property
    def data_iterator(self) -> Optional[Iterator[Data]]:
        content: Optional[Iterable] = self.raw_content.get('data', None)
        return (Data(record) for record in content) if content is not None else None


@dataclass(frozen=True)
class CommonSearchRecordsResult(BaseResult):
    layout: str
    client: object

    @property
    def response(self):
        return CommonSearchRecordsResponseField(self.raw_content['response'])

    @cached_property
    def found_set(self):
        if self.response.data_iterator is None:
            return FoundSet(iter([]))

        return FoundSet(records_iterator_from_common_search_result(self))


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
class PaginatedRecordResult:
    pages: CacheIterator[Page]

    @cached_property
    def found_set(self):
        return FoundSet(records_iterator_from_page_iterator(self.pages.__iter__()).__iter__())


class GetRecordsPaginatedResult(PaginatedRecordResult):
    pass


class FindPaginatedResult(PaginatedRecordResult):
    pass


@dataclass(frozen=True)
class CreateRecordResponse(BaseProxy):

    @property
    def mod_id(self) -> str:
        return self.raw_content['modId']

    @property
    def record_id(self) -> str:
        return self.raw_content['recordId']


@dataclass(frozen=True)
class CreateRecordResult(BaseResult):

    @property
    def response(self):
        return CreateRecordResponse(raw_content=self.raw_content['response'])


@dataclass(frozen=True)
class DuplicateRecordResponse(BaseProxy):

    @property
    def mod_id(self) -> str:
        return self.raw_content['modId']

    @property
    def record_id(self) -> str:
        return self.raw_content['recordId']


@dataclass(frozen=True)
class DuplicateRecordResult(BaseResult):

    @property
    def response(self):
        return DuplicateRecordResponse(raw_content=self.raw_content['response'])


@dataclass(frozen=True)
class EditRecordResponse(BaseProxy):

    @property
    def mod_id(self) -> str:
        return self.raw_content['modId']


@dataclass(frozen=True)
class EditRecordResult(BaseResult):

    @property
    def response(self):
        return EditRecordResponse(raw_content=self.raw_content['response'])


@dataclass(frozen=True)
class DeleteRecordResult(BaseResult):
    pass


@dataclass(frozen=True)
class LoginResponse(BaseProxy):

    @property
    def token(self) -> str:
        return self.raw_content['token']


@dataclass(frozen=True)
class LoginResult(BaseResult):

    @property
    def response(self):
        return LoginResponse(self.raw_content['response'])


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
        return self.raw_content.get('name', None)

    @property
    def build_date(self) -> Optional[str]:
        return self.raw_content.get('buildDate', None)

    @property
    def version(self) -> Optional[str]:
        return self.raw_content.get('version', None)

    @property
    def date_format(self) -> Optional[str]:
        return self.raw_content.get('dateFormat', None)

    @property
    def time_format(self) -> Optional[str]:
        return self.raw_content.get('timeFormat', None)

    @property
    def time_stamp_format(self) -> Optional[str]:
        return self.raw_content.get('timeStampFormat', None)


@dataclass(frozen=True)
class GetProductInfoResult(BaseResult):

    @property
    def response(self):
        return GetProductInfoResponse(self.raw_content['response'])


@dataclass(frozen=True)
class Database(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.raw_content.get('name', None)


@dataclass(frozen=True)
class GetDatabasesResponse(BaseProxy):

    @cached_property
    def databases(self) -> Optional[List[Database]]:
        return optional_list(self.databases_iterator)

    @property
    def databases_iterator(self) -> Optional[Iterator[Database]]:
        content: Optional[Iterable] = self.raw_content.get('databases', None)
        return (Database(database) for database in content) if content is not None else None


@dataclass(frozen=True)
class GetDatabasesResult(BaseResult):

    @property
    def response(self):
        return GetDatabasesResponse(self.raw_content['response'])


@dataclass(frozen=True)
class GetLayoutsLayout(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.raw_content.get('name', None)

    @property
    def is_folder(self) -> Optional[bool]:
        return self.raw_content.get('isFolder', None)

    @cached_property
    def folder_layout_names(self) -> Optional[List[GetLayoutsLayout]]:
        return optional_list(self.folder_layout_names_iterator)

    @property
    def folder_layout_names_iterator(self) -> Optional[Iterator[GetLayoutsLayout]]:
        content: Optional[Iterable] = self.raw_content.get('folderLayoutNames', None)
        return (GetLayoutsLayout(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetLayoutsResponse(BaseProxy):

    @cached_property
    def layouts(self) -> Optional[List[GetLayoutsLayout]]:
        return optional_list(self.layouts_iterator)

    @property
    def layouts_iterator(self) -> Optional[Iterator[GetLayoutsLayout]]:
        content: Optional[Iterable] = self.raw_content.get('layouts', None)
        return (GetLayoutsLayout(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetLayoutsResult(BaseResult):

    @property
    def response(self):
        return GetLayoutsResponse(self.raw_content['response'])


@dataclass(frozen=True)
class FieldMetaData(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.raw_content.get('name', None)

    @property
    def type(self) -> Optional[str]:
        return self.raw_content.get('type', None)

    @property
    def display_type(self) -> Optional[str]:
        return self.raw_content.get('displayType', None)

    @property
    def result(self) -> Optional[str]:
        return self.raw_content.get('result', None)

    @property
    def global_(self) -> Optional[bool]:
        return self.raw_content.get('global', None)

    @property
    def auto_enter(self) -> Optional[bool]:
        return self.raw_content.get('autoEnter', None)

    @property
    def four_digit_year(self) -> Optional[bool]:
        return self.raw_content.get('fourDigitYear', None)

    @property
    def max_repeat(self) -> Optional[int]:
        return self.raw_content.get('maxRepeat', None)

    @property
    def max_characters(self) -> Optional[int]:
        return self.raw_content.get('maxCharacters', None)

    @property
    def not_empty(self) -> Optional[bool]:
        return self.raw_content.get('notEmpty', None)

    @property
    def numeric(self) -> Optional[bool]:
        return self.raw_content.get('numeric', None)

    @property
    def time_of_day(self) -> Optional[bool]:
        return self.raw_content.get('timeOfDay', None)

    @property
    def repetition_start(self) -> Optional[int]:
        return self.raw_content.get('repetitionStart', None)

    @property
    def repetition_end(self) -> Optional[int]:
        return self.raw_content.get('repetitionEnd', None)


@dataclass(frozen=True)
class PortalFieldMetaData(FieldMetaData):
    pass


@dataclass(frozen=True)
class GetLayoutResponse(BaseProxy):

    @cached_property
    def field_meta_data(self) -> Optional[List[FieldMetaData]]:
        return optional_list(self.field_meta_data_iterator)

    @property
    def field_meta_data_iterator(self) -> Optional[Iterator[FieldMetaData]]:
        content: Optional[Iterable] = self.raw_content['fieldMetaData']
        return (FieldMetaData(entry) for entry in content) if content is not None else None

    @cached_property
    def portal_meta_data(self) -> Optional[Dict[str, List[PortalFieldMetaData]]]:
        content: Optional[Dict[str, Any]] = self.raw_content.get('portalMetaData', None)
        return {
            key: (PortalFieldMetaData(entry) for entry in value_list)
            for key, value_list in content.items()
        } if content is not None else None

    @property
    def portal_meta_data_iterator(self) -> Optional[Dict[str, Iterator[PortalFieldMetaData]]]:
        content: Optional[Dict[str, Any]] = self.raw_content.get('portalMetaData', None)
        return {
            key: [PortalFieldMetaData(entry) for entry in value_list]
            for key, value_list in content.items()
        } if content is not None else None


@dataclass(frozen=True)
class GetLayoutResult(BaseResult):

    @property
    def response(self):
        return GetLayoutResponse(self.raw_content['response'])


@dataclass(frozen=True)
class GetScriptsScript(BaseProxy):

    @property
    def name(self) -> Optional[str]:
        return self.raw_content.get('name', None)

    @property
    def is_folder(self) -> Optional[bool]:
        return self.raw_content.get('isFolder', None)

    @cached_property
    def folder_script_names(self) -> Optional[List[GetScriptsScript]]:
        return optional_list(self.folder_script_names_iterator)

    @property
    def folder_script_names_iterator(self) -> Optional[Iterator[GetScriptsScript]]:
        content: Optional[Iterable] = self.raw_content.get('folderScriptNames', None)
        return (GetScriptsScript(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetScriptsResponse(BaseProxy):

    @property
    def scripts(self) -> Optional[Iterator[GetScriptsScript]]:
        content: Optional[Iterable] = self.raw_content.get('scripts', None)
        return (GetScriptsScript(entry) for entry in content) if content is not None else None


@dataclass(frozen=True)
class GetScriptsResult(BaseResult):

    @property
    def response(self):
        return GetScriptsResponse(self.raw_content['response'])


class FileMakerErrorException(Exception):

    def __init__(self, code: int, message: str) -> None:
        super().__init__('FileMaker Server returned error {}, {}'.format(code, message))

    @staticmethod
    def from_response_message(error: Message) -> FileMakerErrorException:
        return FileMakerErrorException(code=error.code, message=error.message)


class MutablePortalRecord:

    def __init__(self, portal_records: MutablePortalRecords, portal_data: PortalData):
        self.portal_records: MutablePortalRecords = portal_records
        self.portal_data: PortalData = portal_data

        self.mod_id = self.portal_data.mod_id
        self.field_changes: Dict[str, Any] = {}
        self._to_be_deleted: bool = False

    def __getitem__(self, key: str) -> Optional[str]:
        return self.get(key, None)

    def get(self, key: str, default: Optional[str] = None, table_name: Optional[str] = None) -> Optional[str]:
        if table_name is None:
            table_name = self.table_name

        if table_name is None:
            raise ValueError("Cannot auto detect table name, please provide it .get(key, default, table_name)")

        real_key = table_name + "::" + key

        return self.portal_data.raw_content.get(real_key, self.field_changes.get(real_key, default))

    def __setitem__(self, key: str, value: Any) -> None:
        return self.set(key, value)

    def set(self, key: str, value: Any, table_name: Optional[str] = None) -> None:
        if table_name is None:
            table_name = self.table_name

        if table_name is None:
            raise ValueError("Cannot auto detect table name, please provide it .set(key, value, table_name)")

        self._get_or_init_changes()
        self.field_changes[table_name + "::" + key] = value

    def delete(self, table_name: Optional[str] = None):
        if table_name is None:
            table_name = self.table_name

        if table_name is None:
            raise ValueError("Cannot auto detect table name, please provide it .delete(table_name)")

        self._get_or_init_changes()
        self._to_be_deleted = True

    def _get_or_init_changes(self) -> None:
        self._record.portal_changes.add(self)

    @property
    def to_be_deleted(self) -> bool:
        return self._to_be_deleted

    @property
    def _record(self) -> MutableRecord:
        return self.portal_records.record

    @property
    def record_id(self) -> str:
        return self.portal_data.record_id

    @property
    def table_name(self):
        return self.portal_records.table_name

    @property
    def portal_name(self):
        return self.portal_records.portal_name


class MutablePortalRecords(List[MutablePortalRecord]):
    def __init__(self, record: MutableRecord, portal_data_list: PortalDataList, portal_name: str):
        super().__init__(
            (MutablePortalRecord(portal_records=self, portal_data=portal_data) for portal_data in portal_data_list))
        self.record = record
        self.portal_data_list = portal_data_list
        self.portal_name = portal_name

    @property
    def table_name(self):
        return self.portal_data_list.table_name


class MutableRecord:
    def __init__(self, initial_record: Record):
        self.initial_record = initial_record

        self.mod_id = self.initial_record.mod_id
        self.field_changes: Dict[str, Any] = {}
        self.portal_changes: Set[MutablePortalRecord] = set()

    def commit(self, check_mod_id: bool = True, **kwargs):
        if not self.is_dirty:
            return

        #TODO sbagliato, dobbiamo farlo di tutti i portali perchè dobbiamo sempre checkare il modId
        portal_data = {
            portal.portal_name: {
                "recordId": portal.record_id,
                "modId": portal.mod_id if check_mod_id else None,
                **portal.field_changes,
            }
            for portal in self.portal_changes
        }

        portals_records_to_be_deleted = [portal_record
                                         for portal_record in self.portal_changes if portal_record.to_be_deleted]
        related_records_to_be_deleted = [portal_record.table_name + "." + portal_record.record_id
                                         for portal_record in portals_records_to_be_deleted]

        field_data = {
            **self.field_changes,
            "deleteRelated": related_records_to_be_deleted
        }

        from fmdata import FMClient
        fm_client: FMClient = self.client

        result = fm_client.edit_record(
            record_id=self.record_id,
            mod_id=self.mod_id if check_mod_id else None,
            field_data=field_data,
            portal_data=portal_data,
            **kwargs
        )

        result.raise_exception_if_has_error()
        self.mod_id = result.response.mod_id

        for portal_record in self.portal_changes:
            portal_record._to_be_deleted = False

        self.field_changes.clear()
        self.portal_changes.clear()

    def __getitem__(self, key: str) -> str:
        return self.get(key, None)

    def get(self, key: str, default: Optional[str] = None) -> str:
        return self.field_changes.get(key, self.initial_record.get(key, default))

    def __setitem__(self, key: str, value: Any) -> None:
        self.field_changes[key] = value

    @property
    def is_dirty(self) -> bool:
        return len(self._changes) > 0

    @property
    def record_id(self) -> str:
        return self.initial_record.record_id

    @cached_property
    def portals(self):
        return {
            key: MutablePortalRecords(record=self, portal_data_list=value, portal_name=key)
            for key, value in self.initial_record.portals.items()
        }


@dataclass(frozen=True)
class Record(Data):
    client: object
    layout: str

    @cached_property
    def mutable_instance(self):
        return MutableRecord(self)

    def edit_record(self, check_mod_id: bool = False, **kwargs):
        mod_id = self.mod_id if check_mod_id else None

        from fmdata import FMClient
        fm_client: FMClient = self.client

        return fm_client.edit_record(
            layout=self.layout,
            record_id=self.record_id,
            mod_id=mod_id,
            **kwargs
        )

    def duplicate_record(self, **kwargs):
        from fmdata import FMClient
        fm_client: FMClient = self.client

        return fm_client.duplicate_record(
            layout=self.layout,
            record_id=self.record_id,
            **kwargs)

    def delete_record(self, **kwargs):
        from fmdata import FMClient
        fm_client: FMClient = self.client

        return fm_client.delete_record(
            layout=self.layout,
            record_id=self.record_id,
            **kwargs)

    @property
    def portals(self) -> Optional[Dict[str, PortalDataList]]:
        return self.portal_data if self.portal_data is not None else {}


class FoundSet(CacheIterator[Record]):
    def __init__(self, iterator: Iterator[Record]):
        super().__init__(iterator)

    def __getitem__(self, index: int) -> Record:
        return super().__getitem__(index)

    def __iter__(self) -> Iterator[Record]:
        return super().__iter__()

    def edit_records(self, check_mod_id: bool = False, limit: Optional[int] = None, **kwargs):
        count = 0
        for record in self:
            if limit is not None and count >= limit:
                break

            record.edit_record(check_mod_id=check_mod_id, **kwargs)
            count += 1

    def delete_records(self, limit: Optional[int] = None, **kwargs):
        count = 0
        for record in self:
            if limit is not None and count >= limit:
                break

            record.delete_record(**kwargs)
            count += 1


@dataclass(frozen=True)
class Page:
    result: CommonSearchRecordsResult


PageIterator = Iterator[Page]


def records_iterator_from_common_search_result(
        result: CommonSearchRecordsResult,
) -> Iterator[Record]:
    for data_entry in result.response.data:
        yield Record(
            data=data_entry,
            client=result.client,
            layout=result.layout
        )


def records_iterator_from_page_iterator(page_iterator: PageIterator) -> Iterator[Record]:
    for page in page_iterator:
        yield from records_iterator_from_common_search_result(
            result=page.result,
        )
