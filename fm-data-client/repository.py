from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Optional, Iterator

from statelessfm.fmclient.cache_iterator import CacheIterator
from statelessfm.fmclient.const import FMErrorEnum
from statelessfm.fmclient.fmclient import FMClient
from statelessfm.fmclient.results import Data, CommonSearchRecordsResult, EditRecordResult, DeleteRecordResult


@dataclass(frozen=True)
class RepositoryRecord(Data):
    repository: FMRepository

    def edit_record(self, check_mod_id: bool, **kwargs):
        record_id = self.record_id
        mod_id = self.mod_id if check_mod_id else None

        return self.repository.edit_record(
            record_id=record_id,
            mod_id=mod_id,
            **kwargs
        )

    def delete_record(self, **kwargs):
        record_id = self.record_id
        return self.repository.delete_record(
            record_id=record_id,
            **kwargs)

@dataclass(frozen=True)
class RepositoryGetRecordsResponse:
    repository: FMRepository


@dataclass(frozen=True)
class Page:
    result: CommonSearchRecordsResult
    repository: FMRepository
    page_number: int


PageIterator = Iterator[Page]


def page_generator(
    repository: FMRepository,
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

            assert (remaining > 0, "remaining <= 0! This should not happen")

            limit_for_current_request = min(page_size, remaining)

        client_response = fn_get_response(
            layout=repository.layout,
            offset=offset,
            limit=limit_for_current_request,
            **kwargs
        )

        result = CommonSearchRecordsResult(client_response.original_response)
        has_messages = any(result.messages)
        if has_messages:
            message_is_record_is_missing = any(result.get_errors(include_codes=[FMErrorEnum.RECORD_IS_MISSING]))
            if message_is_record_is_missing:
                is_final_page = True
            else:
                result.raise_exception_if_has_error()

        yield Page(result=result, page_number=page_number, repository=repository)

        # Update offset and retrived for the next page
        records_retrieved += limit_for_current_request
        offset += limit_for_current_request
        page_number += 1


@dataclass(frozen=True)
class RecordGenerator:
    page_iterator: PageIterator

    def __iter__(self) -> Iterator[RepositoryRecord]:
        for page in self.page_iterator:
            for data_entry in page.result.response.data:
                yield RepositoryRecord(original_response=data_entry.original_response, repository=page.repository)


class FoundSet(CacheIterator[RepositoryRecord]):
    def __init__(self, iterator: Iterator[RepositoryRecord]):
        super().__init__(iterator)



class RepositoryCommonSearchRecordResult:
    result: CommonSearchRecordsResult = None
    pages: CacheIterator[Page] = None
    found_set: FoundSet = None

    def __init__(self, result: CommonSearchRecordsResult, pages_iterator: PageIterator):
        self.pages = CacheIterator(pages_iterator)
        self.found_set = FoundSet(RecordGenerator(self.pages.__iter__()).__iter__())


class FMRepository:
    client: FMClient = None
    layout: str = None

    def _common_search_records(
        self,
        fn_get_response: callable,
        offset: int = 1,
        page_size: Optional[int] = 100,
        limit: Optional[int] = 200,
        **kwargs
    ) -> RepositoryCommonSearchRecordResult:
        page_iterator = page_generator(
            repository=self,
            fn_get_response=fn_get_response,
            offset=offset,
            page_size=page_size,
            limit=limit,
            **kwargs
        )

        first_page = next(page_iterator)
        rebuilt_page_iterator = itertools.chain([first_page], page_iterator)

        return RepositoryCommonSearchRecordResult(result=first_page.result,
                                                  pages_iterator=rebuilt_page_iterator)

    def get_record(self,
                   offset: int = 1,
                   page_size: Optional[int] = 100,
                   limit: Optional[int] = 200,
                   **kwargs
                   ) -> RepositoryCommonSearchRecordResult:
        return self._common_search_records(
            fn_get_response=self.client.get_record,
            offset=offset,
            page_size=page_size,
            limit=limit,
            **kwargs
        )

    def get_records(self,
                    offset: int = 1,
                    page_size: Optional[int] = 100,
                    limit: Optional[int] = 200,
                    **kwargs
                    ) -> RepositoryCommonSearchRecordResult:
        return self._common_search_records(
            fn_get_response=self.client.get_records,
            offset=offset,
            page_size=page_size,
            limit=limit,
            **kwargs
        )

    def find(self,
             offset: int = 1,
             page_size: Optional[int] = 100,
             limit: Optional[int] = 200,
             **kwargs
             ) -> RepositoryCommonSearchRecordResult:
        return self._common_search_records(
            fn_get_response=self.client.find,
            offset=offset,
            page_size=page_size,
            limit=limit,
            **kwargs
        )

    def edit_record(self, **kwargs) -> EditRecordResult:
        return self.client.edit_record(
            layout=self.layout,
            **kwargs
        )

    def delete_record(self, **kwargs) -> DeleteRecordResult:
        return self.client.delete_record(
            layout=self.layout,
            **kwargs
        )


