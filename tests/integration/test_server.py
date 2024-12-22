import unittest

import fmdata
from fmdata import FMErrorEnum
from fmdata.session_providers import UsernamePasswordSessionProvider
from tests import env

STUDENT_LAYOUT = 'test_fmdata_student_layout'


class FMClientTestSuite(unittest.TestCase):
    def setUp(self) -> None:
        self.fm_client = fmdata.FMClient(
            url=env('FMS_ADDRESS'),
            database=env('FMS_DB_NAME'),
            login_provider=UsernamePasswordSessionProvider(
                username=env('FMS_DB_USER'),
                password=env('FMS_DB_PASSWORD'),
            )
        )

    def test_create_get_delete_record(self) -> None:
        result = self.fm_client.find(
            layout=STUDENT_LAYOUT,
            query=[{'FullName': 'TestRecord0'}]
        )

        result.raise_exception_if_has_error(exclude_codes=[FMErrorEnum.NO_ERROR, FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set

        if not found_set.empty:
            found_set[0].delete_record()

        result = self.fm_client.create_record(
            layout=STUDENT_LAYOUT,
            field_data={'FullName': 'TestRecord0', 'EnrollmentDate': '04.11.2017'}
        )

        result.raise_exception_if_has_error()

        record_id = result.response.record_id
        self.fm_client.delete_record(layout=STUDENT_LAYOUT, record_id=record_id)

        result = self.fm_client.find(
            layout=STUDENT_LAYOUT,
            query=[{'FullName': 'TestRecord0'}]
        )

        result.raise_exception_if_has_error(exclude_codes=[FMErrorEnum.NO_ERROR, FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set
        self.assertTrue(found_set.empty)
