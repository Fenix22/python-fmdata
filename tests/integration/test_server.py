import unittest

import fmdata
from fmdata import FMErrorEnum
from fmdata.session_providers import UsernamePasswordSessionProvider
from tests import env

STUDENT_LAYOUT = 'test_fmdata_student_layout'

fm_client = fmdata.FMClient(
    url=env('FMS_ADDRESS'),
    database=env('FMS_DB_NAME'),
    login_provider=UsernamePasswordSessionProvider(
        username=env('FMS_DB_USER'),
        password=env('FMS_DB_PASSWORD'),
    )
)

fm_client_with_student_layout = (fm_client
                                 .with_layout(STUDENT_LAYOUT)
                                 .with_api_version('v1'))


class FMClientTestSuite(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_create_get_delete_record(self) -> None:
        result = fm_client_with_student_layout.find(
            query=[{'FullName': 'TestRecord0'}]
        )

        result.raise_exception_if_has_message(exclude_codes=[FMErrorEnum.NO_ERROR, FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set

        if not found_set.empty:
            found_set[0].delete_record()

        result = fm_client_with_student_layout.create_record(
            field_data={'FullName': 'TestRecord0', 'EnrollmentDate': '04.11.2017'}
        )

        result.raise_exception_if_has_error()

        record_id = result.response.record_id
        fm_client_with_student_layout.delete_record(record_id=record_id)

        result = fm_client_with_student_layout.find(
            query=[{'FullName': 'TestRecord0'}]
        )

        # Assert that the record has been deleted
        result.raise_exception_if_has_message(exclude_codes=[FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set
        self.assertTrue(found_set.empty)
