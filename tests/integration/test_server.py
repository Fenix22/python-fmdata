from __future__ import annotations

import json
import random
import unittest
from datetime import date

from marshmallow import fields

import fmdata
from fmdata import FMErrorEnum
from fmdata.orm import Model, PortalField, PortalModel, PortalManager
from fmdata.session_providers import UsernamePasswordSessionProvider
from tests import env

STUDENT_LAYOUT = 'test_fmdata_student_layout'
CLASS_LAYOUT = 'test_fmdata_class_layout'
STUDENT_CLASS_LAYOUT = 'test_fmdata_student_class_layout'
EXAM_LAYOUT = 'test_fmdata_exam_layout'
STUDENT_EXAM_LAYOUT = 'test_fmdata_student_exam_layout'

fm_client = fmdata.FMClient(
    url=env('FMS_ADDRESS'),
    database=env('FMS_DB_NAME'),
    login_provider=UsernamePasswordSessionProvider(
        username=env('FMS_DB_USER'),
        password=env('FMS_DB_PASSWORD'),
    )
)

student_layout = fm_client

class_layout = fm_client

student_class_layout = fm_client

exam_layout = fm_client

student_exam_layout = fm_client


class EmptyStringToNoneInteger(fields.Integer):
    def _deserialize(self, value, attr, data, **kwargs):
        if value == '':
            return None
        return super()._deserialize(value, attr, data, **kwargs)


class ClassPortal(PortalModel):
    name = fields.Str(required=False, data_key="test_fmdata_class_1::Name")
    description = fields.Str(required=False, data_key="test_fmdata_class_1::Description")


class BaseBase():
    full_name = fields.Str(data_key="FullName")


class BaseStudent(BaseBase):
    GraduationYear = EmptyStringToNoneInteger(as_string=True, allow_none=True)
    test_fmdata_class_1 = PortalField(model=ClassPortal, name="test_fmdata_class_1")


class Student(Model, BaseStudent):
    class Meta:
        client = fm_client
        layout = 'test_fmdata_student_layout'

    pk = fields.Str(data_key="PrimaryKey")
    enrollment_date = fields.Date(data_key="EnrollmentDate")


class FMClientTestSuite(unittest.TestCase):
    def test_reset_db(self):
        student_layout.find(query=[{"PrimaryKey": "*"}]).raise_exception_if_has_error().found_set.delete_all_records()
        class_layout.find(query=[{"PrimaryKey": "*"}]).raise_exception_if_has_error().found_set.delete_all_records()
        exam_layout.find(query=[{"PrimaryKey": "*"}]).raise_exception_if_has_error().found_set.delete_all_records()
        student_class_layout.find(
            query=[{"PrimaryKey": "*"}]).raise_exception_if_has_error().found_set.delete_all_records()
        exam_layout.find(query=[{"PrimaryKey": "*"}]).raise_exception_if_has_error().found_set.delete_all_records()

    def action(self, i):
        result_set = Student.objects.order_by("pk").find(full_name__raw="*")
        for item in result_set:
            print(i, item.pk, item.full_name)

    def test_0(self):
        result_set = (Student.objects.order_by("pk")
                      .find(full_name__raw="*")
                      .chunk_size(100)
                      .prefetch_portal("test_fmdata_class_1", limit=100))

        for item in result_set:
            print("item", item.record_id, item.pk, item.full_name)
            portals_class_1: PortalManager = item.test_fmdata_class_1.only_prefetched()

            for index, portal in enumerate(portals_class_1):
                print("portal", portal.record_id, portal.mod_id, portal.name, portal.description)

                if index > 5:
                    print("delete portal")
                    portal.delete()
                else:
                    portal.description = portal.description + "."
                    portal.save()

            random_int = random.randint(1, 9999)
            item.test_fmdata_class_1.create(name="APA" + str(random_int), description="AIA")
            item.full_name = item.full_name + "."

            first_entry = item.test_fmdata_class_1.avoid_prefetch_cache().first()
            first_entry.description = first_entry.description + "|"
            print("new portal:", first_entry.record_id, first_entry.mod_id, first_entry.name, first_entry.description)

            item.save(portals=([first_entry]))

        random_year = random.randint(2050, 999999)
        for i in range(10):
            student = Student.objects.create(full_name="Test" + str(i),
                                             enrollment_date=date(2024, 5, 18),
                                             GraduationYear=random_year, )

            student.refresh_from_db()
            print(student.pk, student.full_name)

        all_ordered = Student.objects.find(GraduationYear=random_year).order_by("full_name").chunk_size(1000)

        all_ordered[0:5].delete()

        print(" ")
        for item in all_ordered:
            print(item.pk, item.full_name)

        all_ordered.delete()

    def test_fill_db(self):
        # Create classes
        class_layout.create_record(field_data={
            "Name": "APA",
            "StartDate": "11/01/2024 08:00",
            "EndDate": "11/03/2024 13:00",
        })

        class_layout.create_record(field_data={
            "Name": "ELO",
            "StartDate": "01/01/2024 08:00",
            "EndDate": "12/06/2024 16:00",
        })

        # Create Students
        lorenzo_id = student_layout.create_record(field_data={
            "FullName": "Lorenzo",
            "EnrollmentDate": "01/01/2024",
        }).response.record_id

        alph_id = student_layout.create_record(field_data={
            "FullName": "Alph",
            "EnrollmentDate": "10/03/2024",
        }).response.record_id

        # Lorenzo join class
        student_class_layout.create_record(field_data={
            "ClassName": "APA",
            "StudentId": lorenzo_id,
        })

        student_class_layout.create_record(field_data={
            "ClassName": "ELO",
            "StudentId": lorenzo_id,
        })

        # Alph join class
        student_class_layout.create_record(field_data={
            "ClassName": "APA",
            "StudentId": alph_id,
        })

        # Create exam
        apa_winter_2024_id = exam_layout.create_record(field_data={
            "ClassName": "APA",
            "Name": "APA Winter 2024",
            "Date": "18/02/2024 08:00",
        }).response.record_id

        apa_summer_2024_id = exam_layout.create_record(field_data={
            "ClassName": "APA",
            "Name": "APA Summer 2024",
            "Date": "18/07/2024 08:00",
        }).response.record_id

        elo_winter_2024_id = exam_layout.create_record(field_data={
            "ClassName": "ELO",
            "Name": "ELO Winter 2024",
            "Date": "17/02/2024 08:00",
        }).response.record_id

        elo_summer_2024_id = exam_layout.create_record(field_data={
            "ClassName": "ELO",
            "Name": "ELO Summer 2024",
            "Date": "18/07/2024 08:00",
        }).response.record_id

        # Lorenzo join winter exams
        student_exam_layout.create_record(field_data={
            "ExamId": apa_winter_2024_id,
            "StudentId": lorenzo_id,
        })

        student_exam_layout.create_record(field_data={
            "ExamId": elo_winter_2024_id,
            "StudentId": lorenzo_id,
        })

        # Alph join summer exam
        student_exam_layout.create_record(field_data={
            "ExamId": apa_summer_2024_id,
            "StudentId": alph_id,
        })

    def test(self):
        result = class_layout.find(
            query=[{'Name': 'APA'}]
        )

        print(json.dumps(result.raw_content, indent=2))

        result = student_layout.find(
            query=[{'FullName': 'Lorenzo'}]
        )

        print(json.dumps(result.raw_content, indent=2))

        result = exam_layout.find(
            query=[{'PrimaryKey': '=443A8DD6-C254-4A38-9D70-1FB9731BBB9E'}]
        )

        print(json.dumps(result.raw_content, indent=2))

    def test_create_get_delete_record(self) -> None:
        result = student_layout.find(
            query=[{'FullName': 'TestRecord0'}]
        )

        result.raise_exception_if_has_message(
            exclude_codes=[FMErrorEnum.NO_ERROR, FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set

        if not found_set.empty:
            found_set[0].delete_record()

        result = student_layout.create_record(
            field_data={'FullName': 'TestRecord0', 'EnrollmentDate': '04.11.2017'}
        )

        result.raise_exception_if_has_error()

        record_id = result.response.record_id
        student_layout.delete_record(record_id=record_id)

        result = student_layout.find(
            query=[{'FullName': 'TestRecord0'}]
        )

        # Assert that the record has been deleted
        result.raise_exception_if_has_message(exclude_codes=[FMErrorEnum.NO_RECORDS_MATCH_REQUEST])

        found_set = result.found_set
        self.assertTrue(found_set.empty)
