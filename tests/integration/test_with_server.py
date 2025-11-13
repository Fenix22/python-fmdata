from __future__ import annotations

import json
import logging
import os
import random
import unittest
from dataclasses import asdict
from datetime import date, time, datetime
from decimal import Decimal as PythonDecimal

import fmdata
from fmdata import FMFieldType, fmd_fields
from fmdata.orm import (
    Model,
    PortalField,
    PortalModel,
)
from fmdata.results import FieldMetaData
from fmdata.session_providers import UsernamePasswordSessionProvider
from tests import env

fmd_logger = logging.getLogger("fmdata")
fmd_logger.setLevel(logging.DEBUG)

# Add a console handler
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
fmd_logger.addHandler(handler)

PERSON_LAYOUT = "person"
ADDRESS_PORTAL_NAME = "person_addresses"

# Build a client using test env (tests/.env or process env)
fm_client = fmdata.FMClient(
    url=env("FMS_ADDRESS"),
    database=env("FMS_DB_NAME"),
    login_provider=UsernamePasswordSessionProvider(
        username=env("FMS_DB_USER"),
        password=env("FMS_DB_PASSWORD"),
    ),
    filemaker_version=env("FMS_VERSION"),
)


def error_if_no_env_server(f):
    """Return unittest.skipIf condition for when server env is not set."""
    required = ["FMS_ADDRESS", "FMS_DB_NAME", "FMS_DB_USER", "FMS_DB_PASSWORD"]
    missing = [k for k in required if not os.getenv(k) and not env(k)]

    if len(missing) > 0:
        raise ValueError(f"FM Server env not configured: missing {missing}")

    return f


# --------------------------------------------------------------------------------------
# Portal models
# --------------------------------------------------------------------------------------
class Place(PortalModel):
    street = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::Street", field_type=FMFieldType.Text)


class AddressPortal(Place):
    city = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::City", field_type=FMFieldType.Text)
    zip = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::Zip", field_type=FMFieldType.Text)
    code = fmd_fields.Integer(field_name=f"{ADDRESS_PORTAL_NAME}::Code", field_type=FMFieldType.Number)
    zone = fmd_fields.Integer(field_name=f"{ADDRESS_PORTAL_NAME}::Zone", field_type=FMFieldType.Text)
    reviewed_at = fmd_fields.DateTime(field_name=f"{ADDRESS_PORTAL_NAME}::ReviewedAt", field_type=FMFieldType.Text)


# --------------------------------------------------------------------------------------
# Main model using the fmdata ORM + our portal
# --------------------------------------------------------------------------------------

# Fake layout only to check that the Metaclass is working (so we inherit client and we override layout)
class LivingBeing(Model):
    class Meta:
        client = fm_client
        layout = 'living_being'

    full_name = fmd_fields.String(field_name="FullName", field_type=FMFieldType.Text)


class Person(LivingBeing):
    class Meta:
        layout = PERSON_LAYOUT

    pk = fmd_fields.String(field_name="PrimaryKey", field_type=FMFieldType.Text)
    birth_date = fmd_fields.Date(field_name="BirthDate", field_type=FMFieldType.Date)
    wakes_at = fmd_fields.Time(field_name="WakesUpAt", field_type=FMFieldType.Time)
    Score = fmd_fields.Float(field_name="Score", field_type=FMFieldType.Number)
    avg_time = fmd_fields.Decimal(field_name="AvgTime", field_type=FMFieldType.Number)
    is_active = fmd_fields.Bool(field_name="IsActive", field_type=FMFieldType.Number)
    id_card_file = fmd_fields.Container(field_name="IDCardFile", field_type=FMFieldType.Container)

    addresses = PortalField(model=AddressPortal, name=ADDRESS_PORTAL_NAME)


# --------------------------------------------------------------------------------------
# Integration tests for ORM + portals (server required)
# --------------------------------------------------------------------------------------
@error_if_no_env_server
class ORMIntegrationTests(unittest.TestCase):

    # TODO test on empty data (in general with fm errors)
    # TODO delete portal using the manager

    def test_get_product_info(self):
        response = fm_client.get_product_info()
        print(json.dumps(asdict(response.response)))
        response.raise_exception_if_has_error()

    def test_get_databases(self):
        result = fm_client.get_databases(username=env("FMS_DB_USER"), password=env("FMS_DB_PASSWORD"))
        print(json.dumps(asdict(result.response)))
        result.raise_exception_if_has_error()

        for layout in result.response.databases:
            data = {
                "name": layout.name,
            }

            print(json.dumps(data))

            # Assert no none
            for key in data:
                self.assertNotEquals(data[key], None)

    def test_get_layouts(self):
        result = fm_client.get_layouts()
        result.raise_exception_if_has_error()
        print(json.dumps(asdict(result.response)))

        for layout in result.response.layouts:
            data = {
                "name": layout.name,
                # "table": layout.table,
            }

            print("layout_name:", layout.name, "- table:", layout.table)

            # Assert no none
            for key in data:
                self.assertNotEquals(data[key], None)

            layout_result = fm_client.get_layout(layout=layout.name)
            print(json.dumps(asdict(layout_result.response)))
            layout_result.raise_exception_if_has_error()

            for field_meta in layout_result.response.field_meta_data:
                self.check_field_meta(field_meta)

            for portal_name, portal_field_meta in layout_result.response.portal_meta_data.items():
                print("portal_name:", portal_name)

                self.assertNotEquals(portal_name, None)

                for portal_field_meta_item in portal_field_meta:
                    self.check_field_meta(portal_field_meta_item)

    def check_field_meta(self, field_meta: FieldMetaData):
        data = {
            "field_name": field_meta.name,
            "field_type": field_meta.type,
            "display_type": field_meta.display_type,
            "result": field_meta.result,
            "global_": field_meta.global_,
            "auto_enter": field_meta.auto_enter,
            "four_digit_year": field_meta.four_digit_year,
            "max_repeat": field_meta.max_repeat,
            "max_characters": field_meta.max_characters,
            "not_empty": field_meta.not_empty,
            "numeric": field_meta.numeric,
            "time_of_day": field_meta.time_of_day,
            "repetition_start": field_meta.repetition_start,
            "repetition_end": field_meta.repetition_end,
        }

        print(json.dumps(data))

        # Assert no none
        for key in data:
            self.assertNotEquals(data[key], None)

    def test_get_scripts(self):
        response = fm_client.get_scripts()
        response.raise_exception_if_has_error()
        print(json.dumps(asdict(response.response)))

    def test_create_find_update_delete_with_portals(self):
        # Create several people with mixed data
        cohort_tag = "ctag-" + str(random.randint(1000, 999999))

        # ---- Phase 0 ----
        # Delete waste form old test execution: all Test People with same cohort tag
        old_persons = list(Person.objects.find(full_name__contains=f"{cohort_tag}")[:500])
        print(f"Found {len(old_persons)} persons in the database with the same tag {cohort_tag}, deleting them...")

        for person in old_persons:
            person.delete()

        # ---- Phase 1 ----
        # Create new test people
        created: list[Person] = []
        for i in range(10):

            person_data = {
                "full_name": f"Test Person {cohort_tag}-{i:03d}",
                "birth_date": date(1990 + i, 1 + (i % 12), 10 + i),
                "wakes_at": time((6 + i) % 24, 30, 0),
                "Score": 3.14 + i,
                "avg_time": PythonDecimal("12.34") + PythonDecimal(i),
                "is_active": True,
            }

            # Create a new person record
            person = Person.objects.create(
                **person_data
            )
            created.append(person)

            # Check that every data stay the same after creation

            self.assertEqual(person.full_name, person_data["full_name"])
            self.assertEqual(person.birth_date, person_data["birth_date"])
            self.assertEqual(person.wakes_at, person_data["wakes_at"])
            self.assertEqual(person.Score, person_data["Score"])
            self.assertEqual(person.avg_time, person_data["avg_time"])
            self.assertEqual(person.is_active, person_data["is_active"])

            # Then refresh db and recheck that what we read is the same
            person.refresh_from_db()

            self.assertEqual(person.full_name, person_data["full_name"])
            self.assertEqual(person.birth_date, person_data["birth_date"])
            self.assertEqual(person.wakes_at, person_data["wakes_at"])
            self.assertEqual(person.Score, person_data["Score"])
            self.assertEqual(person.avg_time, person_data["avg_time"])
            self.assertEqual(person.is_active, person_data["is_active"])

            print("Created person :", person.to_dict())

            # Create some portals for this person
            created_portals_for_this_person = []
            for e in range(5):
                # Add a portal row via the portal manager on the instance (.create() will immediately save)
                address_data = {
                    "street": f"{cohort_tag} {i:03d}-{e:03d} Main St",
                    "city": "Springfield",
                    "zip": f"100:{i}:{e}",
                    "code": 20 + e,
                    "zone": random.randint(1000, 9999),
                    "reviewed_at": datetime(1 + 1 * 100 * i * e, (5 + e) % 12, 18, (6 + e) % 24, 30, 5),
                }

                # TODO add support also for .new() (without saving immediately)
                person.addresses.create(
                    **address_data
                )
                # TODO change DateTime -> Timestamp

                created_portals_for_this_person.append(address_data)

            read_addresses = list(person.addresses.all()[:1000])

            for (index, address) in enumerate(read_addresses):
                data_written = created_portals_for_this_person[index]

                print("Readed portal :", address.to_dict())

                # Check that every data stay the same after creation
                self.assertEqual(address.street, data_written["street"])
                self.assertEqual(address.city, data_written["city"])
                self.assertEqual(address.zip, data_written["zip"])
                self.assertEqual(address.code, data_written["code"])
                self.assertEqual(address.zone, data_written["zone"])
                self.assertEqual(address.reviewed_at, data_written["reviewed_at"])

                # Make a bit of change to each portal before saving
                address.city = address.city + "!"
                address.street = address.street + "r."
                address.zip = address.zip + "."
                address.code = address.code + 100000
                address.zone = address.zone + 100000

            # Change person data
            person.full_name = person.full_name + "."
            person.birth_date = person.birth_date.replace(day=1)
            person.is_active = False
            person.avg_time = PythonDecimal(1)
            person.Score = 1.0
            person.a_field_that_does_not_exist = "This field does not exist in FM and should be ignored"

            # We save all changes in one go
            person.save(portals=read_addresses)

            # Now we check that each change was committed successfully

            # Check model change
            self.assertEqual(person.full_name, person_data["full_name"] + ".")
            self.assertEqual(person.birth_date.day, 1)
            self.assertEqual(person.is_active, False)
            self.assertEqual(person.avg_time, PythonDecimal(1))
            self.assertEqual(person.Score, 1.0)

            self.assertEqual(person.a_field_that_does_not_exist,
                             "This field does not exist in FM and should be ignored")

            # Check portal changes by reading them back
            all_addresses_find = person.addresses.all()

            for address in all_addresses_find[:1000]:
                self.assertEqual("!", address.city[-1:])
                self.assertEqual(".", address.street[-1:])
                self.assertEqual(".", address.zip[-1:])
                self.assertTrue(address.code > 100000)
                self.assertTrue(address.code > 100000)

            # Refresh model and assert again
            person.refresh_from_db()

            self.assertEqual(person.full_name, person_data["full_name"] + ".")
            self.assertEqual(person.birth_date.day, 1)
            self.assertEqual(person.is_active, False)
            self.assertEqual(person.avg_time, PythonDecimal(1))
            self.assertEqual(person.Score, 1.0)
            self.assertEqual(person.a_field_that_does_not_exist,
                             "This field does not exist in FM and should be ignored")

        # ---- Phase 2 ----
        # Insert some spurious records to make sure filtering works
        for j in range(5):
            Person.objects.create(
                full_name=f"Spurious Person {cohort_tag}-{j:03d}",
                birth_date=date(1990 + j, 1 + (j % 12), 10 + j),
                wakes_at=time((6 + j) % 24, 30, 0),
                Score=3.14 + j,
                avg_time=PythonDecimal("12.34") + PythonDecimal(j),
                is_active=True,
            )

        # Find them back and order by name
        result = (Person.objects
        .find(full_name__contains=f"Test Person {cohort_tag}")
        .order_by("-full_name")
        .prefetch_portal("addresses", limit=5, offset=1)[:1000])

        # Check size of result
        result_list = len(list(result))
        self.assertEqual(result_list, len(created))

        # Check sort/filter
        sorted_desc_created = sorted(created, key=lambda x: x.full_name, reverse=True)

        for i in range(result_list):
            self.assertEqual(result[i].full_name, sorted_desc_created[i].full_name)

        # Check prefetch address
        for person in result:
            all_addresses_of_person = person.addresses.all()
            self.assertEqual(len(all_addresses_of_person[:1000]), 5)

            # Slice and iterate portal
            top_two_portal = all_addresses_of_person[0:2]
            self.assertEqual(2, len(list(top_two_portal)))

            # Assert they are the first 2 (check zip.split(":")[2] value)
            self.assertEqual(top_two_portal[0].zip.split(":")[2], "0.")
            self.assertEqual(top_two_portal[1].zip.split(":")[2], "1.")

            third_forth = all_addresses_of_person[2:4]
            self.assertEqual(2, len(list(third_forth)))

            self.assertEqual(third_forth[0].zip.split(":")[2], "2.")
            self.assertEqual(third_forth[1].zip.split(":")[2], "3.")

            # TODO avoid_prefetch_cache -> ignore_prefetched
            # TODO chunking with anchors ?
            # TODO safe limit: limit [:] ok but [:safelimit] if we reach the safe limit block everything with error

            # Delete portals
            print("Deleting first 2 portals for person :", person.full_name, "")
            person.save(portals_to_delete=top_two_portal)

            all_portals_readed = all_addresses_of_person.avoid_prefetch_cache()[:1000]
            self.assertEqual(len(all_portals_readed), 3)

            # Check that all portals were deleted
            for portal in all_portals_readed:
                self.assertNotIn(portal.record_id, [p.record_id for p in top_two_portal])

            print("Deleting remaining portals for person :", person.full_name, "")
            # Delete all remaining portals
            all_addresses_of_person.avoid_prefetch_cache()[:1000].delete()

        print("Deleting all person test data...")
        # Delete all people for this cohort tag
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()


if __name__ == "__main__":
    unittest.main()  # pragma: no cover
