from __future__ import annotations

import json
import logging
import os
import random
import unittest
from dataclasses import asdict
from datetime import date, time, datetime, timedelta
from decimal import Decimal as PythonDecimal
from pathlib import Path

import requests

import fmdata
from fmdata import fm_version_gte, FMVersion, Model, PortalField, PortalModel, FMFieldType, ScriptResult, \
    UsernamePasswordSessionProvider
from fmdata.results import FieldMetaData
from tests import env

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fmd_logger = logging.getLogger("fmdata")
fmd_logger.setLevel(logging.DEBUG)

# Add a console handler
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
fmd_logger.addHandler(handler)
logger.addHandler(handler)

PERSON_LAYOUT = "person"
BASE_PERSON_LAYOUT = "base_person"  # Slim version of Person layout with fewer fields.
ADDRESS_LAYOUT = "person_address"
ADDRESS_PORTAL_NAME = "person_addresses"
ADDRESS_SORTED_BY_CITY_PORTAL_NAME = "portal_person_addresses_sorted_by_city"
ADDRESS_PORTAL_TABLE_OCCURRANCE = "person_addresses"
current_dir = Path(__file__).parent

# Build a client using test env (tests/.env or process env)
fm_client = fmdata.Client(
    url=env("FMS_ADDRESS"),
    database=env("FMS_DB_NAME"),
    login_provider=UsernamePasswordSessionProvider(
        username=env("FMS_DB_USER"),
        password=env("FMS_DB_PASSWORD"),
    ),
    version=env("FMS_VERSION"),
    verify_ssl=env("FMS_VERIFY_SSL", default=False),
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
    street = fmdata.String(field_name=f"{ADDRESS_PORTAL_NAME}::Street", field_type=FMFieldType.Text)


class AddressPortal(Place):
    class Meta:
        table_occurrence_name = ADDRESS_PORTAL_TABLE_OCCURRANCE

    city = fmdata.String(field_name=f"{ADDRESS_PORTAL_NAME}::City", field_type=FMFieldType.Text)
    zip = fmdata.String(field_name=f"{ADDRESS_PORTAL_NAME}::Zip", field_type=FMFieldType.Text)
    code = fmdata.Integer(field_name=f"{ADDRESS_PORTAL_NAME}::Code", field_type=FMFieldType.Number)
    zone = fmdata.Integer(field_name=f"{ADDRESS_PORTAL_NAME}::Zone", field_type=FMFieldType.Text)
    reviewed_at = fmdata.DateTime(field_name=f"{ADDRESS_PORTAL_NAME}::ReviewedAt", field_type=FMFieldType.Text)
    picture = fmdata.Container(field_name=f"{ADDRESS_PORTAL_NAME}::Picture")


# --------------------------------------------------------------------------------------
# Main model using the fmdata ORM + our portal
# --------------------------------------------------------------------------------------

class AddressLayoutModel(Model):
    class Meta:
        client = fm_client
        layout = ADDRESS_LAYOUT

    the_city = fmdata.String(field_name=f"City", field_type=FMFieldType.Text)
    code = fmdata.Integer(field_name=f"Code", field_type=FMFieldType.Number)
    zone = fmdata.Integer(field_name=f"Zone", field_type=FMFieldType.Text)
    reviewed_at = fmdata.DateTime(field_name=f"ReviewedAt", field_type=FMFieldType.Text)
    picture = fmdata.Container(field_name=f"Picture")


# Fake layout only to check that the Metaclass is working (so we inherit client and we override layout)
class LivingBeing(Model):
    class Meta:
        client = fm_client
        layout = 'living_being'

    full_name = fmdata.String(field_name="FullName", field_type=FMFieldType.Text)


class Person(LivingBeing):
    class Meta:
        layout = PERSON_LAYOUT

    creation_timestamp = fmdata.DateTime(field_name="CreationTimestamp", field_type=FMFieldType.Timestamp)
    pk = fmdata.String(field_name="PrimaryKey", field_type=FMFieldType.Text)
    birth_date = fmdata.Date(field_name="BirthDate", field_type=FMFieldType.Date)
    join_time = fmdata.DateTime(field_name="JoinTime", field_type=FMFieldType.Timestamp)
    wakes_at = fmdata.Time(field_name="WakesUpAt", field_type=FMFieldType.Time)
    Score = fmdata.Float(field_type=FMFieldType.Number)
    avg_time = fmdata.Decimal(field_name="AvgTime", field_type=FMFieldType.Number)
    height = fmdata.Integer(field_name="Height", field_type=FMFieldType.Number)
    is_active = fmdata.Bool(field_name="IsActive", field_type=FMFieldType.Number)
    id_card_file = fmdata.Container(field_name="IDCardFile", field_type=FMFieldType.Container)

    addresses = PortalField(model=AddressPortal, name=ADDRESS_PORTAL_NAME)
    addresses_sorted_by_city = PortalField(model=AddressPortal, name=ADDRESS_SORTED_BY_CITY_PORTAL_NAME)


# --------------------------------------------------------------------------------------
# Integration tests for ORM + portals (server required)
# --------------------------------------------------------------------------------------
@error_if_no_env_server
class IntegrationTests(unittest.TestCase):

    def get_cohort_tag(self) -> str:
        return "ctag-" + str(random.randint(100000, 999999))

    def test_get_product_info(self):
        if not fm_version_gte(fm_client, FMVersion.V18):
            self.skipTest("This test requires FileMaker Server 18 or greater")

        response = fm_client.get_product_info()
        logger.info(json.dumps(asdict(response.response)))
        response.raise_exception_if_has_error()

    def test_get_databases(self):
        if not fm_version_gte(fm_client, FMVersion.V18):
            self.skipTest("This test requires FileMaker Server 18 or greater")

        result = fm_client.get_databases(username=env("FMS_DB_USER"), password=env("FMS_DB_PASSWORD"))
        logger.info(json.dumps(asdict(result.response)))
        result.raise_exception_if_has_error()

        for layout in result.response.databases:
            data = {
                "name": layout.name,
            }

            logger.info(json.dumps(data))

            # Assert no none
            for key in data:
                self.assertNotEquals(data[key], None)

    def test_get_layouts(self):
        if not fm_version_gte(fm_client, FMVersion.V18):
            self.skipTest("This test requires FileMaker Server 18 or greater")

        result = fm_client.get_layouts()
        result.raise_exception_if_has_error()
        logger.info(json.dumps(asdict(result.response)))

        for layout in result.response.layouts:
            data = {
                "name": layout.name,
                # "table": layout.table,
            }

            logger.info(f"layout_name: {layout.name} - table: {layout.table}")

            # Assert no none
            for key in data:
                self.assertNotEquals(data[key], None)

            layout_result = fm_client.get_layout(layout=layout.name)
            logger.info(json.dumps(asdict(layout_result.response)))
            layout_result.raise_exception_if_has_error()

            for field_meta in layout_result.response.field_meta_data:
                self.check_field_meta(field_meta)

            for portal_name, portal_field_meta in layout_result.response.portal_meta_data.items():
                logger.info(f"portal_name: {portal_name}")

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

        logger.info(json.dumps(data))

        # Assert no none
        for key in data:
            self.assertNotEquals(data[key], None)

    def test_get_scripts(self):
        if not fm_version_gte(fm_client, FMVersion.V18):
            self.skipTest("This test requires FileMaker Server 18 or greater")

        result = fm_client.get_scripts()
        result.raise_exception_if_has_error()

        logger.info(json.dumps(asdict(result.response)))

        for script in result.response.scripts:
            data = {
                "name": script.name,
                "is_folder": script.is_folder,
                # "folder_script_names": script.folder_script_names,
            }

            logger.info(json.dumps(data))

            # Assert no none
            for key in data:
                self.assertNotEquals(data[key], None)

    def test_execute_script(self):
        if not fm_version_gte(fm_client, FMVersion.V18):
            self.skipTest("This test requires FileMaker Server 18 or greater")

        result: ScriptResult = fm_client.perform_script(layout=PERSON_LAYOUT, name="ReturnInput",
                                                        param="ItsASuperInput")
        result.raise_exception_if_has_error()

        logger.info(json.dumps(result.response.raw_content))

        self.assertEqual(result.response.script_result, "OKItsASuperInput")
        self.assertEqual(result.response.script_error, "0")

    def test_create_find_update_delete_with_portals(self):
        # Create several people with mixed data
        cohort_tag = self.get_cohort_tag()

        # ---- Phase 0 ----
        # Delete waste form old test execution: all Test People with same cohort tag
        old_persons = list(Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000])
        logger.info(
            f"Found {len(old_persons)} persons in the database with the same tag {cohort_tag}, deleting them...")

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

            logger.info(f"Created person: {person.to_dict()}")

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

                created_portals_for_this_person.append(address_data)

            # Read fresh addresses
            read_addresses = list(person.addresses.all()[:1000])
            len_read_addresses = len(read_addresses)

            for (index, address) in enumerate(read_addresses):
                data_written = created_portals_for_this_person[index]

                logger.info(f"Readed portal: {address.to_dict()}")

                # Check that every data stay the same after creation
                self.assertEqual(address.street, data_written["street"])
                self.assertEqual(address.city, data_written["city"])
                self.assertEqual(address.zip, data_written["zip"])
                self.assertEqual(address.code, data_written["code"])
                self.assertEqual(address.zone, data_written["zone"])
                self.assertEqual(address.reviewed_at, data_written["reviewed_at"])

                # Make a bit of change to each portal before saving
                patch_address_data = {
                    "city": address.city + "!",
                    "street": address.street + "r.",
                    "zip": address.zip + ".",
                }

                address.update(**patch_address_data)
                address.code = address.code + 100000
                address.zone = address.zone + 100000

                # For half of them use the portal.save() (the others will be saved later with the model.save())
                if index < len_read_addresses // 2:
                    address.save()

            # Change person data

            person_patch_data = {
                "full_name": person.full_name + ".",
                "birth_date": person.birth_date.replace(day=1),
                "is_active": False,
            }

            person.update(**person_patch_data)

            person.avg_time = PythonDecimal(1)
            person.Score = 1.0
            person.a_field_that_does_not_exist = "This field does not exist in FM and should be ignored"

            # We save all changes in one go
            person.save(portals=read_addresses[len_read_addresses // 2:])

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

        # Check we can find them all back omitting the first one
        logging.info("Read people omitting 1 and check")
        result = Person.objects.find(full_name__contains=f"{cohort_tag}").omit(full_name__contains="-000")[:1000]
        self.assertEqual(len(result), len(created) - 1)

        for person in result:
            self.assertEqual(person.full_name.endswith(f"000"), False)

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

            # Delete portals
            logger.info(f"Deleting first 2 portals for person: {person.full_name}")
            person.save(portals_to_delete=top_two_portal)

            all_portals_readed = all_addresses_of_person.ignore_prefetched()[:1000]
            self.assertEqual(len(all_portals_readed), 3)

            # Check that all portals were deleted
            for portal in all_portals_readed:
                self.assertNotIn(portal.record_id, [p.record_id for p in top_two_portal])

            logger.info(f"Deleting remaining portals for person: {person.full_name}")
            # Delete all remaining portals
            all_addresses_of_person.ignore_prefetched()[:1000].delete()

        logger.info("Deleting all person test data...")
        # Delete all people for this cohort tag
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    def test_prefetch_portal(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        person_data = {
            "full_name": f"Test prefetch portal {cohort_tag}",
            "birth_date": date(1990, 1, 1),
        }

        person = Person.objects.create(**person_data)

        for i in range(5):
            address_data = {
                "street": f"Test prefetch portal {cohort_tag}-{i:03d}",
                "city": f"Test city prefetch portal {cohort_tag}-{i:03d}",
                "zip": f"Test zip prefetch portal {cohort_tag}-{i:03d}",
            }

            person.addresses.create(**address_data)

        # Prefetch portals
        people = Person.objects.find(full_name__contains=f"{cohort_tag}").prefetch_portal("addresses")

        self.assertEqual(len(people), 1)
        self.assertEqual(len(people[0].addresses.all()), 5)

        for address in people[0].addresses.all():
            self.assertEqual(address.street.startswith(f"Test prefetch portal {cohort_tag}-"), True)
            self.assertEqual(address.city.startswith(f"Test city prefetch portal {cohort_tag}-"), True)
            self.assertEqual(address.zip.startswith(f"Test zip prefetch portal {cohort_tag}-"), True)

        people = Person.objects.find(full_name__contains=f"{cohort_tag}").prefetch_portal("addresses", limit=2)
        self.assertEqual(len(people), 1)
        self.assertEqual(len(people[0].addresses.all()), 2)

        people = Person.objects.find(full_name__contains=f"{cohort_tag}").prefetch_portal("addresses", offset=4)
        self.assertEqual(len(people[0].addresses.all()), 2)
        self.assertEqual(people[0].addresses.all()[0].street, f"Test prefetch portal {cohort_tag}-003")
        self.assertEqual(people[0].addresses.all()[1].street, f"Test prefetch portal {cohort_tag}-004")

        people = Person.objects.find(full_name__contains=f"{cohort_tag}").prefetch_portal("addresses", limit=1, offset=3)
        self.assertEqual(len(people[0].addresses.all()), 1)
        self.assertEqual(people[0].addresses.all()[0].street, f"Test prefetch portal {cohort_tag}-002")

        logger.info("Deleting all person test data...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    def test_bulk_update_records(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        created = []

        for i in range(5):
            person_data = {
                "full_name": f"Test bulk update Person {cohort_tag}-{i:03d}",
                "birth_date": date(1990 + i, 1 + (i % 12), 10 + i),
                "wakes_at": time((6 + i) % 24, 30, 0),
                "Score": 3.14 + i,
                "avg_time": PythonDecimal("12.34") + PythonDecimal(i),
                "is_active": True,
            }

            created.append(Person.objects.create(**person_data))

        # Bulk update
        qs = Person.objects.find(full_name__contains=f"{cohort_tag}").omit(full_name__contains="-000")[:1000]

        qs.update({"is_active": False, "Score": 0})

        for index, person in enumerate(created):
            # Check that the first entry (-000) is untouched
            if index == 0:
                self.assertEqual(person.is_active, True)
                self.assertEqual(person.Score, 3.14)

                person.refresh_from_db()

                self.assertEqual(person.is_active, True)
                self.assertEqual(person.Score, 3.14)

                continue

            # Check that the rest of the entries are updated
            self.assertEqual(person.is_active, True)
            self.assertNotEqual(person.Score, 0)

            person.refresh_from_db()

            self.assertEqual(person.is_active, False)
            self.assertEqual(person.Score, 0)

        logger.info("Clearing testing data...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    def test_bulk_update_portal_records(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        person_data = {
            "full_name": f"Test bulk update portal records {cohort_tag}",
            "birth_date": date(1990, 1, 1),
            "wakes_at": time(6, 0, 0),
            "Score": 3.14,
            "avg_time": PythonDecimal("12.34"),
            "is_active": True,
        }

        person = Person.objects.create(**person_data)

        for i in range(5):
            address_data = {
                "street": f"Test bulk update portal records {cohort_tag}-{i:03d}",
                "city": f"Test bulk update portal records {cohort_tag}-{i:03d}",
                "zip": f"Test bulk update portal records {cohort_tag}-{i:03d}",
            }

            person.addresses.create(**address_data)

        # Test exception if we try to iterate/fetch without .all()
        with self.assertRaises(Exception):
            list(person.addresses)

        # Bulk update portals
        person.addresses.all().update({"zip": "0."})

        addresses = person.addresses.all()

        for address in addresses:
            self.assertEqual(address.zip, "0.")
            self.assertEqual(address.street.startswith(f"Test bulk update portal records {cohort_tag}-"), True)
            self.assertEqual(address.city.startswith(f"Test bulk update portal records {cohort_tag}-"), True)

        logger.info("Clearing testing data...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()


    def test_duplicate_records(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        person_data = {
            "full_name": f"Test duplicate records {cohort_tag}",
            "birth_date": date(1990, 1, 1),
            "wakes_at": time(6, 0, 0),
            "Score": 3.14,
            "avg_time": PythonDecimal("12.34"),
            "is_active": True,
        }

        # Create person
        person = Person.objects.create(**person_data)

        # Duplicate and check data
        person_dup = person.duplicate()

        self.assertNotEqual(person.record_id, person_dup.record_id)
        self.assertNotEqual(None, person_dup.mod_id)
        self.assertEqual(person.full_name, person_dup.full_name)
        self.assertEqual(person.birth_date, person_dup.birth_date)
        self.assertEqual(person.wakes_at, person_dup.wakes_at)
        self.assertEqual(person.Score, person_dup.Score)
        self.assertEqual(person.avg_time, person_dup.avg_time)
        self.assertEqual(person.is_active, person_dup.is_active)

        # Refresh from db and recheck data

        person_dup.refresh_from_db()

        self.assertNotEqual(person.record_id, person_dup.record_id)
        self.assertNotEqual(None, person_dup.mod_id)
        self.assertEqual(person.full_name, person_dup.full_name)
        self.assertEqual(person.birth_date, person_dup.birth_date)
        self.assertEqual(person.wakes_at, person_dup.wakes_at)
        self.assertEqual(person.Score, person_dup.Score)
        self.assertEqual(person.avg_time, person_dup.avg_time)
        self.assertEqual(person.is_active, person_dup.is_active)

        # Create a person without save() and check raise error

        person = Person(**person_data)

        with self.assertRaises(Exception):
            person.duplicate()

        logger.info("Deleting all person test data...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    def test_chunking(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        created: list[Person] = []
        for i in range(5):
            logger.info(f"Creating person {cohort_tag}-{i:03d}")
            person_data = {
                "full_name": f"Test chunking Person {cohort_tag}-{i:03d}",
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

        logger.info("Reading all people again with chunking (chunk_size=1)...")
        people = Person.objects.find(full_name__startswith=f"Test chunking Person {cohort_tag}").chunking(1)

        self.assertEqual(len(people), 5)
        # The pages are 1 more because we have 5 records with chunk size 1 => 5 pages + 1 empty page at the end
        self.assertEqual(len(people._result_pages), 6)

        logger.info("Reading all people again with chunking (chunk_size=2)...")

        people = Person.objects.find(full_name__startswith=f"Test chunking Person {cohort_tag}").chunking(2)
        self.assertEqual(len(people), 5)
        # Because the 3rd pages contains only 1 element (< chunk size)
        self.assertEqual(len(people._result_pages), 3)

        logger.info("Reading all people again with chunking (chunk_size=2) + first()...")

        people = Person.objects.find(full_name__startswith=f"Test chunking Person {cohort_tag}").chunking(2)
        people.first()

        # We ensure the the resultset in cloned correctly in .first() and is capable to make another query
        people = people[:1000]
        self.assertEqual(len(people), 5)
        self.assertEqual(len(people._result_pages), 3)

        logger.info("Reading all people again with chunking (chunk_size=2) + slice[] ...")

        people = Person.objects.find(full_name__startswith=f"Test chunking Person {cohort_tag}").chunking(2)[:3]

        first_two = people[:2]
        self.assertEqual(len(first_two), 2)
        self.assertEqual(len(first_two._result_pages), 1)

        all_people = people[:1000]  # Slice after slice (so it will return 3 elements because of the previous slice)
        self.assertEqual(len(all_people), 3)
        self.assertEqual(len(all_people._result_pages), 2)

        logger.info("Test element shift during chunking (duplicates)")

        people = Person.objects.find(full_name__startswith=f"Test chunking Person {cohort_tag}").chunking(2).order_by(
            "full_name", )[:1000]

        for index, person in enumerate(people):

            # When we are at the end of the first page we add another element in the first page, so the next page will contain again the element 2
            if index == 1:
                Person.objects.create(
                    full_name=f"Test chunking Person {cohort_tag}-000",
                )

        self.assertEqual(len(people), 5)

        record_ids = [person.record_id for person in people]
        self.assertEqual(len(record_ids), len(set(record_ids)), "Duplicates found in chunked resultset!")

        logger.info("Clearing testing data...")

        # Clear
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    # TODO add test for corner error cases (in general with fm errors)

    def test_portals_chunking(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        person = Person.objects.create(
            full_name=f"Test portals chunking Person {cohort_tag}-000",
            birth_date=date(1990, 1, 1),
            wakes_at=time(6, 0, 0),
            Score=3.14,
            avg_time=PythonDecimal("12.34"),
            is_active=True,
        )

        def city(i):
            return f"Test portals chunking City {cohort_tag}-{i:03d}"

        for i in range(5):
            logger.info(f"Creating address {cohort_tag}-{i:03d}")
            address_data = {
                "city": city(i),
                "code": i,
            }

            person.addresses.create(
                **address_data
            )

        logger.info("Reading all addresses with chunking (chunk_size=1)...")
        addresses = person.addresses.chunking(1)

        self.assertEqual(len(addresses), 5)
        # The pages are 1 more because we have 5 records with chunk size 1 => 5 pages + 1 empty page at the end
        self.assertEqual(len(addresses._result_pages), 6)

        logger.info("Reading all addresses again with chunking (chunk_size=2)...")

        addresses = person.addresses.chunking(2)
        self.assertEqual(len(addresses), 5)
        # Because the 3rd pages contains only 1 element (< chunk size)
        self.assertEqual(len(addresses._result_pages), 3)

        logger.info("Reading all addresses again with chunking (chunk_size=2) + first()...")

        addresses = person.addresses.chunking(2)
        self.assertEqual(addresses.first().code, 0)

        # We ensure the the resultset in cloned correctly in .first() and is capable to make another query
        addresses = addresses[:1000]
        self.assertEqual(len(addresses), 5)
        self.assertEqual(len(addresses._result_pages), 3)

        logger.info("Reading all adresses again with chunking (chunk_size=2) + slice[] ...")

        addresses = person.addresses.chunking(2)[:3]

        first_two = addresses[:2]
        self.assertEqual(len(first_two), 2)
        self.assertEqual(len(first_two._result_pages), 1)

        all_addresses = addresses[
            :1000]  # Slice after slice (so it will return 3 elements because of the previous slice)
        self.assertEqual(len(all_addresses), 3)
        self.assertEqual(len(all_addresses._result_pages), 2)

        logger.info("Test element shift during chunking (duplicates)")

        # Pay attention to "addresses_sorted_by_city" (city asc)
        # So when we add a new city "city 0" it will be the first in the sorted list, so the second page will contain again the element with city "city 1"
        addresses = person.addresses_sorted_by_city.chunking(2)[:1000]

        for index, address in enumerate(addresses):

            # When we are at the end of the first page we add another element in the first page, so the next page will contain again the element 2
            if index == 1:
                person.addresses.create(
                    city=city(0)
                )

        self.assertEqual(len(addresses), 5)

        record_ids = [address.record_id for address in addresses]
        self.assertEqual(len(record_ids), len(set(record_ids)), "Duplicates found in chunked resultset!")

        logger.info("Clearing testing data...")
        person.addresses[:1000].delete()
        person.delete()

    def test_container_upload_download_and_as_layout_model(self):
        cohort_tag = self.get_cohort_tag()

        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        # Create a new person record
        person = Person.objects.create(
            full_name=f"Test Container Person {cohort_tag}",
        )

        # Update container with new file
        file_path = current_dir / "fixture/test_file_0.txt"
        with open(file_path, "rb") as file:
            person.update_container("id_card_file", file)

        person.refresh_from_db()
        download_link = person.id_card_file
        logger.info(f"Container file download link: {download_link}")

        # Download and test content
        response = requests.get(download_link, verify=fm_client.verify_ssl)
        self.assertEqual(response.status_code, 200)

        with open(file_path, "rb") as file:
            self.assertEqual(response.content, file.read())

        # Do the same for a portal container
        person.addresses.create(
            city="Test City",
        )

        addresses = person.addresses.all()[:1000]
        address = addresses[0]

        self.assertEqual(address.city, "Test City")

        # Modify a field to test if the updated_fields are correctly passed
        address.city = "Test City!"

        # Convert to layout model
        address_as_layout_model = address.as_layout_model(model_class=AddressLayoutModel)

        # So now we save and refresh_from_db() to test that the modified field is saved too
        address_as_layout_model.save()
        address_as_layout_model.refresh_from_db()

        # Assert each field is copied correctly
        self.assertEqual(address.record_id, address_as_layout_model.record_id)
        self.assertEqual(str(int(address.mod_id) + 1), address_as_layout_model.mod_id)
        self.assertEqual(address.zone, address_as_layout_model.zone)
        self.assertEqual(address.reviewed_at, address_as_layout_model.reviewed_at)
        # Pay attention to "the_city" instead of "city"
        self.assertEqual(address.city, address_as_layout_model.the_city)

        # Try to modify a "the_city" field
        address_as_layout_model.the_city = "Test City 2"

        # Update portal container with new file
        file_path = current_dir / "fixture/test_file_1.txt"
        with open(file_path, "rb") as file:
            address_as_layout_model.update_container("picture", file)

        # Save the layout model
        address_as_layout_model.save()

        # Read again the portal model and check
        addresses = person.addresses.all()[:1000]
        address = addresses[0]

        # Check the "the_city" field was modified
        self.assertEqual(address.city, "Test City 2")

        download_link = address.picture
        logger.info(f"Portal container file download link: {download_link}")

        response = requests.get(download_link, verify=fm_client.verify_ssl)
        self.assertEqual(response.status_code, 200)
        with open(file_path, "rb") as file:
            self.assertEqual(response.content, file.read())

        # Cleanup and call delete function to test them
        address.delete()

        addresses = person.addresses.all()[:1000]
        self.assertEqual(len(addresses), 0)

        # But first, a last test for testing create/delete on portals through the person manager
        for i in range(5):
            person.addresses.create(
                city="Test! City!",
            )

        addresses = person.addresses.all()[:1000]
        self.assertEqual(len(addresses), 5)

        person.addresses.all()[:1000].delete()

        addresses = person.addresses.all()[:1000]
        self.assertEqual(len(addresses), 0)

        # Delete person and check
        person.delete()

        persons = Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000]
        self.assertEqual(len(persons), 0)

    def test_get_records(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        with self.assertRaises(Exception):
            list(Person.objects)

        created_people = []
        for i in range(3):
            created_person = Person.objects.create(
                full_name=f"Test DB Analysis Person {cohort_tag}",
                Score=1.0,
                avg_time=PythonDecimal(1),
                height=i,
                birth_date=date(2023, 1, 1),
                wakes_at=time(0, 0, 0),
                join_time=datetime(2023, 1, 1, 0, 0, 0),
                is_active=True,
            )

            created_people.append(created_person)

        created_people_ids = [p.record_id for p in created_people]

        # To test get_records() (no condition in the query)
        people = Person.objects.all().order_by("-creation_timestamp", "-height")[:50]
        self.assertGreaterEqual(len(people), 0)

        found = 0
        for index, person in enumerate(people):
            # Search a person with the same ID of the created one

            if person.record_id in created_people_ids:
                found += 1

                # Check written values
                self.assertEqual(person.full_name, f"Test DB Analysis Person {cohort_tag}")
                self.assertEqual(person.Score, 1.0)
                self.assertEqual(person.avg_time, PythonDecimal(1))
                self.assertGreaterEqual(person.height, 0)
                self.assertEqual(person.birth_date, date(2023, 1, 1))
                self.assertEqual(person.wakes_at, time(0, 0, 0))
                self.assertEqual(person.join_time, datetime(2023, 1, 1, 0, 0, 0))
                self.assertEqual(person.is_active, True)

                # Check sorting descending (inverted to created order)
                self.assertEqual(person.record_id, created_people[-(found)].record_id)

        self.assertEqual(found, 3)

        for person in created_people:
            person.delete()

    def test_criteria(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")

        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        def full_name(index):
            return f"{cohort_tag} Test{index:02d} Container Person{cohort_tag}, {index:02d} {cohort_tag}"

        def score(index):
            return index + 0.5

        def avg_time(index):
            return PythonDecimal(index + 0.5)

        def height(index):
            return index

        def birth_date(index):
            return datetime(2023, 1, 1) + timedelta(days=index)

        def wakes_at(index):
            return time(index, 0, 0)

        def join_time(index):
            return datetime(2023, 1, 1, index, 0, 0)

        def is_active(index):
            return index % 2 == 0

        for i in range(11):
            # Create a new person record
            person = Person.objects.create(
                full_name=full_name(i),
                Score=score(i),
                avg_time=avg_time(i),
                height=height(i),
                birth_date=birth_date(i),
                wakes_at=wakes_at(i),
                join_time=join_time(i),
                is_active=is_active(i),
            )

        # @, *, #, ?, !, =, <, >, and "

        # ---- Strings ----
        result_set = Person.objects.find(full_name__exact=full_name(5))
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name=full_name(5))
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__raw=f"=={cohort_tag}*t05*")
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__startswith=f"{cohort_tag} Test03")[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__endswith=f"07 {cohort_tag}")[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"Person{cohort_tag}, 0")[:1000]
        self.assertEqual(len(result_set), 10)

        # Because 5 is int and is not accepted
        with self.assertRaises(ValueError):
            Person.objects.find(full_name__contains=f"{cohort_tag}", full_name__endswith=5)

        # ---- Integer ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__exact=5)[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__startswith=5)[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__endswith=0)[:1000]
        self.assertEqual(len(result_set), 2)  # 0, 10

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__gt=5)[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__gte=5)[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__lt=5)[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__lte=5)[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__range=[1, 3, ])[:1000]
        self.assertEqual(len(result_set), 3)

        with self.assertRaises(ValueError):
            Person.objects.find(full_name__contains=f"{cohort_tag}", height__exact=5.5)

        with self.assertRaises(ValueError):
            Person.objects.find(full_name__contains=f"{cohort_tag}", height__range=[1, 3, 4])

        # ---- Float ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__exact=5)[:1000]
        self.assertEqual(len(result_set), 0)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__exact=5.5)[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__startswith=5)[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__endswith=5)[:1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__contains=5)[:1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__gt=5)[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__gt=5.5)[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__gte=5.5)[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__lt=5)[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__lt=5.5)[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__lte=5.5)[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", Score__range=[1, 3])[:1000]
        self.assertEqual(len(result_set), 2)

        # Because "5.5" is str and is not accepted
        with self.assertRaises(ValueError):
            Person.objects.find(full_name__contains=f"{cohort_tag}", Score__lte="5.5")

        # ---- Decimal ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__exact=PythonDecimal(5))[:1000]
        self.assertEqual(len(result_set), 0)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__exact=PythonDecimal(5.5))[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(
            full_name__contains=f"{cohort_tag}", avg_time__startswith=PythonDecimal(5))[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__endswith=PythonDecimal(5))[
            :1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(
            full_name__contains=f"{cohort_tag}", avg_time__contains=PythonDecimal(5))[:1000]

        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__gt=PythonDecimal(5))[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__gt=PythonDecimal("5.5"))[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__gte=PythonDecimal("5.5"))[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__lt=PythonDecimal(5))[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__lt=PythonDecimal("5.5"))[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__lte=PythonDecimal("5.5"))[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         avg_time__range=[PythonDecimal(1), PythonDecimal(3)])[:1000]
        self.assertEqual(len(result_set), 2)

        # Because 5.5 is float and is not accepted
        with self.assertRaises(ValueError):
            Person.objects.find(full_name__contains=f"{cohort_tag}", avg_time__lte=5.5)

        # ---- Date ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__exact=datetime(2023, 1, 1))[
            :1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__gt=datetime(2023, 1, 1))[
            :1000]
        self.assertEqual(len(result_set), 10)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__gte=datetime(2023, 1, 1))[
            :1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__lt=datetime(2023, 2, 1))[
            :1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__lt=datetime(2023, 1, 5))[
            :1000]
        self.assertEqual(len(result_set), 4)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", birth_date__lte=datetime(2023, 1, 5))[
            :1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         birth_date__range=[datetime(2023, 1, 1), datetime(2023, 1, 5)])[:1000]
        self.assertEqual(len(result_set), 5)

        # ---- Time ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__exact=time(0, 0, 0))[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__gt=time(0, 0, 0))[:1000]
        self.assertEqual(len(result_set), 10)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__gte=time(0, 0, 0))[:1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__lt=time(23, 59, 59))[:1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__lt=time(4, 0, 00))[:1000]
        self.assertEqual(len(result_set), 4)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", wakes_at__lte=time(4, 0, 00))[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         wakes_at__range=[time(0, 1, 0), time(7, 1, 59)])[:1000]
        self.assertEqual(len(result_set), 7)

        #  ---- DateTime ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__exact=datetime(2023, 1, 1, 5, 0, 0))[:1000]
        self.assertEqual(len(result_set), 1)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__gt=datetime(2023, 1, 1, 4, 0, 0))[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__gte=datetime(2023, 1, 1, 4, 0, 0))[:1000]
        self.assertEqual(len(result_set), 7)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__lt=datetime(2023, 1, 3, 0, 0, 0))[:1000]
        self.assertEqual(len(result_set), 11)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__lt=datetime(2023, 1, 1, 5, 0, 0))[:1000]
        self.assertEqual(len(result_set), 5)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__lte=datetime(2023, 1, 1, 5, 0, 0))[:1000]
        self.assertEqual(len(result_set), 6)

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}",
                                         join_time__range=[datetime(2023, 1, 1, 5, 0, 0),
                                                           datetime(2023, 1, 1, 11, 0, 0)])[:1000]
        self.assertEqual(len(result_set), 6)

        # ---- Boolean ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}", height__lt=4, is_active__exact=True)[
            :1000]
        self.assertEqual(len(result_set), 2)

        # Wipe test data
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

    def test_response_layout_and_scripts(self):
        cohort_tag = self.get_cohort_tag()

        logger.info(f"Deleting all person test data for cohort tag: {cohort_tag} ...")
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()

        logger.info(f"Creating test data for cohort tag: {cohort_tag} ...")
        created_people = []

        for i in range(5):
            person = Person.objects.create(
                full_name=f"Test Person {cohort_tag}, {i}",
                height=i,
                Score=i,
                avg_time=PythonDecimal(i),
                birth_date=datetime(2023, 1, i + 1),
                wakes_at=time(0, i, 0),
                join_time=datetime(2023, 1, 1, i, 0, 0),
            )

            created_people.append(person)

        # Test response layout with fewer fields
        search_with_base_response_layout = Person.objects.find(full_name__contains=f"{cohort_tag}").response_layout(
            BASE_PERSON_LAYOUT)[:1000]

        for person in search_with_base_response_layout:
            self.assertTrue(person.full_name.startswith(f"Test Person {cohort_tag},"))
            self.assertLessEqual(person.height, 4)

            self.assertIsNone(person.Score)
            self.assertIsNone(person.avg_time)
            self.assertIsNone(person.birth_date)
            self.assertIsNone(person.wakes_at)
            self.assertIsNone(person.join_time)

        # Test a search with a script that will add to each records in output 10000 to Score value

        # ---- after_script ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}").after_script("AddToScore", "10000")[:1000]

        scripts_response = next(result_set.scripts_responses())
        self.assertEqual(scripts_response.after.result, "OK10000")
        self.assertEqual(scripts_response.after.error, "101")

        # The order of this assert is important! We want to make sure that the cache iterator of the scripts_response is not stealing data to the result's one
        self.assertEqual(len(result_set), 5)

        for person in result_set:
            self.assertTrue(person.full_name.startswith(f"Test Person {cohort_tag},"))
            self.assertLessEqual(person.height, 4)
            self.assertGreaterEqual(person.Score, 10000)
            self.assertLess(person.Score, 11000)

        # ---- pre_sort_script ----

        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}").presort_script(
            "AddToScore", "20000")[:1000]

        scripts_response = next(result_set.scripts_responses())
        self.assertEqual(scripts_response.presort.result, "OK20000")
        self.assertEqual(scripts_response.presort.error, "101")

        self.assertEqual(len(result_set), 5)

        for person in result_set:
            self.assertTrue(person.full_name.startswith(f"Test Person {cohort_tag},"))
            self.assertLessEqual(person.height, 4)
            # Because we are adding 20000 (and 10000 where already added) = 30000
            self.assertGreaterEqual(person.Score, 30000)
            self.assertLess(person.Score, 31000)

        # ---- pre_request_script ----
        result_set = Person.objects.find(full_name__contains=f"{cohort_tag}").prerequest_script(
            "ReturnInput", "AFantasticInput!!")[:1000]

        scripts_response = next(result_set.scripts_responses())
        self.assertEqual(scripts_response.prerequest.result, "OKAFantasticInput!!")
        self.assertEqual(scripts_response.prerequest.error, "0")

        self.assertEqual(len(result_set), 5)

        # A test with get_records instead of find
        result_set = Person.objects.all().prerequest_script("ReturnInput", "AnotherInput!!").order_by("height")[:1]

        scripts_response = next(result_set.scripts_responses())
        self.assertEqual(scripts_response.prerequest.result, "OKAnotherInput!!")
        self.assertEqual(scripts_response.prerequest.error, "0")

        self.assertEqual(len(result_set), 1)

        # Clear test data
        Person.objects.find(full_name__contains=f"{cohort_tag}")[:1000].delete()


if __name__ == "__main__":
    unittest.main()  # pragma: no cover
