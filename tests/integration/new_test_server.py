from __future__ import annotations

import os
import random
import unittest
from datetime import date, time
from decimal import Decimal as PythonDecimal

import fmdata
from fmdata import FMFieldType, fmd_fields
from fmdata.orm import (
    Model,
    PortalField,
    PortalModel,
    PortalManager,
)
from fmdata.session_providers import UsernamePasswordSessionProvider
from tests import env

# --------------------------------------------------------------------------------------
# CONFIG NOTE FOR THE USER
# --------------------------------------------------------------------------------------
# These integration tests expect that corresponding FileMaker layouts, fields and portals
# exist in your FileMaker database. Please create them before running the tests.
#
# Layouts referenced (create these in FileMaker):
#   - test_fmdata_person_layout
#   - test_fmdata_address_layout (portal target)
#   - test_fmdata_person_address_layout (join table if needed)
#   - test_fmdata_exam_layout (optional extra portal demonstration)
#
# Fields expected on test_fmdata_person_layout:
#   - PrimaryKey (Text)
#   - FullName (Text)
#   - BirthDate (Date)
#   - WakesUpAt (Time)
#   - ScoreFloat (Number)
#   - ScoreDecimal (Number)
#   - IsActive (Number, 1/0 or Yes/No but returning 1/0)
#   - ContainerField (Container)
#
# Portal "test_fmdata_address_1" to related table occurrence with fields:
#   - test_fmdata_address_1::Street (Text)
#   - test_fmdata_address_1::City (Text)
#   - test_fmdata_address_1::Zip (Text)
#
# You can adjust names if needed but then update constants below.
# --------------------------------------------------------------------------------------

PERSON_LAYOUT = "test_fmdata_person_layout"
ADDRESS_PORTAL_NAME = "test_fmdata_address_1"
EXAM_PORTAL_NAME = "test_fmdata_exam_1"  # only if you add a second portal for stress tests

# Build a client using test env (tests/.env or process env)
fm_client = fmdata.FMClient(
    url=env("FMS_ADDRESS"),
    database=env("FMS_DB_NAME"),
    login_provider=UsernamePasswordSessionProvider(
        username=env("FMS_DB_USER"),
        password=env("FMS_DB_PASSWORD"),
    ),
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
class AddressPortal(PortalModel):
    street = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::Street", field_type=FMFieldType.Text)
    city = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::City", field_type=FMFieldType.Text)
    zip = fmd_fields.String(field_name=f"{ADDRESS_PORTAL_NAME}::Zip", field_type=FMFieldType.Text)


# --------------------------------------------------------------------------------------
# Main model using the fmdata ORM + our portal
# --------------------------------------------------------------------------------------
class Person(Model):
    class Meta:
        client = fm_client
        layout = PERSON_LAYOUT

    pk = fmd_fields.String(field_name="PrimaryKey", field_type=FMFieldType.Text)
    full_name = fmd_fields.String(field_name="FullName", field_type=FMFieldType.Text)
    birth_date = fmd_fields.Date(field_name="BirthDate", field_type=FMFieldType.Date)
    wakes_at = fmd_fields.Time(field_name="WakesUpAt", field_type=FMFieldType.Time)
    score_float = fmd_fields.Float(field_name="ScoreFloat", field_type=FMFieldType.Number)
    score_decimal = fmd_fields.Decimal(field_name="ScoreDecimal", field_type=FMFieldType.Number)
    is_active = fmd_fields.Bool(field_name="IsActive", field_type=FMFieldType.Number)
    container = fmd_fields.Container(field_name="ContainerField", field_type=FMFieldType.Container)

    addresses = PortalField(model=AddressPortal, name=ADDRESS_PORTAL_NAME)


# --------------------------------------------------------------------------------------
# Integration tests for ORM + portals (server required)
# --------------------------------------------------------------------------------------
@error_if_no_env_server
class ORMIntegrationTests(unittest.TestCase):

    def test_create_find_update_delete_with_portals(self):
        # Create several people with mixed data
        cohort_tag = random.randint(1000, 999999)
        created: list[Person] = []

        for i in range(3):
            person = Person.objects.create(
                full_name=f"Test Person {cohort_tag}-{i:03d}",
                birth_date=date(1990 + i, 1 + (i % 12), 10 + i),
                wakes_at=time(6 + i, 30, 0),
                score_float=3.14 + i,
                score_decimal=PythonDecimal("12.34") + PythonDecimal(i),
                is_active=True if i % 2 == 0 else False,
            )
            created.append(person)

            # Add a portal row via the portal manager on the instance
            addr = person.addresses.create(street=f"{i} Main St", city="Springfield", zip=f"000{i}")
            # Make one extra change and save only that portal via main save
            addr.city = addr.city + "!"
            person.save(portals=[addr])

            # Ensure we can read back portal content without prefetch cache
            first_addr = person.addresses.avoid_prefetch_cache().first()
            self.assertIsNotNone(first_addr)
            self.assertIn("Springfield", first_addr.city)

        # Find them back and order by name
        result = (Person.objects
                  .find(full_name__contains=f"{cohort_tag}-")
                  .order_by("full_name")
                  .chunk_size(50)
                  .prefetch_portal(ADDRESS_PORTAL_NAME, limit=10, offset=1))

        # Slice and iterate
        top_two = result[0:2]
        self.assertEqual(2, len(list(top_two)))

        for p in result:
            self.assertIsInstance(p.record_id, str)
            self.assertTrue(p.full_name.startswith("Test Person "))
            # Only use prefetched portals for quick iteration
            portals: PortalManager = p.addresses.only_prefetched()
            for addr in portals:
                self.assertIsInstance(addr.name if hasattr(addr, 'name') else "", str)
                self.assertIsInstance(addr.record_id, str)

            # Update some scalar fields and a portal at the same time
            p.full_name = p.full_name + "."
            # Create a new portal entry and then immediately update it
            new_addr = p.addresses.create(street="X St", city="Metropolis", zip="12345")
            new_addr.city = "Metropolis+"
            p.save(portals=[new_addr])

        # Delete portals for one person using manager.delete and via save(portals_to_delete)
        victim = result.first()
        self.assertIsNotNone(victim)

        # delete via manager.delete(): remove all its portals
        victim.addresses.avoid_prefetch_cache().delete()

        # recreate two portals and then delete a single one via save(portals_to_delete)
        a1 = victim.addresses.create(street="A St", city="Town", zip="11111")
        a2 = victim.addresses.create(street="B St", city="Town", zip="22222")
        # Persist both portals explicitly
        victim.save(portals=[a1, a2])
        # Now mark one for deletion
        victim.save(portals_to_delete=[a1])

        # Finally delete all created records
        for p in created:
            p.delete()

    def test_prefetch_and_first_helpers(self):
        # Create one record and two portals, then exercise first/only_prefetched and slicing
        p = Person.objects.create(
            full_name="Prefetch Person",
            birth_date=date(2000, 1, 1),
            wakes_at=time(7, 0, 0),
            score_float=1.5,
            score_decimal=PythonDecimal("2.5"),
            is_active=True,
        )
        a = p.addresses.create(street="One", city="City", zip="10001")
        b = p.addresses.create(street="Two", city="City", zip="10002")
        p.save(portals=[a, b])

        # With prefetch
        res = (Person.objects
               .find(full_name__exact="Prefetch Person")
               .prefetch_portal(ADDRESS_PORTAL_NAME, limit=10, offset=1))

        item = res.first()
        self.assertIsNotNone(item)
        portals = item.addresses.only_prefetched()
        self.assertGreaterEqual(len(list(portals)), 1)

        # Slicing portals
        sliced = item.addresses[0:1]
        self.assertEqual(1, len(list(sliced)))

        # Clean up
        p.delete()


if __name__ == "__main__":
    unittest.main()  # pragma: no cover
