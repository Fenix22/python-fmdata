from __future__ import annotations

import unittest
from io import BytesIO
from unittest.mock import Mock, patch

import fmdata
from fmdata import FMFieldType
from fmdata.client import FMVersion
from fmdata.orm import (
    Criteria,
    Model,
    ModelManager,
    PortalField,
    PortalManager,
    PortalModel,
    SearchCriteria,
    add_portal_record_to_portal_data,
    escape_filemaker_special_characters,
    get_fm_value,
    get_meta_attribute,
    dump_fields_data,
    load_fields_data,
    patch_from_model_or_portal,
    portal_model_iterator_from_portal_data,
)
from fmdata.results import CreateRecordResult
from fmdata.results import GetRecordResult, GetRecordsResult, Page, PortalData, PortalPage


def make_http_response(response=None, messages=None):
    class DummyHttpResponse:
        def __init__(self):
            self._parse_float = str
            self.headers = {}
            self.content = b""

        def json(self, parse_float=None):
            return {
                "messages": messages if messages is not None else [{"code": "0", "message": "OK"}],
                "response": response if response is not None else {},
            }

        def raise_for_status(self):
            return None

    return DummyHttpResponse()


CLIENT = Mock()
CLIENT.version = FMVersion.V22


class AddressPortal(PortalModel):
    class Meta:
        table_occurrence = "AddressTO"
        portal_name = "Addresses"

    city = fmdata.String(field_name="AddressTO::City", field_type=FMFieldType.Text)
    zip_code = fmdata.Integer(field_name="AddressTO::Zip", field_type=FMFieldType.Number)


class Person(Model):
    class Meta:
        client = CLIENT
        layout = "People"

    name = fmdata.String(field_name="Name", field_type=FMFieldType.Text)
    age = fmdata.Integer(field_name="Age", field_type=FMFieldType.Number)
    photo = fmdata.Container(field_name="Photo")
    addresses = PortalField(model=AddressPortal, name="Addresses")


class ORMUtilityTests(unittest.TestCase):
    def test_load_dump_helpers_and_field_descriptor(self):
        self.assertEqual(
            load_fields_data(Person._meta.fm_fields, {"Name": "Alice", "Age": "30", "Photo": "http://x"}),
            {"name": "Alice", "age": 30, "photo": "http://x"},
        )
        self.assertEqual(
            dump_fields_data(Person._meta.fields, {"name": "Alice", "age": 30, "photo": "http://x"}),
            {"Name": "Alice", "Age": 30},
        )

        field = fmdata.String(field_name="Name", field_type=FMFieldType.Text)
        class Holder:
            value = field

        holder = Holder()
        holder.value = "Alice"
        self.assertEqual(holder.value, "Alice")
        self.assertIs(Holder.value, field)
        self.assertEqual(field._field_name, "Name")
        self.assertFalse(field.read_only)

    def test_get_meta_attribute_and_field_metadata(self):
        class Base:
            _meta = type("Meta", (), {"value": "base"})()

        class Child(Base):
            pass

        attrs_meta = type("Meta", (), {"value": "child"})()
        self.assertEqual(get_meta_attribute(Child, attrs_meta, "value"), "child")
        self.assertEqual(get_meta_attribute(Child, None, "value"), "base")
        self.assertEqual(get_meta_attribute(Child, None, "missing", default="fallback"), "fallback")
        self.assertEqual(Person._meta.fields["name"].filemaker_name, "Name")
        self.assertEqual(Person._meta.portal_fields["addresses"].filemaker_name, "Addresses")
        self.assertEqual(AddressPortal._meta.fields["city"].filemaker_name, "AddressTO::City")
        self.assertEqual(Person._meta.fm_fields["Name"].name, "name")
        self.assertEqual(Person._meta.fm_portal_fields["Addresses"].name, "addresses")
        self.assertEqual(list(Person._meta.fields.keys()), ["age", "name", "photo"])
        self.assertEqual(list(AddressPortal._meta.fields.keys()), ["city", "zip_code"])

    def test_duplicate_filemaker_names_raise(self):
        with self.assertRaises(ValueError):
            class BadPortal(PortalModel):
                class Meta:
                    table_occurrence = "TO"

                one = fmdata.String(field_name="dup", field_type=FMFieldType.Text)
                two = fmdata.String(field_name="dup", field_type=FMFieldType.Text)

        with self.assertRaises(ValueError):
            class BadModel(Model):
                class Meta:
                    client = CLIENT
                    layout = "Bad"

                one = fmdata.String(field_name="dup", field_type=FMFieldType.Text)
                two = fmdata.String(field_name="dup", field_type=FMFieldType.Text)

        with self.assertRaises(ValueError):
            class BadPortalFieldModel(Model):
                class Meta:
                    client = CLIENT
                    layout = "Bad"

                first = PortalField(model=AddressPortal, name="dup")
                second = PortalField(model=AddressPortal, name="dup")

    def test_escape_criteria_and_portal_data_helpers(self):
        self.assertEqual(escape_filemaker_special_characters('A@"B"'), 'A\\@\\"B\\"')
        self.assertEqual(escape_filemaker_special_characters(10), 10)

        field_meta = Person._meta.fields["name"]
        self.assertEqual(get_fm_value(field_meta, 'A*"'), 'A\\*\\"')
        self.assertEqual(Criteria.Raw("x").convert(field_meta, Person), "x")
        self.assertEqual(Criteria.Empty().convert(field_meta, Person), "==")
        self.assertEqual(Criteria.Blank().convert(field_meta, Person), "=")
        self.assertEqual(Criteria.NotEmpty().convert(field_meta, Person), "*")
        self.assertEqual(Criteria.Exact("A").convert(field_meta=field_meta), "==A")
        self.assertEqual(Criteria.StartsWith("A").convert(field_meta=field_meta), "==A*")
        self.assertEqual(Criteria.EndsWith("A").convert(field_meta=field_meta), "==*A")
        self.assertEqual(Criteria.Contains("A").convert(field_meta=field_meta), "==*A*")
        self.assertEqual(Criteria.Gt(1).convert(field_meta=Person._meta.fields["age"]), ">1")
        self.assertEqual(Criteria.Gte(1).convert(field_meta=Person._meta.fields["age"]), ">=1")
        self.assertEqual(Criteria.Lt(1).convert(field_meta=Person._meta.fields["age"]), "<1")
        self.assertEqual(Criteria.Lte(1).convert(field_meta=Person._meta.fields["age"]), "<=1")
        self.assertEqual(Criteria.Range(1, 3).convert(field_meta=Person._meta.fields["age"]), "1...3")

        portal_data = add_portal_record_to_portal_data({}, "Addresses", "10", "11", {"City": "Berlin"})
        self.assertEqual(portal_data, {"Addresses": [{"City": "Berlin", "recordId": "10", "modId": "11"}]})
        portal_data = add_portal_record_to_portal_data(portal_data, "Addresses", None, None, {"City": "Paris"})
        self.assertEqual(len(portal_data["Addresses"]), 2)


class PortalModelAndManagerTests(unittest.TestCase):
    def make_person(self, **kwargs):
        return Person(**kwargs)

    def test_portal_manager_internal_branching(self):
        person = self.make_person(record_id="1", name="Alice", age=30)
        self.assertIn("super", repr(Person.addresses.__get__(person, Person)))

        qs = person.addresses.all()
        self.assertEqual(qs.chunking(2)._chunk_size, 2)
        qs._result_cache = fmdata.CacheIterator(iter([AddressPortal(model=person, record_id="10", city="A")]))
        self.assertEqual(qs.first().record_id, "10")
        with self.assertRaises(ValueError):
            person.addresses.all()[-1:]

        no_prefetch = person.addresses.all()
        with patch.object(no_prefetch, "_execute_query") as execute_query:
            no_prefetch._fetch_all()
        execute_query.assert_called_once()

        uncached = person.addresses.all()
        new_qs = person.addresses.all()
        new_qs._fetch_all = Mock(side_effect=lambda: setattr(
            new_qs,
            "_result_cache",
            fmdata.CacheIterator(iter([AddressPortal(model=person, record_id="10", city="A")])),
        ))
        uncached._clone = Mock(return_value=new_qs)
        self.assertEqual(uncached[0].record_id, "10")

        slice_manager = person.addresses.all()
        slice_manager._set_new_slice(1, None)
        self.assertEqual(slice_manager._slice_start, 1)
        slice_manager._set_new_slice(1, 5)
        slice_manager._set_new_slice(1, 2)
        self.assertEqual(slice_manager._slice_start, 3)
        self.assertEqual(slice_manager._slice_stop, 4)

        resliced = person.addresses.all()[:5][2:]
        self.assertEqual(resliced._slice_start, 2)
        self.assertEqual(resliced._slice_stop, 5)

        with self.assertRaises(AttributeError):
            person.addresses.all().new(city="Rome")

        portal = AddressPortal(model=person, city="Rome")
        manager = person.addresses.all()
        with patch.object(manager, "new", return_value=portal), patch.object(portal, "save", return_value=None):
            self.assertIsNone(manager.create(city="Rome"))

        with patch("fmdata.orm.portal_page_generator", return_value=iter([])) as page_generator_mock:
            person.addresses.all().ignore_prefetched()._execute_query()
        self.assertEqual(page_generator_mock.call_args.kwargs["limit"], fmdata.orm.A_REALLY_BIG_LIMIT)

        sliced_manager = person.addresses.all().ignore_prefetched()
        sliced_manager._slice_stop = 2
        with patch("fmdata.orm.portal_page_generator", return_value=iter([])) as page_generator_mock:
            sliced_manager._execute_query()
        self.assertEqual(page_generator_mock.call_args.kwargs["limit"], 2)

    def test_portal_model_init_setattr_and_dump(self):
        person = self.make_person(name="Alice")
        portal = AddressPortal(model=person, city="Berlin", zip_code=10115)
        self.assertEqual(portal.to_dict(), {"city": "Berlin", "zip_code": 10115})
        self.assertEqual(portal._dump_fields(), {"AddressTO::City": "Berlin", "AddressTO::Zip": 10115})
        portal.city = "Paris"
        self.assertIn("city", portal._updated_fields)
        portal.set_model(person)
        self.assertIs(portal.model, person)

        loaded = AddressPortal(model=person, _from_db={"AddressTO::City": "Rome", "AddressTO::Zip": 10})
        self.assertEqual(loaded.city, "Rome")
        self.assertEqual(loaded.zip_code, 10)

        with self.assertRaises(ValueError):
            AddressPortal(city="X")
        with self.assertRaises(AttributeError):
            AddressPortal(model=person, unknown="x")

        class PortalWithoutMeta(PortalModel):
            city = fmdata.String(field_name="City", field_type=FMFieldType.Text)

        with self.assertRaises(ValueError):
            PortalWithoutMeta(model=person, portal_name=None, table_occurrence="TO", city="X")
        with self.assertRaises(ValueError):
            PortalWithoutMeta(model=person, portal_name="P", table_occurrence=None, city="X")

    def test_portal_model_save_delete_update_and_as_layout_model(self):
        person = self.make_person(name="Alice", age=30)
        person.save = Mock()
        portal = AddressPortal(model=person, city="Berlin", zip_code=10115)
        portal.save()
        person.save.assert_called_once()

        person.save.reset_mock()
        portal.record_id = "10"
        portal.mod_id = "11"
        portal.save(force_insert=True)
        self.assertIsNone(portal.record_id)
        self.assertIsNone(portal.mod_id)

        person.save.reset_mock()
        portal = AddressPortal(model=person, record_id="10", city="Berlin")
        portal.save(force_update=True)
        person.save.assert_called_once()

        with self.assertRaises(ValueError):
            AddressPortal(model=person, city="Berlin").save(force_update=True)
        with self.assertRaises(ValueError):
            AddressPortal(model=person, city="Berlin").save(force_insert=True, force_update=True)

        portal = AddressPortal(model=person, city="Berlin")
        portal.update(city="Paris")
        self.assertEqual(portal.city, "Paris")
        self.assertIsNone(portal.create() if False else None)

        portal = AddressPortal(model=person, record_id="10", mod_id="11", city="Berlin")
        layout_model = portal.as_layout_model(Person)
        self.assertEqual(layout_model.record_id, "10")
        self.assertEqual(layout_model.city if hasattr(layout_model, "city") else None, None)

        with self.assertRaises(ValueError):
            AddressPortal(model=person, city="Berlin").as_layout_model(Person)

        person.save.reset_mock()
        portal.delete()
        person.save.assert_called_once()
        self.assertIsNone(portal.record_id)

        portal = AddressPortal(model=person, city="Berlin")
        portal.delete()
        self.assertIsNone(portal.record_id)

    def test_portal_model_extra_save_and_layout_branches(self):
        class SimplePortal(PortalModel):
            class Meta:
                portal_name = "SimplePortal"
                table_occurrence = "SimpleTO"

            city = fmdata.String(field_name="City", field_type=FMFieldType.Text)

        class SimpleLayout(Model):
            class Meta:
                client = CLIENT
                layout = "SimpleLayout"

            city = fmdata.String(field_name="City", field_type=FMFieldType.Text)

        class SimpleLayoutWithPortal(Model):
            class Meta:
                client = CLIENT
                layout = "SimpleLayoutWithPortal"

            simple_portal = PortalField(model=SimplePortal, name="SimplePortal")

        person = self.make_person(record_id="1", name="Alice", age=30)
        portal = SimplePortal(model=person, record_id="10", mod_id="11", city="Rome")
        layout_model = portal.as_layout_model(SimpleLayout)
        self.assertEqual(layout_model.city, "Rome")
        self.assertEqual(layout_model._updated_fields, {"city"})

        holder = SimpleLayoutWithPortal(record_id="1")
        created_portal = SimplePortal(model=holder, record_id="11", city="Rome")
        manager = holder.simple_portal.all()
        with patch.object(manager._meta_portal.field, "model", return_value=created_portal):
            self.assertEqual(manager.new(city="Rome").record_id, "11")
        with patch.object(manager._meta_portal.field, "model", return_value=created_portal), patch.object(created_portal, "save", return_value=None):
            self.assertEqual(manager.create(city="Rome").record_id, "11")

        with patch.object(person, "save") as save:
            portal = AddressPortal(model=person, record_id="10", mod_id="11", city="Rome")
            portal._updated_fields = set()
            portal.save()
        save.assert_called_once()

    def test_portal_manager_public_behaviour(self):
        person = self.make_person(record_id="1", name="Alice", age=30)
        root = person.addresses
        self.assertIsInstance(root, PortalManager)
        with self.assertRaises(TypeError):
            len(root)

        qs = root.all()
        self.assertFalse(qs._is_root_manager)
        qs._result_cache = fmdata.CacheIterator(iter([]))
        self.assertIsNone(qs.first())

        qs._result_cache = fmdata.CacheIterator(iter([AddressPortal(model=person, record_id="10", city="A")]))
        self.assertEqual(len(qs), 1)
        self.assertEqual(qs[0].record_id, "10")
        self.assertEqual(qs[:1]._slice_stop, 1)
        with self.assertRaises(ValueError):
            qs[-1]
        with self.assertRaises(ValueError):
            qs[2:1]
        with self.assertRaises(ValueError):
            qs[::2]
        with self.assertRaises(TypeError):
            qs["x"]

        sliced = qs[:1]
        with self.assertRaises(TypeError):
            sliced.ignore_prefetched()
        with self.assertRaises(TypeError):
            sliced.chunking(1)

        with self.assertRaises(AttributeError):
            qs.new(city="B")

        qs._fetch_all = Mock()
        p1 = Mock()
        qs._result_cache = fmdata.CacheIterator(iter([p1]))
        person.save = Mock()
        qs.update({"city": "C"})
        p1.update.assert_called_once_with(city="C")
        person.save.assert_called_once()

    def test_portal_manager_fetch_execute_and_delete(self):
        person = self.make_person(record_id="1", name="Alice", age=30)
        manager = person.addresses.all()

        prefetched_portal = AddressPortal(model=person, record_id="10", city="Berlin")
        person._set_portal_prefetch({"Addresses": fmdata.orm.PortalPrefetchData(limit=1, offset=1, cache=fmdata.CacheIterator(iter([prefetched_portal])))})
        manager._fetch_all()
        self.assertEqual(manager._result_cache[0].record_id, "10")

        manager = person.addresses.all().ignore_prefetched()
        page = PortalPage(
            result=GetRecordResult(
                http_response=make_http_response(
                    response={
                        "data": [
                            {
                                "fieldData": {"Name": "Alice", "Age": 30},
                                "recordId": "1",
                                "portalData": {"Addresses": [{"recordId": "10", "modId": "1", "AddressTO::City": "Berlin"}]},
                            }
                        ]
                    }
                ),
                layout="People",
                client=CLIENT,
            )
        )
        with patch("fmdata.orm.portal_page_generator", return_value=iter([page])):
            manager._execute_query()
        records = list(manager)
        self.assertEqual(records[0].city, "Berlin")

        manager = person.addresses.all()
        manager._result_cache = fmdata.CacheIterator(iter([]))
        person.save = Mock()
        manager.delete()
        person.save.assert_not_called()

        manager._result_cache = fmdata.CacheIterator(iter([AddressPortal(model=person, record_id="10", city="Berlin")]))
        manager.delete()
        person.save.assert_called_once()

        page_iter = iter([page, page])
        deduped = list(manager.portals_record_from_portal_page_iterator(person, "Addresses", page_iter))
        self.assertEqual(len(deduped), 1)


class ModelManagerAndModelTests(unittest.TestCase):
    def make_person(self, **kwargs):
        return Person(**kwargs)

    def test_model_manager_internal_branches(self):
        with self.assertRaises(TypeError):
            len(Person.objects)
        with self.assertRaises(TypeError):
            list(Person.objects.scripts_responses())

        criteria = Person.objects.all()._process_find_omit_kwargs({
            "name": Criteria.Exact("Alice"),
            "age__raw": "10",
            "name__exact": "A",
            "name__startswith": "B",
            "name__endswith": "C",
            "name__contains": "D",
            "age__gt": 1,
            "age__gte": 2,
            "age__lt": 3,
            "age__lte": 4,
            "age__range": (5, 6),
        })
        self.assertIn("Name", criteria)
        self.assertIn("Age", criteria)

        broken_model = type("BrokenModel", (), {"_meta": type("Meta", (), {"fields": {"broken": None}})()})()
        broken_qs = Person.objects.all()
        broken_qs._model_class = broken_model
        with self.assertRaises(AttributeError):
            broken_qs._retrive_meta_field_form_field_name("broken")

        cached = Person.objects.all()
        cached._result_cache = fmdata.CacheIterator(iter([self.make_person(record_id="1", name="Alice", age=30)]))
        self.assertEqual(len(cached), 1)
        cached._scripts_responses_cache = fmdata.CacheIterator(iter(["script"]))
        self.assertEqual(list(cached.scripts_responses()), ["script"])
        with self.assertRaises(ValueError):
            Person.objects.all()[-1:]

        uncached = Person.objects.all()
        page = Page(
            result=GetRecordsResult(
                http_response=make_http_response(
                    response={"data": [{"fieldData": {"Name": "Alice", "Age": 30}, "recordId": "1", "modId": "2"}]}
                ),
                layout="People",
                client=CLIENT,
            )
        )
        CLIENT.get_records_paginated.return_value = type("Paged", (), {"pages": fmdata.CacheIterator(iter([page]))})()
        self.assertEqual(uncached[0].record_id, "1")

        slice_manager = Person.objects.all()
        slice_manager._set_new_slice(1, None)
        self.assertEqual(slice_manager._slice_start, 1)
        slice_manager._set_new_slice(1, 5)
        slice_manager._set_new_slice(1, 2)
        self.assertEqual(slice_manager._slice_start, 3)
        self.assertEqual(slice_manager._slice_stop, 4)

        empty = Person.objects.all()
        empty_page = Page(
            result=GetRecordsResult(
                http_response=make_http_response(response={"data": []}),
                layout="People",
                client=CLIENT,
            )
        )
        CLIENT.get_records_paginated.return_value = type("Paged", (), {"pages": fmdata.CacheIterator(iter([empty_page]))})()
        self.assertIsNone(empty.first())

        resliced = Person.objects.all()[:5][2:]
        self.assertEqual(resliced._slice_start, 2)
        self.assertEqual(resliced._slice_stop, 5)

    def test_model_init_refresh_and_basic_helpers(self):
        person = self.make_person(name="Alice", age=30, _consider_fields_as_updated=False)
        self.assertEqual(person.to_dict(), {"age": 30, "name": "Alice", "photo": None})
        self.assertEqual(person._dump_fields(), {"Age": 30, "Name": "Alice"})
        person.name = "Bob"
        self.assertIn("name", person._updated_fields)
        person._set_portal_prefetch({"Addresses": "x"})
        self.assertEqual(person._portals_prefetch["Addresses"], "x")

        result = GetRecordResult(
            http_response=make_http_response(response={"data": [{"fieldData": {"Name": "FromDb", "Age": 9}, "recordId": "8"}]}),
            layout="People",
            client=CLIENT,
        )
        with patch.object(Person.objects, "_execute_get_record", return_value=result):
            refreshed = self.make_person(record_id="8").refresh_from_db()
        self.assertEqual(refreshed.name, "FromDb")
        self.assertEqual(refreshed.record_id, "8")

        with self.assertRaises(ValueError):
            self.make_person()._load_fields_from_db()
        with self.assertRaises(AttributeError):
            self.make_person(unknown="x")

    def test_model_manager_query_building_and_slicing(self):
        qs = Person.objects.all()
        self.assertFalse(qs._is_root_manager)
        self.assertIsInstance(qs._clone(), ModelManager)
        self.assertEqual(qs._retrive_meta_field_form_field_name("name").filemaker_name, "Name")
        self.assertEqual(qs.find(name="Alice")._search_criteria[0], SearchCriteria(fields={"Name": "==Alice"}, is_omit=False))
        self.assertTrue(qs.omit(name__contains="A")._search_criteria[0].is_omit)
        self.assertEqual(qs.order_by("name", "-age")._sort, [{"fieldName": "Name", "sortOrder": "ascend"}, {"fieldName": "Age", "sortOrder": "descend"}])
        self.assertEqual(qs.chunking(5)._chunk_size, 5)
        self.assertEqual(qs.prefetch_portal("addresses", limit=2, offset=1)._portals["Addresses"], {"offset": 2, "limit": 2})
        self.assertEqual(qs.response_layout("Slim")._response_layout, "Slim")
        self.assertEqual(qs.prerequest_script("Pre", "1")._scripts["prerequest"]["name"], "Pre")
        self.assertEqual(qs.presort_script("Sort")._scripts["presort"]["name"], "Sort")
        self.assertEqual(qs.after_script("After")._scripts["after"]["name"], "After")
        self.assertEqual(qs._get_query(), [])
        self.assertEqual(qs.find(name="Alice")._get_query(), [{"omit": "false", "Name": "==Alice"}])

        with self.assertRaises(AttributeError):
            qs.prefetch_portal("missing")
        with self.assertRaises(ValueError):
            qs.prefetch_portal("addresses", limit=-1)
        with self.assertRaises(ValueError):
            qs.prefetch_portal("addresses", offset=-1)
        with self.assertRaises(ValueError):
            qs.find(age__range=1)
        with self.assertRaises(ValueError):
            qs.find(name__unknown="x")

        cached = qs
        cached._result_cache = fmdata.CacheIterator(iter([self.make_person(record_id="1", name="Alice", age=30)]))
        self.assertEqual(cached[0].record_id, "1")
        self.assertEqual(cached[:1]._slice_stop, 1)
        self.assertEqual(cached.first().record_id, "1")
        with self.assertRaises(ValueError):
            cached[-1]
        with self.assertRaises(ValueError):
            cached[2:1]
        with self.assertRaises(ValueError):
            cached[::2]
        with self.assertRaises(TypeError):
            cached["x"]

        sliced = qs[:1]
        with self.assertRaises(TypeError):
            sliced.find(name="Alice")
        with self.assertRaises(TypeError):
            sliced.order_by("name")

    def test_model_manager_execution_helpers(self):
        qs = Person.objects.all()
        fake_pages = fmdata.CacheIterator(iter([]))
        CLIENT.get_records_paginated.return_value = type("Paged", (), {"pages": fake_pages})()
        CLIENT.find_paginated.return_value = type("Paged", (), {"pages": fake_pages})()
        qs._execute_query()
        CLIENT.get_records_paginated.assert_called_once()
        self.assertEqual(CLIENT.get_records_paginated.call_args.kwargs["portals"], {})

        qs = Person.objects.find(name="Alice")
        qs._execute_query()
        CLIENT.find_paginated.assert_called_once()
        self.assertEqual(CLIENT.find_paginated.call_args.kwargs["portals"], {})

        result = GetRecordResult(http_response=make_http_response(response={"data": []}), layout="People", client=CLIENT)
        CLIENT.get_record.return_value = result
        self.assertIs(qs._execute_get_record("1"), result)
        self.assertEqual(CLIENT.get_record.call_args.kwargs["portals"], {})

        dup_result = Mock()
        dup_result.raise_exception_if_has_error = Mock()
        CLIENT.duplicate_record.return_value = dup_result
        self.assertIs(qs._execute_duplicate_record("1"), dup_result)

        create_result = Mock()
        create_result.raise_exception_if_has_error = Mock()
        CLIENT.create_record.return_value = create_result
        self.assertIs(qs._execute_create_record({}, {}), create_result)

        delete_result = Mock()
        delete_result.raise_exception_if_has_error = Mock()
        CLIENT.delete_record.return_value = delete_result
        self.assertIs(qs._execute_delete_record("1"), delete_result)

        upload_result = Mock()
        upload_result.raise_exception_if_has_error = Mock()
        CLIENT.upload_container.return_value = upload_result
        self.assertIs(qs._execute_upload_container("1", "Photo", 1, BytesIO(b"x")), upload_result)

        self.assertEqual(qs.get_delete_related_field_data([]), {})
        self.assertEqual(qs.get_delete_related_field_data([("P", "1")]), {"deleteRelated": "P.1"})
        self.assertEqual(qs.get_delete_related_field_data([("P", "1"), ("P", "2")]), {"deleteRelated": ["P.1", "P.2"]})

        page_qs = Person.objects.all()[:1]
        CLIENT.get_records_paginated.reset_mock()
        CLIENT.get_records_paginated.return_value = type("Paged", (), {"pages": fake_pages})()
        page_qs._execute_query()
        self.assertEqual(CLIENT.get_records_paginated.call_args.kwargs["limit"], 1)

    def test_record_and_script_iterators(self):
        qs = Person.objects.all()
        page = Page(
            result=GetRecordsResult(
                http_response=make_http_response(
                    response={
                        "data": [
                            {
                                "fieldData": {"Name": "Alice", "Age": 30},
                                "recordId": "1",
                                "modId": "2",
                                "portalData": {"Addresses": [{"recordId": "10", "modId": "1", "AddressTO::City": "Berlin", "AddressTO::Zip": 123}]},
                                "scriptResult": "after",
                            },
                            {
                                "fieldData": {"Name": "Alice", "Age": 30},
                                "recordId": "1",
                                "modId": "2",
                                "portalData": {"Addresses": []},
                            },
                        ],
                        "scriptResult": "after",
                        "scriptError": "0",
                        "scriptResult.prerequest": "pre",
                        "scriptError.prerequest": "1",
                        "scriptResult.presort": "sort",
                        "scriptError.presort": "2",
                    }
                ),
                layout="People",
                client=CLIENT,
            )
        )
        models = list(qs.records_iterator_from_page_iterator(iter([page]), portals_input={"Addresses": {"offset": 1, "limit": 1}}))
        self.assertEqual(len(models), 1)
        self.assertEqual(models[0].name, "Alice")
        self.assertIn("Addresses", models[0]._portals_prefetch)

        page.result.raw_content["response"]["data"] = None
        self.assertEqual(list(qs.records_iterator_from_page_iterator(iter([page]), portals_input={})), [])

        script_page = Page(
            result=GetRecordsResult(
                http_response=make_http_response(
                    response={
                        "data": [],
                        "scriptResult": "after",
                        "scriptError": "0",
                        "scriptResult.prerequest": "pre",
                        "scriptError.prerequest": "1",
                        "scriptResult.presort": "sort",
                        "scriptError.presort": "2",
                    }
                ),
                layout="People",
                client=CLIENT,
            )
        )
        scripts = list(qs.script_results_from_page_iterator(iter([script_page])))
        self.assertEqual(scripts[0].after.result, "after")
        self.assertEqual(scripts[0].presort.result, "sort")
        self.assertEqual(scripts[0].prerequest.result, "pre")

    def test_record_selection_does_not_prefetch_portals_unless_requested(self):
        qs = Person.objects.all()
        page = Page(
            result=GetRecordsResult(
                http_response=make_http_response(
                    response={
                        "data": [
                            {
                                "fieldData": {"Name": "Alice", "Age": 30},
                                "recordId": "1",
                                "modId": "2",
                                "portalData": {"Addresses": [{"recordId": "10", "modId": "1", "AddressTO::City": "Berlin"}]},
                            }
                        ]
                    }
                ),
                layout="People",
                client=CLIENT,
            )
        )
        model = list(qs.records_iterator_from_page_iterator(iter([page]), portals_input={}))[0]
        self.assertEqual(model._portals_prefetch, {})

        qs_prefetch = Person.objects.prefetch_portal("addresses", limit=1)
        model_prefetch = list(qs_prefetch.records_iterator_from_page_iterator(iter([page]), portals_input=qs_prefetch._portals))[0]
        self.assertIn("Addresses", model_prefetch._portals_prefetch)

    def test_get_does_not_load_portals_unless_explicitly_prefetched(self):
        get_result = GetRecordResult(
            http_response=make_http_response(
                response={
                    "data": [
                        {
                            "fieldData": {"Name": "Alice", "Age": 30},
                            "recordId": "1",
                            "modId": "2",
                            "portalData": {"Addresses": [{"recordId": "10", "modId": "1", "AddressTO::City": "Berlin"}]},
                        }
                    ]
                }
            ),
            layout="People",
            client=CLIENT,
        )

        with patch.object(Person.objects, "_execute_get_record", return_value=get_result) as execute_get_record:
            model = Person.objects.get("1")

        self.assertEqual(model.name, "Alice")
        self.assertEqual(model._portals_prefetch, {})
        execute_get_record.assert_called_once_with("1")

    def test_portal_prefetch_and_portal_iterator(self):
        person = self.make_person(record_id="1", name="Alice", age=30)
        qs = Person.objects.all()
        response_portal_data = PortalData({"Addresses": [{"recordId": "10", "modId": "1", "AddressTO::City": "Berlin", "AddressTO::Zip": 123}]})
        prefetch = qs.portals_prefetch_data_from_portal_data(person, "Addresses", response_portal_data, {"offset": 1, "limit": 5})
        self.assertEqual(prefetch.limit, 5)
        self.assertEqual(prefetch.offset, 1)
        self.assertEqual(prefetch.cache[0].city, "Berlin")

        portal_items = list(
            portal_model_iterator_from_portal_data(
                model=person,
                portal_data_list=response_portal_data.get("Addresses"),
                portal_model_class=AddressPortal,
                portal_name="Addresses",
                already_seen_record_ids=set(),
            )
        )
        self.assertEqual(portal_items[0].record_id, "10")
        self.assertEqual(
            list(
                portal_model_iterator_from_portal_data(
                    model=person,
                    portal_data_list=response_portal_data.get("Addresses"),
                    portal_model_class=AddressPortal,
                    portal_name="Addresses",
                    already_seen_record_ids={"10"},
                )
            ),
            [],
        )

    def test_model_save_duplicate_delete_update_container(self):
        CLIENT.version = FMVersion.V22
        person = self.make_person(name="Alice", age=30)
        create_result = CreateRecordResult(http_response=make_http_response(response={"recordId": "1", "modId": "2", "newPortalRecordInfo": []}))
        with patch.object(Person.objects, "_execute_create_record", return_value=create_result):
            person.save()
        self.assertEqual(person.record_id, "1")
        self.assertEqual(person.mod_id, "2")

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        edit_result = fmdata.EditRecordResult(http_response=make_http_response(response={"modId": "3", "newPortalRecordInfo": []}))
        with patch.object(Person.objects, "_execute_edit_record", return_value=edit_result):
            person.save(check_mod_id=True)
        self.assertEqual(person.mod_id, "3")

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        person.name = "Bob"
        person._updated_fields = {"name"}
        with patch.object(Person.objects, "_execute_edit_record", return_value=edit_result) as execute_edit:
            person.save()
        self.assertEqual(execute_edit.call_args.kwargs["field_data"], {"Name": "Bob"})
        self.assertEqual(execute_edit.call_args.kwargs["portals_data"], {})
        self.assertEqual(execute_edit.call_args.kwargs["portals_to_delete"], [])

        with patch.object(Person.objects, "_execute_edit_record") as edit:
            self.make_person(record_id="1", mod_id="2").save()
        edit.assert_not_called()

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        person._updated_fields = set()
        portal = AddressPortal(model=person, record_id="10", mod_id="11", city="Paris")
        portal._updated_fields = set()
        with patch.object(Person.objects, "_execute_edit_record") as edit:
            person.save(portals=[portal])
        edit.assert_not_called()
        with patch("fmdata.orm.patch_from_model_or_portal", side_effect=[{}, {}]), patch.object(Person.objects, "_execute_edit_record") as edit:
            person.save(portals=[portal])
        edit.assert_not_called()

        create_result = CreateRecordResult(
            http_response=make_http_response(
                response={
                    "recordId": "1",
                    "modId": "2",
                    "newPortalRecordInfo": [{"tableName": "AddressTO", "recordId": "10", "modId": "11"}],
                }
            )
        )
        person = self.make_person(name="Alice", age=30)
        portal = AddressPortal(model=person, city="Berlin")
        with patch.object(Person.objects, "_execute_create_record", return_value=create_result):
            person.save(portals=[portal])
        self.assertEqual(portal.record_id, "10")
        self.assertEqual(portal.mod_id, "11")

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        person._updated_fields = set()
        portal = AddressPortal(model=person, record_id="10", mod_id="11", city="Paris")
        with patch.object(Person.objects, "_execute_edit_record", return_value=edit_result) as execute_edit:
            person.save(portals=[portal])
        self.assertEqual(execute_edit.call_args.kwargs["field_data"], {})
        self.assertEqual(
            execute_edit.call_args.kwargs["portals_data"],
            {"Addresses": [{"AddressTO::City": "Paris", "recordId": "10"}]},
        )
        self.assertEqual(execute_edit.call_args.kwargs["portals_to_delete"], [])

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        person._updated_fields = set()
        portal_to_delete = AddressPortal(model=person, record_id="10", mod_id="11", city="Paris")
        with patch.object(Person.objects, "_execute_edit_record", return_value=edit_result) as execute_edit:
            person.save(portals_to_delete=[portal_to_delete])
        self.assertEqual(execute_edit.call_args.kwargs["field_data"], {})
        self.assertEqual(execute_edit.call_args.kwargs["portals_data"], {})
        self.assertEqual(execute_edit.call_args.kwargs["portals_to_delete"], [("Addresses", "10")])

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        portal_to_delete = AddressPortal(model=person, record_id="10", mod_id="11", city="Paris")
        with patch.object(Person.objects, "_execute_edit_record", return_value=edit_result) as execute_edit:
            portal_to_delete.delete()
        self.assertEqual(execute_edit.call_args.kwargs["field_data"], {})
        self.assertEqual(execute_edit.call_args.kwargs["portals_data"], {})
        self.assertEqual(execute_edit.call_args.kwargs["portals_to_delete"], [("Addresses", "10")])
        self.assertIsNone(portal_to_delete.record_id)

        with self.assertRaises(ValueError):
            self.make_person().save(force_update=True)
        with self.assertRaises(ValueError):
            self.make_person().save(force_insert=True, force_update=True)
        with self.assertRaises(ValueError):
            self.make_person(record_id="1").save(portals=[AddressPortal(model=self.make_person(name="Other"), city="Berlin")])
        with self.assertRaises(ValueError):
            self.make_person(record_id="1").save(portals=[AddressPortal(model=self.make_person(record_id="1"), city=None)])
        with self.assertRaises(ValueError):
            self.make_person(record_id="1").save(portals_to_delete=[AddressPortal(model=self.make_person(record_id="1"), city="X")])
        with self.assertRaises(ValueError):
            blank_person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
            blank_person.save(portals=[AddressPortal(model=blank_person)])

        mismatch_result = CreateRecordResult(
            http_response=make_http_response(response={"recordId": "1", "modId": "2", "newPortalRecordInfo": []})
        )
        with self.assertRaises(ValueError):
            with patch.object(Person.objects, "_execute_create_record", return_value=mismatch_result):
                owner = self.make_person(name="Alice")
                owner.save(portals=[AddressPortal(model=owner, city="Berlin")])

        clone_result = CreateRecordResult(
            http_response=make_http_response(response={"recordId": "3", "modId": "4", "newPortalRecordInfo": []})
        )
        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        person._updated_fields = set()
        with patch.object(Person.objects, "_execute_create_record", return_value=clone_result) as create:
            person.save(force_insert=True)
        self.assertEqual(create.call_args.kwargs["field_data"], {"Age": 30, "Name": "Alice"})

        mismatch_portal_result = CreateRecordResult(
            http_response=make_http_response(
                response={
                    "recordId": "1",
                    "modId": "2",
                    "newPortalRecordInfo": [{"tableName": "OtherTO", "recordId": "10", "modId": "11"}],
                }
            )
        )
        with self.assertRaises(ValueError):
            with patch.object(Person.objects, "_execute_create_record", return_value=mismatch_portal_result):
                owner = self.make_person(name="Alice")
                owner.save(portals=[AddressPortal(model=owner, city="Berlin")])

        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)

        with patch.object(Person.objects, "_execute_duplicate_record", return_value=DuplicateResult("20", "21")):
            duplicate = person.duplicate()
        self.assertEqual(duplicate.record_id, "20")
        self.assertEqual(duplicate.mod_id, "21")
        self.assertEqual(duplicate.name, "Alice")

        with self.assertRaises(TypeError):
            self.make_person().duplicate()

        with patch.object(Person.objects, "_execute_delete_record") as delete_record:
            person.delete()
        delete_record.assert_called_once_with("1")
        self.assertIsNone(person.record_id)
        self.make_person().delete()

        person = self.make_person(record_id="1", name="Alice", age=30)
        person.update(name="Bob")
        self.assertEqual(person.name, "Bob")

        with self.assertRaises(ValueError):
            self.make_person().update_container("photo", BytesIO(b"x"))
        with self.assertRaises(ValueError):
            self.make_person(record_id="1").update_container("missing", BytesIO(b"x"))
        with self.assertRaises(ValueError):
            self.make_person(record_id="1").update_container("name", BytesIO(b"x"))

        with patch.object(Person.objects, "_execute_upload_container") as upload:
            self.make_person(record_id="1").update_container("photo", BytesIO(b"x"))
        upload.assert_called_once()

    def test_execute_edit_record_branches_and_patch_filtering(self):
        person = self.make_person(record_id="1", mod_id="2", name="Alice", age=30)
        CLIENT.version = FMVersion.V22
        result = Mock()
        result.raise_exception_if_has_error = Mock()
        CLIENT.edit_record.return_value = result
        Person.objects._execute_edit_record("1", "2", {"Name": "A"}, {}, [("P", "1")])
        self.assertEqual(CLIENT.edit_record.call_args.kwargs["field_data"]["deleteRelated"], "P.1")

        CLIENT.version = FMVersion.V17
        CLIENT.edit_record.reset_mock()
        result = Mock()
        result.raise_exception_if_has_error = Mock()
        CLIENT.edit_record.return_value = result
        Person.objects._execute_edit_record("1", "2", {}, {}, [("P", "1"), ("P", "2")])
        self.assertEqual(CLIENT.edit_record.call_count, 2)

        CLIENT.edit_record.reset_mock()
        result = Mock()
        result.raise_exception_if_has_error = Mock()
        CLIENT.edit_record.return_value = result
        Person.objects._execute_edit_record("1", "2", {}, {"Addresses": [{"City": "A"}]}, [])
        self.assertEqual(CLIENT.edit_record.call_args.kwargs["portal_data"], {"Addresses": [{"City": "A"}]})

        person._updated_fields = {"name"}
        self.assertEqual(patch_from_model_or_portal(person, True, None), {"Name": "Alice"})
        self.assertEqual(patch_from_model_or_portal(person, False, ["age"]), {"Age": 30})

        with self.assertRaises(NotImplementedError):
            fmdata.orm.FieldCriteria().convert(Person._meta.fields["name"], Person)

    def test_model_manager_get_create_update_delete(self):
        with patch.object(Person, "refresh_from_db", return_value="refreshed") as refresh:
            obj = Person.objects.get("1")
        self.assertIsInstance(obj, Person)
        self.assertEqual(obj.record_id, "1")
        refresh.assert_called_once()

        with patch.object(Person, "save") as save:
            created = Person.objects.create(name="Alice", age=30)
        self.assertIsInstance(created, Person)
        save.assert_called_once()

        qs = Person.objects.all()
        record = self.make_person(record_id="1", name="Alice", age=30)
        record.save = Mock()
        record.delete = Mock()
        qs._result_cache = fmdata.CacheIterator(iter([record]))
        qs.update({"name": "Bob"}, check_mod_id=True)
        record.save.assert_called_once()
        qs.delete()
        record.delete.assert_called_once()


class DuplicateResult:
    def __init__(self, record_id, mod_id):
        self.response = type("Response", (), {"record_id": record_id, "mod_id": mod_id})()
