from __future__ import annotations

import dataclasses
from datetime import date, datetime
from functools import cached_property
from typing import Type, Optional, List, Any, Iterator

from marshmallow import Schema, fields

from fmdata import FMClient
from fmdata.cache_iterator import CacheIterator
from fmdata.inputs import SingleSortInput, ScriptsInput, ScriptInput
from fmdata.results import PageIterator

FM_DATE_FORMAT = "%m/%d/%Y"
FM_DATE_TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"


class FileMakerSchema(Schema):
    class Meta:
        # datetimeformat = "%Y-%m"
        dateformat = FM_DATE_FORMAT


def read_field_from_Meta(field_name, bases, namespace):
    meta = namespace.get("Meta")
    field_value = getattr(meta, field_name, None)
    if field_value is None:
        for base_ in bases:
            if hasattr(base_, "Meta"):
                field_value = getattr(base_.Meta, field_name, None)
                if field_value is not None:
                    break
    return field_value


@dataclasses.dataclass
class ModelMetaField:
    name: str
    field: fields.Field

    @cached_property
    def filemaker_name(self) -> str:
        return self.field.data_key or self.name


@dataclasses.dataclass
class ModelMeta:
    fields: dict[str, ModelMetaField]
    fm_fields: dict[str, ModelMetaField]


class FMCriteria:

    @classmethod
    def raw(cls, value: str, escape_special_chars: bool = False):
        return RawCriteria(cls._eventually_escape_special_chars(value, escape_special_chars))

    @classmethod
    def empty(cls):
        return RawCriteria("==")

    @classmethod
    def blank(cls):
        return RawCriteria("=")

    @classmethod
    def exact(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"=={cls.convert_value(value, escape_special_chars)}")

    @classmethod
    def starts_with(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"=={cls.convert_value(value, escape_special_chars)}*")

    @classmethod
    def ends_with(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"==*{cls.convert_value(value,escape_special_chars)}")

    @classmethod
    def contains(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"==*{cls.convert_value(value,escape_special_chars)}*")

    @classmethod
    def not_empty(cls):
        return RawCriteria("*")

    @classmethod
    def gt(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f">{cls.convert_value(value,escape_special_chars)}")

    @classmethod
    def gte(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f">={cls.convert_value(value, escape_special_chars)}")

    @classmethod
    def lt(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"<{cls.convert_value(value, escape_special_chars)}")

    @classmethod
    def lte(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"<={cls.convert_value(value, escape_special_chars)}")

    @classmethod
    def range(cls, from_value: Any, to_value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"{cls.convert_value(from_value, escape_special_chars)}...{cls.convert_value(to_value, escape_special_chars)}")

    @classmethod
    def _eventually_escape_special_chars(cls, value, escape_special_chars: bool):
        if escape_special_chars:
            return cls.escape_filemaker_special_characters(value)
        return value

    @classmethod
    def convert_value(cls, value: Any, escape_special_chars: bool) -> str:
        if value is None:
            raise ValueError("Value cannot be None, use FMCriteria.empty() or FMCriteria.blank() instead.")

        if isinstance(value, str):
            ret_value = value
        elif isinstance(value, int):
            ret_value = str(value)
        elif isinstance(value, float):
            ret_value = str(value)
        elif isinstance(value, bool):
            ret_value = "1" if value else "0"
        elif isinstance(value, date):
            ret_value = value.strftime(FM_DATE_FORMAT)
        elif isinstance(value, datetime):
            ret_value = value.strftime(FM_DATE_TIME_FORMAT)
        else:
            raise ValueError(f"Unsupported value type {type(value)}")

        if escape_special_chars:
            ret_value = cls.escape_filemaker_special_characters(ret_value)

        return ret_value

    @staticmethod
    def escape_filemaker_special_characters(s: str) -> str:
        """
        Escapes FileMaker special characters in the input string.

        FileMaker treats these characters as operators in finds:
          @, *, #, ?, !, =, <, >, and "

        This function returns a new string where each occurrence of any of these
        characters is prefixed by a backslash.

        Example:
          Input: 'Price>100 and "Discount"'
          Output: 'Price\>100 and \"Discount\"'
        """
        # List of characters that FileMaker treats specially.
        special_chars = '@*#?!=<>"'
        # Create a mapping from each character's ordinal to its escaped version.
        mapping = {ord(c): f"\\{c}" for c in special_chars}
        # Translate the input string using the mapping.
        return s.translate(mapping)



class FieldCriteria:
    def convert(self, schema: FileMakerSchema, fm_file_name, field_name) -> str:
        raise NotImplementedError()



@dataclasses.dataclass
class RawCriteria(FieldCriteria):
    value: str

    def convert(self, schema: FileMakerSchema, fm_file_name, field_name) -> str:
        return self.value


class FMManager:
    def __init__(self):
        self._search_criteria: List[SearchCriteria] = []
        self._sort: List[SingleSortInput] = []
        self._scripts: ScriptsInput = {}
        self._chunk_size = 1000
        self._slice_start: int = 0
        self._slice_stop: Optional[int] = None
        self._response_layout = None
        self._result_cache: Optional[CacheIterator[FMModel]] = None

    def _set_model_class(self, model_class: Type[FMModel]):
        self._model_class = model_class
        self._client: FMClient = model_class.Meta.client
        self._layout: str = model_class.Meta.layout

    def _clone(self):
        qs = FMManager()
        qs._model_class = self._model_class
        qs._client = self._client
        qs._layout = self._layout
        qs._search_criteria = self._search_criteria[:]
        qs._sort = self._sort[:]
        qs._scripts = self._scripts.copy()
        qs._chunk_size = self._chunk_size
        qs._slice_start = self._slice_start
        qs._slice_stop = self._slice_stop
        qs._response_layout = self._response_layout
        qs._result_cache = self._result_cache

        return qs

    def _fetch_all(self):
        if self._result_cache is None:
            self._execute_query()

    def __len__(self):
        self._fetch_all()
        return len(self._result_cache)

    def __iter__(self):
        self._fetch_all()
        return iter(self._result_cache)

    def all(self):
        return self._clone()

    def _process_find_omit_kwargs(self, kwargs):
        criteria = {}
        for key, value in kwargs.items():
            if isinstance(value, FieldCriteria):
                field_name = key
                field_criteria = value
            else:
                if '__' not in key:
                    field_name = key
                    field_criteria = FMCriteria.exact(value)
                else:
                    field_name, query_type = key.split('__', 1)

                    if query_type == 'raw':
                        field_criteria = FMCriteria.raw(value)
                    elif query_type == 'exact':
                        field_criteria = FMCriteria.exact(value)
                    elif query_type == 'startswith':
                        field_criteria = FMCriteria.starts_with(value)
                    elif query_type == 'endswith':
                        field_criteria = FMCriteria.ends_with(value)
                    elif query_type == 'contains':
                        field_criteria = FMCriteria.contains(value)
                    elif query_type == 'gt':
                        field_criteria = FMCriteria.gt(value)
                    elif query_type == 'gte':
                        field_criteria = FMCriteria.gte(value)
                    elif query_type == 'lt':
                        field_criteria = FMCriteria.lt(value)
                    elif query_type == 'lte':
                        field_criteria = FMCriteria.lte(value)
                    elif query_type == 'range':
                        if not isinstance(value, (list, tuple)):
                            raise ValueError(f"Value for query type 'range' must be a list or tuple, got {type(value)}")
                        field_criteria = FMCriteria.range(value[0], value[1])
                    else:
                        raise ValueError(f"Unknown query type '{query_type}' on field '{key}'")

            field = self._retrive_meta_field_form_field_name(field_name)
            criteria[field.filemaker_name] = field_criteria.convert(schema=self._model_class.schema_instance,
                                                                    fm_file_name=field.filemaker_name,
                                                                    field_name=field_name)

        return criteria

    def _assert_not_sliced(self):
        if self._is_sliced():
            raise TypeError("Cannot filter a query once a slice has been taken.")

    def find(self, **kwargs):
        self._assert_not_sliced()

        new_qs = self._clone()
        criteria = self._process_find_omit_kwargs(kwargs)
        new_qs._search_criteria.append(SearchCriteria(fields=criteria, is_omit=False))
        return new_qs

    def omit(self, **kwargs):
        self._assert_not_sliced()

        new_qs = self._clone()
        criteria = self._process_find_omit_kwargs(kwargs)
        new_qs._search_criteria.append(SearchCriteria(fields=criteria, is_omit=True))
        return new_qs

    def _retrive_meta_field_form_field_name(self, field_name) -> ModelMetaField:
        res = self._model_class._meta.fields[field_name]

        if res is None:
            raise AttributeError(f"Field '{field_name}' does not exist")

        return res

    def order_by(self, *fields):
        self._assert_not_sliced()

        """Add sort options."""
        new_qs = self._clone()

        for field_name in fields:
            direction = "ascend"
            if field_name.startswith('-'):
                direction = "descend"
                field_name = field_name[1:]

            field = self._retrive_meta_field_form_field_name(field_name)

            new_qs._sort.append(SingleSortInput(fieldName=field.filemaker_name, sortOrder=direction))

        return new_qs

    def chunk(self, size):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._chunk_size = size
        return new_qs

    def response_layout(self, response_layout):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._response_layout = response_layout
        return new_qs

    def pre_request_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts.prerequest = ScriptInput(name=name, param=param)
        return new_qs

    def pre_sort_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts.presort = ScriptInput(name=name, param=param)
        return new_qs

    def after_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts.after = ScriptInput(name=name, param=param)
        return new_qs

    def __getitem__(self, k):
        if isinstance(k, slice):
            if (k.start is not None and k.start < 0) or (k.stop is not None and k.stop < 0):
                raise ValueError("Negative indexing is not supported.")
            if k.stop is not None and k.stop <= (k.start or 0):
                raise ValueError("Stop index must be greater than start index.")

            new_qs = self._clone()
            new_qs._set_new_slice(k.start, k.stop)

            # In case step is present, the list() force the execution of the query then use the list step to provide the result
            return list(new_qs)[::k.step] if k.step else new_qs

        elif isinstance(k, int):
            if k < 0:
                raise ValueError("Negative indexing is not supported.")

            new_qs = self._clone()
            new_qs._set_new_slice(k, k + 1)
            new_qs._fetch_all()

            return self._result_cache[0]

        else:
            raise TypeError(
                "QuerySet indices must be integers or slices, not %s."
                % type(k).__name__
            )

    def _is_sliced(self):
        return self._slice_start != 0 or self._slice_stop is not None

    def _set_new_slice(self, start, stop):
        print("set new slice", start, stop)
        # Trick to manage multiple slicing before executing the query
        if stop is not None:
            if self._slice_stop is not None:
                self._slice_stop = min(self._slice_stop, self._slice_start + stop)
            else:
                self._slice_stop = self._slice_start + stop
        if start is not None:
            if self._slice_stop is not None:
                self._slice_start = min(self._slice_stop, self._slice_start + start)
            else:
                self._slice_start = self._slice_start + start

        print("setted", self._slice_start, self._slice_stop)

    def first(self):
        return self[0]

    def update(self, check_mod_id: bool = False, **kwargs):
        self._fetch_all()

        for record in self:
            record.update(**kwargs)
            record.save(check_mod_id=check_mod_id)

    def delete(self, **kwargs):
        self._fetch_all()

        for record in self:
            record.delete(**kwargs)

    def _get_query(self):
        query = []
        for criteria in self._search_criteria:
            query.append({
                "omit": "true" if criteria.is_omit == True else "false",
                **criteria.fields
            })

        return query

    def _execute_query(self):
        # TODO
        print("execute query")

        offset = self._slice_start + 1
        limit = None

        if self._slice_stop is not None:
            limit = self._slice_stop - self._slice_start

        sort = None if len(self._sort) == 0 else self._sort
        script = None if len(self._scripts) == 0 else self._scripts

        # Get records in case of no search (find/omit) criteria
        if len(self._search_criteria) == 0:
            paged_result = self._client.get_records_paginated(
                layout=self._layout,
                offset=offset,
                limit=limit,
                page_size=self._chunk_size,
                response_layout=self._response_layout,
                sort=sort,
                scripts=script,
            )
        else:
            paged_result = self._client.find_paginated(
                layout=self._layout,
                offset=offset,
                limit=limit,
                page_size=self._chunk_size,
                response_layout=self._response_layout,
                sort=sort,
                scripts=script,
                query=self._get_query(),
            )

        self._result_cache = CacheIterator(self.records_iterator_from_page_iterator(paged_result.pages.__iter__()))
        return self._result_cache

    def records_iterator_from_page_iterator(self, page_iterator: PageIterator) -> Iterator[FMModel]:
        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            if page.result.response.data is None:
                continue
            for data_entry in page.result.response.data:
                yield self._model_class(
                    record_id=data_entry.record_id,
                    mod_id=data_entry.mod_id,
                    _from_db=True,
                    **data_entry.field_data
                )


class ModelMetaclass(type):
    def __new__(mcls, name, bases, namespace):
        _meta_fields: dict[str, ModelMetaField] = {}
        _meta_fm_fields: dict[str, ModelMetaField] = {}
        schema_fields = {}

        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, fields.Field):
                schema_fields[attr_name] = attr_value
                model_meta_field = ModelMetaField(name=attr_name, field=attr_value)
                _meta_fields[attr_name] = model_meta_field
                _meta_fm_fields[model_meta_field.filemaker_name] = model_meta_field

        for name in _meta_fields.keys():
            namespace.pop(name)

        cls = super().__new__(mcls, name, bases, namespace)

        # TODO probably cls.Meta is good enough
        base_schema_cls: Type[FileMakerSchema] = read_field_from_Meta("base_schema", bases,
                                                                      namespace) or FileMakerSchema
        schema_cls = type(f'{name}Schema', (base_schema_cls,), schema_fields)
        schema_config = read_field_from_Meta("schema_config", bases, namespace) or {}

        cls._meta = ModelMeta(fields=_meta_fields, fm_fields=_meta_fm_fields)
        cls.schema_class = schema_cls
        cls.schema_instance = schema_cls(**schema_config)

        manager = None
        if hasattr(cls, 'objects'):
            manager = cls.objects

        if manager is None:
            manager = FMManager()
            cls.objects = manager

        manager._set_model_class(cls)

        return cls


# class PortalSchema(metaclass=ModelMetaclass):
#     class Meta:
#         base_schema: FileMakerSchema = None
#         schema_config: dict = None
#
#     def __init__(self, **kwargs):
#         self._changed_fields = self.__class__.schema_instance.load(data=kwargs)
#         self._record: Optional[PortalData] = None


# @dataclasses.dataclass()
# class RecordState:
#     record_id: Optional[str]
#     mod_id: Optional[str]
#     fields: dict[str, Any]
#     updated_fields: Set[str]

# TODO no _record_state and field in obj
class FMModel(metaclass=ModelMetaclass):
    class Meta:
        client: FMClient = None
        layout: str = None
        base_schema: FileMakerSchema = None
        schema_config: dict = None

    objects: FMManager

    def __init__(self, **kwargs):
        self.record_id: Optional[str] = kwargs.pop("record_id", None)
        self.mod_id: Optional[str] = kwargs.pop("mod_id", None)
        _from_db: bool = kwargs.pop("_from_db", False)

        self._updated_fields = set()

        if _from_db:
            load_data = {key: kwargs[key] for key in kwargs.keys()
                         if key in self._meta.fm_fields}

            schema_instance: Schema = self.__class__.schema_instance
            fields = schema_instance.load(data=load_data)

            for field_name, value in fields.items():
                setattr(self, field_name, value)
                self._updated_fields.discard(field_name)
        else:
            for key, value in kwargs:
                if key in self._meta.fields:
                    setattr(self, key, value)
                    self._updated_fields.add(key)

    def _load_fields_from_db(self):
        if self.record_id is None:
            raise ValueError("Cannot refresh record that has not been saved yet.")

        result = self.Meta.client.get_record(layout=self.Meta.layout, record_id=self.record_id)
        result.raise_exception_if_has_error()

        record_data = result.response.data[0]

        load_data = {key: value for key, value in record_data.field_data.items() if key in self._meta.fm_fields}
        schema_instance: Schema = self.__class__.schema_instance
        fields = schema_instance.load(data=load_data)

        for field_name, value in fields.items():
            setattr(self, field_name, value)
            self._updated_fields.discard(field_name)

        self.record_id = record_data.record_id

    def to_dict(self):
        result: result[str, Any] = {}

        for field_name in self._meta.fields.keys():
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = value

        return result

    def _dump_fields(self):
        schema_instance: Schema = self.__class__.schema_instance
        return schema_instance.dump(self.to_dict())

    def __getattr__(self, attr_name):
        meta_field = self._meta.fields.get(attr_name, None)
        if meta_field is not None:
            return None
        else:
            raise AttributeError(f"Attribute '{attr_name}' not found")

    def __setattr__(self, attr_name, value):
        meta_field = self._meta.fields.get(attr_name, None)

        if meta_field is not None:
            super().__setattr__(attr_name, value)
            self._updated_fields.add(meta_field.name)
        else:
            super().__setattr__(attr_name, value)

    def refresh_from_db(self):
        self._load_fields_from_db()
        return self

    def save(self, force_insert=False, force_update=False, update_fields=None, only_updated_fields=True,
             check_mod_id=False):

        if force_insert and (force_update or update_fields):
            raise ValueError("Cannot force both insert and updating in model saving.")

        record_id_exists = self.record_id is not None

        if (not record_id_exists and not force_update) or (record_id_exists and force_insert):
            result = self.Meta.client.create_record(layout=self.Meta.layout,
                                                    field_data=self._dump_fields())
            result.raise_exception_if_has_error()

            self.record_id = result.response.record_id
            self.mod_id = result.response.mod_id
        elif not record_id_exists and force_update:
            raise ValueError("Cannot update a record without record_id.")
        elif record_id_exists and not force_insert:
            patch = self._dump_fields()

            if update_fields is not None:
                patch = {key: value for key, value in patch.items()
                         if key in update_fields}

            if only_updated_fields:
                patch = {key: value for key, value in patch.items()
                         if key in self._updated_fields}

            used_mod_id = self.mod_id if check_mod_id else None

            result = self.Meta.client.edit_record(layout=self.Meta.layout,
                                                  record_id=self.record_id,
                                                  mod_id=used_mod_id,
                                                  field_data=patch)
            result.raise_exception_if_has_error()

            self.mod_id = result.response.mod_id
        else:
            raise ValueError("Impossible case")

        return self

    def delete(self, delete_also_portals_records: bool = False):
        if self.record_id is None:
            return

        if delete_also_portals_records:  # TODO
            pass

        self.Meta.client.delete_record(layout=self.Meta.layout, record_id=self.record_id)
        self.record_id = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@dataclasses.dataclass
class SearchCriteria:
    fields: dict[str, Any]
    is_omit: bool

# class FMPortalManager:
#
#     def __init__(self, portal_model_cls: Type[FMModel] = None):
#         self._portal_model_class = portal_model_cls
#         self._chunk_size = 100
#         self._offset = None
#         self._limit = None
#
#     def _clone(self):
#         qs = FMPortalManager(portal_model_cls=self._portal_model_class)
#         qs._chunk_size = self._chunk_size
#         qs._offset = self._offset
#         qs._limit = self._limit
#
#         return qs
#
#     def all(self):
#         return self._clone()
#
#     def __getitem__(self, key):
#         if isinstance(key, slice):
#             if (key.start is not None and key.start < 0) or (key.stop is not None and key.stop < 0):
#                 raise ValueError("Negative indexing is not supported.")
#             start = key.start or 1
#             stop = key.stop
#         elif isinstance(key, int):
#             if key < 0:
#                 raise ValueError("Negative indexing is not supported.")
#             start = key
#             stop = start + 1
#         else:
#             raise TypeError(
#                 "QuerySet indices must be integers or slices, not %s."
#                 % type(key).__name__
#             )
#
#         new_qs = self._clone()
#         new_qs._offset = start
#
#         if stop is None:
#             new_qs._limit = None
#         else:
#             new_qs._limit = stop - start
#
#         return new_qs._execute_query()
