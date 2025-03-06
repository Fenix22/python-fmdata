from __future__ import annotations

import dataclasses
from datetime import date, datetime
from functools import cached_property
from typing import Type, Optional, List, Any, Iterator, Iterable, Set, Dict, Union, Tuple

from marshmallow import Schema, fields

from fmdata import FMClient
from fmdata.cache_iterator import CacheIterator
from fmdata.fmclient import portal_page_generator
from fmdata.inputs import SingleSortInput, ScriptsInput, ScriptInput, SinglePortalInput, PortalsInput
from fmdata.results import PageIterator, PortalData, PortalDataList, PortalPageIterator

FM_DATE_FORMAT = "%m/%d/%Y"
FM_DATE_TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def get_meta_attribute(cls, attrs_meta, attribute_name: str, default=None) -> Any:
    """
    Retrieve an attribute from the Meta class, looking up the inheritance chain.
    """
    if attrs_meta:
        if hasattr(attrs_meta, attribute_name):
            return getattr(attrs_meta, attribute_name)

    for base in cls.mro():
        if hasattr(base, "_meta"):
            base_meta = getattr(base, "_meta")
            if base_meta and hasattr(base_meta, attribute_name):
                return getattr(base_meta, attribute_name)

    return default


class FileMakerSchema(Schema):
    class Meta:
        datetimeformat = FM_DATE_TIME_FORMAT
        dateformat = FM_DATE_FORMAT


# ---------------------------
# Common Meta & Field Classes
# ---------------------------

@dataclasses.dataclass
class ModelMetaField:
    name: str
    field: fields.Field

    @cached_property
    def filemaker_name(self) -> str:
        return self.field.data_key or self.name


@dataclasses.dataclass
class ModelMetaPortalField:
    name: str
    field: PortalField

    @cached_property
    def filemaker_name(self) -> str:
        return self.field.name or self.name


@dataclasses.dataclass
class ModelMeta:
    client: FMClient
    layout: str
    base_schema: Type[FileMakerSchema]
    schema_config: dict
    fields: dict[str, ModelMetaField]
    fm_fields: dict[str, ModelMetaField]
    portal_fields: dict[str, ModelMetaPortalField]
    fm_portal_fields: dict[str, ModelMetaPortalField]


@dataclasses.dataclass
class PortalField:
    model: Type[PortalModel]
    name: str


@dataclasses.dataclass
class PortalModelMeta:
    portal_name: str
    base_schema: Type[FileMakerSchema]
    schema_config: dict
    fields: dict[str, ModelMetaField]
    fm_fields: dict[str, ModelMetaField]


class PortalMetaclass(type):
    def __new__(mcls, name, bases, attrs):

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, PortalMetaclass)]
        if not parents:
            return super().__new__(mcls, name, bases, attrs)

        attrs_meta = attrs.pop("Meta", None)

        cls = super().__new__(mcls, name, bases, attrs)

        _meta_fields: dict[str, ModelMetaField] = {}
        _meta_fm_fields: dict[str, ModelMetaField] = {}
        schema_fields = {}

        for attr_name in dir(cls):
            attr_value = getattr(cls, attr_name)

            if isinstance(attr_value, fields.Field):
                schema_fields[attr_name] = attr_value
                model_meta_field = ModelMetaField(name=attr_name, field=attr_value)
                _meta_fields[attr_name] = model_meta_field
                _meta_fm_fields[model_meta_field.filemaker_name] = model_meta_field

        base_schema_cls: Type[FileMakerSchema] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                                    attribute_name="base_schema") or FileMakerSchema

        schema_config = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="schema_config") or {}

        portal_name = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="portal_name")

        cls._meta = PortalModelMeta(
            base_schema=base_schema_cls,
            schema_config=schema_config,
            portal_name=portal_name,
            fields=_meta_fields,
            fm_fields=_meta_fm_fields
        )

        schema_cls = type(f'{name}Schema', (base_schema_cls,), schema_fields)

        cls.schema_class = schema_cls
        cls.schema_instance = schema_cls(**schema_config)

        return cls


class PortalManager:
    def __init__(self):
        self._chunk_size = None
        self._slice_start: int = 0
        self._slice_stop: Optional[int] = None
        self._result_cache: Optional[CacheIterator[PortalModel]] = None
        self._avoid_prefetch_cache = False
        self._only_prefetched = False

    def _set_model(self, model: Model, meta_portal: ModelMetaPortalField):
        self._model = model
        self._meta_portal = meta_portal

    def _clone(self):
        qs = PortalManager()
        qs._model = self._model
        qs._meta_portal = self._meta_portal
        qs._chunk_size = self._chunk_size
        qs._slice_start = self._slice_start
        qs._slice_stop = self._slice_stop
        qs._result_cache = self._result_cache
        qs._avoid_prefetch_cache = self._avoid_prefetch_cache
        qs._only_prefetched = self._only_prefetched

        return qs

    def _fetch_all(self):
        if self._result_cache is not None:
            return

        prefetch_data: PortalPrefetchData = self._model._portals_prefetch.get(self._meta_portal.name)

        if self._only_prefetched:
            if prefetch_data is None:
                raise ValueError(
                    "Cannot use only_prefetched() method without prefetching portal data: model.objects.prefetch_portals('portal_name')")
            self._result_cache = prefetch_data.cache
            return

        # Try to use cache if the request is inside the prefetch data slice
        if prefetch_data is not None:
            prefetch_data_slice_start = prefetch_data.offset - 1
            prefetch_data_slice_stop = prefetch_data_slice_start + prefetch_data.limit

            search_slice_is_inside_prefetch_slice = self._slice_stop is not None and (
                    self._slice_start >= prefetch_data_slice_start and self._slice_stop <= prefetch_data_slice_stop)

            if not self._avoid_prefetch_cache and search_slice_is_inside_prefetch_slice:
                slice_relative_start = self._slice_start - prefetch_data_slice_start
                slice_relative_stop = self._slice_stop - prefetch_data_slice_start
                self._result_cache = prefetch_data.cache[slice_relative_start:slice_relative_stop]
                return

        # In worst case scenario, execute the query
        self._execute_query()

    def __len__(self) -> int:
        self._fetch_all()
        return len(self._result_cache)

    def __iter__(self) -> Iterator[PortalModel]:
        self._fetch_all()
        return iter(self._result_cache)

    def all(self):
        return self

    def first(self):
        for obj in self[:1]:
            return obj
        return None

    def _assert_not_sliced(self):
        if self._is_sliced():
            raise TypeError("Cannot filter a query once a slice has been taken.")

    def _is_sliced(self):
        return self._slice_start != 0 or self._slice_stop is not None

    def _set_new_slice(self, start, stop):
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

    def __getitem__(self, k):
        if isinstance(k, slice):
            if (k.start is not None and k.start < 0) or (k.stop is not None and k.stop < 0):
                raise ValueError("Negative indexing is not supported.")
            if k.stop is not None and k.stop <= (k.start or 0):
                raise ValueError("Stop index must be greater than start index.")

            if self._result_cache is not None:
                return self._result_cache[k]

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

    def avoid_prefetch_cache(self, avoid: bool = True):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._avoid_prefetch_cache = avoid
        return new_qs

    def only_prefetched(self):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._only_prefetched = True
        return new_qs

    def chunk_size(self, size):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._chunk_size = size
        return new_qs

    def create(self, **kwargs):
        portal = self._meta_portal.field.model(model=self._model, portal_name=self._meta_portal.filemaker_name,
                                               **kwargs)
        portal.save()

    def delete(self):
        self._fetch_all()
        portal_records = [portal.record_id for portal in self._result_cache]

        if not portal_records:
            return

        # TODO It seem that old filemaker version does not support multiple portal delete
        # self._model.objects._execute_delete_portal_records(
        #     record_id=self._model.record_id,
        #     portal_name=self._meta_portal.filemaker_name,
        #     portal_record_ids=portal_records,
        # )

        for portal in portal_records:
            self._model.objects._execute_delete_portal_records(
                record_id=self._model.record_id,
                portal_name=self._meta_portal.filemaker_name,
                portal_record_ids=[portal],
            )

    def _execute_query(self):
        offset = self._slice_start + 1
        limit = None

        if self._slice_stop is not None:
            limit = self._slice_stop - self._slice_start

        chunk_size = self._chunk_size
        if chunk_size is None or chunk_size == 0:
            if limit is None:
                raise ValueError(
                    "Cannot execute a query without a limit or chunk size. If you want to retrieve all records, use chunk_size(size) method in the query. Pay attention to the incoherence results it can cause!")

        client: FMClient = self._model._meta.client
        layout = self._model._meta.layout
        record_id = self._model.record_id

        paged_result = portal_page_generator(
            client=client,
            layout=layout,
            record_id=record_id,
            portal_name=self._meta_portal.filemaker_name,
            offset=offset,
            limit=limit,
            page_size=chunk_size,
        )

        self._result_cache = CacheIterator(self.portals_record_from_portal_page_iterator(
            model=self._model,
            portal_fm_name=self._meta_portal.filemaker_name,
            page_iterator=paged_result
        ))

        return self._result_cache

    def portals_record_from_portal_page_iterator(self,
                                                 model: Model,
                                                 portal_fm_name: str,
                                                 page_iterator: PortalPageIterator, ) -> Iterator[PortalModel]:
        portal_field = self._model._meta.fm_portal_fields[portal_fm_name]
        portal_model: Type[PortalModel] = portal_field.field.model

        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            response_data = page.result.response.data
            record_data = response_data[0]
            portal_data_list = record_data.portal_data.get(portal_fm_name)

            yield from portal_model_iterator_from_portal_data(
                model=model,
                portal_data_list=portal_data_list,
                portal_name=portal_field.filemaker_name,
                portal_model_class=portal_model)


class PortalModel(metaclass=PortalMetaclass):
    #Example of Meta
    #
    # class Meta:
    #     base_schema: FileMakerSchema = None
    #     schema_config: dict = None
    #     portal_name: str = None

    def __init__(self, **kwargs):
        self.model: Optional[Model] = kwargs.pop("model", None)
        self.record_id: Optional[str] = kwargs.pop("record_id", None)
        self.mod_id: Optional[str] = kwargs.pop("mod_id", None)
        self._portal_name: str = self._meta.portal_name or kwargs.pop("portal_name")
        _from_db: Optional[dict] = kwargs.pop("_from_db", None)

        self._updated_fields = set()

        for name in self._meta.fields.keys():
            super().__setattr__(name, None)

        if _from_db:
            load_data = {key: _from_db[key] for key in _from_db.keys()
                         if key in self._meta.fm_fields}

            schema_instance: Schema = self.__class__.schema_instance
            fields = schema_instance.load(data=load_data)

            for field_name, value in fields.items():
                super().__setattr__(field_name, value)
        else:
            for key, value in kwargs.items():
                if key in self._meta.fields:
                    super().__setattr__(key, value)
                    self._updated_fields.add(key)
                else:
                    raise AttributeError(f"Field '{key}' does not exist")

    def set_model(self, model: Model):
        self.model = model

    def to_dict(self) -> Dict[str, Any]:
        return {field: getattr(self, field) for field in self._meta.fields}

    def _dump_fields(self):
        schema_instance: Schema = self.__class__.schema_instance
        return schema_instance.dump(self.to_dict())

    def __setattr__(self, attr_name, value):
        meta_field = self._meta.fields.get(attr_name, None)

        if meta_field is not None:
            super().__setattr__(attr_name, value)
            self._updated_fields.add(meta_field.name)
        else:
            super().__setattr__(attr_name, value)

    def save(self,
             force_insert=False,
             force_update=False,
             update_fields=None,
             only_updated_fields=True,
             check_mod_id=False):

        if force_insert and (force_update or update_fields):
            raise ValueError("Cannot force both insert and updating in model saving.")

        record_id_exists = self.record_id is not None

        if (not record_id_exists and not force_update) or (record_id_exists and force_insert):
            patch = patch_from_model_or_portal(model_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=None)

            self.model.objects._execute_create_portal_record(
                record_id=self.model.record_id,
                portal_name=self._portal_name,
                portal_field_data=patch,
            )
        elif not record_id_exists and force_update:
            raise ValueError("Cannot update a record without record_id.")
        elif record_id_exists and not force_insert:

            patch = patch_from_model_or_portal(model_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=update_fields)

            used_mod_id = self.mod_id if check_mod_id else None

            self.model.objects._execute_edit_portal_record(
                record_id=self.model.record_id,
                portal_name=self._portal_name,
                portal_field_data=patch,
                portal_record_id=self.record_id,
                portal_mod_id=used_mod_id,
            )

        else:
            raise ValueError("Impossible case")

        return self

    def delete(self):
        if self.record_id is None:
            return

        self.model.objects._execute_delete_portal_records(
            record_id=self.model.record_id,
            portal_name=self._portal_name,
            portal_record_ids=[self.record_id],
        )

        self.record_id = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Criteria:

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
        return RawCriteria(f"==*{cls.convert_value(value, escape_special_chars)}")

    @classmethod
    def contains(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f"==*{cls.convert_value(value, escape_special_chars)}*")

    @classmethod
    def not_empty(cls):
        return RawCriteria("*")

    @classmethod
    def gt(cls, value: Any, escape_special_chars: bool = True):
        return RawCriteria(f">{cls.convert_value(value, escape_special_chars)}")

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
        return RawCriteria(
            f"{cls.convert_value(from_value, escape_special_chars)}...{cls.convert_value(to_value, escape_special_chars)}")

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


def add_portal_record_to_portal_data(portal_data: dict,
                                     portal_name: str,
                                     portal_record_id: str,
                                     portal_mod_id: Optional[str],
                                     portal_field_data: dict):
    result_data = {
        "recordId": portal_record_id,
        **portal_field_data
    }

    if portal_mod_id is not None:
        result_data["modId"] = portal_mod_id

    portal_data.setdefault(portal_name, []).append(result_data)

    return portal_data


class ModelManager:
    def __init__(self):
        self._search_criteria: List[SearchCriteria] = []
        self._sort: List[SingleSortInput] = []
        self._scripts: ScriptsInput = {}
        self._chunk_size = None
        self._portals: PortalsInput = {}
        self._slice_start: int = 0
        self._slice_stop: Optional[int] = None
        self._response_layout = None
        self._result_cache: Optional[CacheIterator[Model]] = None

    def _set_model_class(self, model_class: Type[Model]):
        self._model_class = model_class
        self._client: FMClient = model_class._meta.client
        self._layout: str = model_class._meta.layout

    def _clone(self):
        qs = ModelManager()
        qs._model_class = self._model_class
        qs._client = self._client
        qs._layout = self._layout
        qs._search_criteria = self._search_criteria[:]
        qs._sort = self._sort[:]
        qs._scripts = self._scripts.copy()
        qs._chunk_size = self._chunk_size
        qs._portals = self._portals
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

    def create(self, **kwargs):
        new_model = self._model_class(**kwargs)
        new_model.save()

        return new_model

    def _process_find_omit_kwargs(self, kwargs):
        criteria = {}
        for key, value in kwargs.items():
            if isinstance(value, FieldCriteria):
                field_name = key
                field_criteria = value
            else:
                if '__' not in key:
                    field_name = key
                    field_criteria = Criteria.exact(value)
                else:
                    field_name, query_type = key.split('__', 1)

                    if query_type == 'raw':
                        field_criteria = Criteria.raw(value)
                    elif query_type == 'exact':
                        field_criteria = Criteria.exact(value)
                    elif query_type == 'startswith':
                        field_criteria = Criteria.starts_with(value)
                    elif query_type == 'endswith':
                        field_criteria = Criteria.ends_with(value)
                    elif query_type == 'contains':
                        field_criteria = Criteria.contains(value)
                    elif query_type == 'gt':
                        field_criteria = Criteria.gt(value)
                    elif query_type == 'gte':
                        field_criteria = Criteria.gte(value)
                    elif query_type == 'lt':
                        field_criteria = Criteria.lt(value)
                    elif query_type == 'lte':
                        field_criteria = Criteria.lte(value)
                    elif query_type == 'range':
                        if not isinstance(value, (list, tuple)):
                            raise ValueError(f"Value for query type 'range' must be a list or tuple, got {type(value)}")
                        field_criteria = Criteria.range(value[0], value[1])
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

    def chunk_size(self, size):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._chunk_size = size
        return new_qs

    def prefetch_portal(self, portal: str, limit: int, offset: int = 1):
        self._assert_not_sliced()

        if limit is None or limit < 0:
            raise ValueError("Limit must a number > 0.")

        if offset is None or offset < 1:
            raise ValueError("Offset must a number >= 1.")

        new_qs = self._clone()

        # Retrive meta field from portal name
        portal_field = self._model_class._meta.portal_fields[portal]
        portal_fm_name = portal_field.filemaker_name

        new_qs._portals[portal_fm_name] = SinglePortalInput(offset=offset, limit=limit)
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

            if self._result_cache is not None:
                return self._result_cache[k]

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

    def first(self):
        for obj in self[:1]:
            return obj
        return None

    def update(self, check_mod_id: bool = False, **kwargs):
        self._fetch_all()

        for record in self:
            record.update(**kwargs)
            record.save(check_mod_id=check_mod_id)

    def delete(self):
        self._fetch_all()

        for record in self:
            record.delete()

    def _get_query(self):
        query = []
        for criteria in self._search_criteria:
            query.append({
                "omit": "true" if criteria.is_omit == True else "false",
                **criteria.fields
            })

        return query

    def _execute_query(self):
        offset = self._slice_start + 1
        limit = None

        if self._slice_stop is not None:
            limit = self._slice_stop - self._slice_start

        chunk_size = self._chunk_size
        if chunk_size is None or chunk_size == 0:
            if limit is None:
                raise ValueError(
                    "Cannot execute a query without a limit or chunk size. If you want to retrieve all records, use chunk_size(size) method in the query. Pay attention to the incoherence results it can cause!")

        sort = None if len(self._sort) == 0 else self._sort
        script = None if len(self._scripts) == 0 else self._scripts

        # Get records in case of no search (find/omit) criteria
        if len(self._search_criteria) == 0:
            paged_result = self._client.get_records_paginated(
                layout=self._layout,
                offset=offset,
                limit=limit,
                portals=self._portals,
                page_size=chunk_size,
                response_layout=self._response_layout,
                sort=sort,
                scripts=script,
            )
        else:
            paged_result = self._client.find_paginated(
                layout=self._layout,
                offset=offset,
                limit=limit,
                portals=self._portals,
                page_size=chunk_size,
                response_layout=self._response_layout,
                sort=sort,
                scripts=script,
                query=self._get_query(),
            )

        self._result_cache = CacheIterator(
            self.records_iterator_from_page_iterator(page_iterator=paged_result.pages.__iter__(),
                                                     portals_input=self._portals))
        return self._result_cache

    def records_iterator_from_page_iterator(self,
                                            page_iterator: PageIterator,
                                            portals_input: PortalsInput) -> Iterator[Model]:
        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            if page.result.response.data is None:
                continue
            for data_entry in page.result.response.data:

                model = self._model_class(
                    record_id=data_entry.record_id,
                    mod_id=data_entry.mod_id,
                    _from_db=data_entry.field_data,
                )

                # In case of portal_prefetch
                portals_prefetch = {}
                if portals_input is not None:
                    for portal_fm_name, portal_value in portals_input.items():
                        portal_prefetch_data: PortalPrefetchData = self.portals_prefetch_data_from_portal_data(
                            model=model,
                            portal_fm_name=portal_fm_name,
                            response_portal_data=data_entry.portal_data,
                            portal_input=portal_value)

                        portals_prefetch[portal_fm_name] = portal_prefetch_data

                model._set_portal_prefetch(portals_prefetch)

                yield model

    def portals_prefetch_data_from_portal_data(self,
                                               model: Model,
                                               portal_fm_name: str,
                                               response_portal_data: PortalData,
                                               portal_input: SinglePortalInput) -> PortalPrefetchData:

        portal_field: ModelMetaPortalField = self._model_class._meta.fm_portal_fields[portal_fm_name]
        portal_model_class: Type[PortalModel] = portal_field.field.model

        # Extract portal data from response
        portal_data_list: PortalDataList = response_portal_data.get(portal_fm_name, [])
        # Generate iterator from portal data
        iterator = portal_model_iterator_from_portal_data(model=model,
                                                          portal_name=portal_field.filemaker_name,
                                                          portal_data_list=portal_data_list,
                                                          portal_model_class=portal_model_class)

        return PortalPrefetchData(
            limit=portal_input['limit'],
            offset=portal_input['offset'],
            cache=CacheIterator(iterator)
        )

    def _execute_get_record(self, record_id):
        result = self._client.get_record(layout=self._layout, record_id=record_id)
        result.raise_exception_if_has_error()

        return result

    def _execute_create_record(self, field_data, portals_data):
        result = self._client.create_record(layout=self._layout, field_data=field_data, portal_data=portals_data)
        result.raise_exception_if_has_error()

        return result

    def _execute_edit_record(self, record_id, mod_id, field_data, portals_data, portal_to_delete):
        delete_related = self.get_delete_related_field_data(portals_to_delete=portal_to_delete)

        if delete_related:
            field_data.update(delete_related)

        result = self._client.edit_record(layout=self._layout,
                                          record_id=record_id,
                                          mod_id=mod_id,
                                          field_data=field_data,
                                          portal_data=portals_data)

        result.raise_exception_if_has_error()

        return result

    def _execute_create_portal_record(self, record_id, portal_name, portal_field_data):
        result = self._client.edit_record(
            record_id=record_id,
            layout=self._layout,
            field_data={},
            portal_data={portal_name: [portal_field_data]})

        result.raise_exception_if_has_error()
        return result

    def _execute_edit_portal_record(self, record_id, portal_name, portal_field_data, portal_record_id, portal_mod_id):

        portal_data = add_portal_record_to_portal_data(
            portal_data={},
            portal_name=portal_name,
            portal_record_id=portal_record_id,
            portal_mod_id=portal_mod_id,
            portal_field_data=portal_field_data)

        result = self._client.edit_record(
            record_id=record_id,
            layout=self._layout,
            field_data={},
            portal_data=portal_data)

        result.raise_exception_if_has_error()
        return result

    def _execute_delete_portal_records(self, record_id, portal_name, portal_record_ids):
        portal_tuple = [(portal_name, portal_record_id) for portal_record_id in portal_record_ids]

        field_data = self.get_delete_related_field_data(portals_to_delete=portal_tuple)

        if not field_data:
            return

        result = self._client.edit_record(
            record_id=record_id,
            layout=self._layout,
            field_data=field_data
        )

        result.raise_exception_if_has_error()
        return result

    def get_delete_related_field_data(self, portals_to_delete: Iterable[Tuple[str, str]]):

        related_records = []
        for portal_name, portal_record_id in portals_to_delete:
            related_records.append(portal_name + "." + portal_record_id)

        if len(related_records) == 0:
            field_data = {}
        elif len(related_records) == 1:
            field_data = {
                "deleteRelated": related_records[0]
            }
        else:
            field_data = {
                "deleteRelated": related_records
            }

        return field_data

    def _execute_delete_record(self, record_id):
        result = self._client.delete_record(layout=self._layout, record_id=record_id)
        result.raise_exception_if_has_error()

        return result


@dataclasses.dataclass
class PortalPrefetchData:
    limit: int
    offset: int
    cache: CacheIterator[PortalModel]


class ModelMetaclass(type):
    def __new__(mcls, name, bases, attrs):
        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, ModelMetaclass)]
        if not parents:
            return super().__new__(mcls, name, bases, attrs)

        attrs_meta = attrs.pop("Meta", None)

        cls = super().__new__(mcls, name, bases, attrs)

        _meta_fields: dict[str, ModelMetaField] = {}
        _meta_fm_fields: dict[str, ModelMetaField] = {}
        _meta_portal_fields: dict[str, ModelMetaPortalField] = {}
        _meta_fm_portal_fields: dict[str, ModelMetaPortalField] = {}

        schema_fields = {}
        schema_portal_fields = {}

        for attr_name in dir(cls):
            attr_value = getattr(cls, attr_name)

            if isinstance(attr_value, fields.Field):
                schema_fields[attr_name] = attr_value
                model_meta_field = ModelMetaField(name=attr_name, field=attr_value)
                _meta_fields[attr_name] = model_meta_field
                _meta_fm_fields[model_meta_field.filemaker_name] = model_meta_field

            if isinstance(attr_value, PortalField):
                schema_portal_fields[attr_name] = attr_value
                model_portal_meta_field = ModelMetaPortalField(name=attr_name, field=attr_value)
                _meta_portal_fields[attr_name] = model_portal_meta_field
                _meta_fm_portal_fields[model_portal_meta_field.filemaker_name] = model_portal_meta_field

        base_schema_cls: Type[FileMakerSchema] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                                    attribute_name="base_schema") or FileMakerSchema

        schema_config = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="schema_config") or {}

        client: FMClient = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="client")
        layout: str = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="layout")

        base_manager: Type[ModelManager] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="base_manager") or ModelManager

        cls._meta = ModelMeta(
            client=client,
            layout=layout,
            base_schema=base_schema_cls,
            schema_config=schema_config,
            fields=_meta_fields,
            fm_fields=_meta_fm_fields,
            portal_fields=_meta_portal_fields,
            fm_portal_fields=_meta_fm_portal_fields
        )

        schema_cls = type(f'{name}Schema', (base_schema_cls,), schema_fields)
        cls.schema_class = schema_cls
        cls.schema_instance = schema_cls(**schema_config)

        manager = base_manager()
        manager._set_model_class(cls)
        cls.objects = manager

        return cls


class Model(metaclass=ModelMetaclass):
    # Example of Meta:
    #
    # class Meta:
    #     client: FMClient = None
    #     layout: str = None
    #     base_schema: FileMakerSchema = None
    #     schema_config: dict = None

    #TODO not used. Only for type hint
    objects: ModelManager = ModelManager()

    def __init__(self, **kwargs):
        self.record_id: Optional[str] = kwargs.pop("record_id", None)
        self.mod_id: Optional[str] = kwargs.pop("mod_id", None)
        self._portals_prefetch: dict[str, PortalPrefetchData] = kwargs.pop("_portals_prefetch", {})
        _from_db: Optional[dict] = kwargs.pop("_from_db", None)

        self._updated_fields = set()

        # Set portal manager for each portal field
        for portal_name, portal_field in self._meta.portal_fields.items():
            portal_manager = PortalManager()
            portal_manager._set_model(model=self, meta_portal=portal_field)

            super().__setattr__(portal_name, portal_manager)

        for name in self._meta.fields.keys():
            super().__setattr__(name, None)

        if _from_db:
            load_data = {key: _from_db[key] for key in _from_db.keys()
                         if key in self._meta.fm_fields}

            schema_instance: Schema = self.__class__.schema_instance
            fields = schema_instance.load(data=load_data)

            for field_name, value in fields.items():
                super().__setattr__(field_name, value)
        else:
            for key, value in kwargs.items():
                if key in self._meta.fields:
                    super().__setattr__(key, value)
                    self._updated_fields.add(key)
                else:
                    raise AttributeError(f"Field '{key}' does not exist")

    def _set_portal_prefetch(self, portal_prefetch: dict[str, PortalPrefetchData]):
        self._portals_prefetch = portal_prefetch

    def _load_fields_from_db(self):
        if self.record_id is None:
            raise ValueError("Cannot refresh record that has not been saved yet.")

        result = self.objects._execute_get_record(self.record_id)
        record_data = result.response.data[0]

        load_data = {key: value for key, value in record_data.field_data.items() if key in self._meta.fm_fields}
        schema_instance: Schema = self.__class__.schema_instance
        fields = schema_instance.load(data=load_data)

        for field_name, value in fields.items():
            super().__setattr__(field_name, value)
            self._updated_fields.discard(field_name)

        self.record_id = record_data.record_id

    def to_dict(self) -> Dict[str, Any]:
        return {field: getattr(self, field) for field in self._meta.fields}

    def _dump_fields(self):
        schema_instance: Schema = self.__class__.schema_instance
        return schema_instance.dump(self.to_dict())

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

    def save(self,
             force_insert=False,
             force_update=False,
             update_fields=None,
             only_updated_fields=True,
             check_mod_id=False,
             portals: Iterable[Union[PortalModel, SavePortalsConfig]] = (),
             portals_to_delete: Iterable[PortalModel] = ()):

        if force_insert and (force_update or update_fields):
            raise ValueError("Cannot force both insert and updating in model saving.")

        record_id_exists = self.record_id is not None

        # Portal save
        portals_input: PortalsInput = {}

        for config in portals:
            if isinstance(config, PortalModel):
                config = SavePortalsConfig(portal=config, check_mod_id=False, update_fields=None,
                                           only_updated_fields=True)

            portal = config.portal
            model: Model = portal.model

            if not model == self:
                raise ValueError("Portal model must be related to this record.")

            used_mod_id = portal.mod_id if config.check_mod_id else None

            patch = patch_from_model_or_portal(model_portal=portal,
                                               only_updated_fields=config.only_updated_fields,
                                               update_fields=config.update_fields)

            add_portal_record_to_portal_data(portal_data=portals_input,
                                             portal_name=portal._portal_name,
                                             portal_record_id=portal.record_id,
                                             portal_mod_id=used_mod_id,
                                             portal_field_data=patch)

        # Execute
        if (not record_id_exists and not force_update) or (record_id_exists and force_insert):
            patch = patch_from_model_or_portal(model_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=None)

            result = self.objects._execute_create_record(field_data=patch, portals_data=portals_input)

            self.record_id = result.response.record_id
            self.mod_id = result.response.mod_id
        elif not record_id_exists and force_update:
            raise ValueError("Cannot update a record without record_id.")
        elif record_id_exists and not force_insert:
            patch = patch_from_model_or_portal(model_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=update_fields, )

            used_mod_id = self.mod_id if check_mod_id else None

            # Portal delete
            portals_to_delete_record_ids = [(portal._portal_name, portal.record_id) for portal in portals_to_delete]

            result = self.objects._execute_edit_record(record_id=self.record_id,
                                                       mod_id=used_mod_id,
                                                       field_data=patch,
                                                       portals_data=portals_input,
                                                       portal_to_delete=portals_to_delete_record_ids)

            self.mod_id = result.response.mod_id
        else:
            raise ValueError("Impossible case")

        return self

    def delete(self):
        if self.record_id is None:
            return

        self.objects._execute_delete_record(self.record_id)
        self.record_id = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def patch_from_model_or_portal(model_portal: Union[PortalModel, Model], only_updated_fields, update_fields):
    patch = model_portal._dump_fields()
    if update_fields is not None:
        patch = {key: value for key, value in patch.items()
                 if key in update_fields}
    if only_updated_fields:
        patch = {key: value for key, value in patch.items()
                 if model_portal._meta.fm_fields[key].name in model_portal._updated_fields}
    return patch


@dataclasses.dataclass
class SavePortalsConfig:
    portal: PortalModel
    check_mod_id: bool
    update_fields: Optional[Set[str]]
    only_updated_fields: bool = True


@dataclasses.dataclass
class SearchCriteria:
    fields: dict[str, Any]
    is_omit: bool


def portal_model_iterator_from_portal_data(model: Model, portal_data_list, portal_model_class: Type[PortalModel],
                                           portal_name=None) -> \
        Iterator[PortalModel]:
    for single_portal_data_value in portal_data_list:
        yield portal_model_class(
            model=model,
            portal_name=portal_name,
            record_id=single_portal_data_value.record_id,
            mod_id=single_portal_data_value.mod_id,
            _from_db=single_portal_data_value.fields)
