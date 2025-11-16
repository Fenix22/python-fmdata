from __future__ import annotations

import dataclasses
import itertools
from functools import cached_property
from typing import Type, Optional, List, Any, Iterator, Iterable, Set, Dict, Union, Tuple, IO, TypeVar

from marshmallow import Schema, fields

from fmdata import FMClient, fmd_fields, FMVersion
from fmdata.cache_iterator import CacheIterator
from fmdata.fmclient import portal_page_generator, fm_version_gte
from fmdata.inputs import SingleSortInput, ScriptsInput, ScriptInput, SinglePortalInput, PortalsInput
from fmdata.results import PageIterator, PortalData, PortalDataList, PortalPageIterator, Page, PortalPage

FM_DATE_FORMAT = "%m/%d/%Y"
FM_DATE_TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"
A_REALLY_BIG_LIMIT = 1000000000


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


@dataclasses.dataclass(frozen=True)
class ScriptsResponse:
    prerequest: Optional[ScriptResponse] = None
    presort: Optional[ScriptResponse] = None
    after: Optional[ScriptResponse] = None


@dataclasses.dataclass(frozen=True)
class ScriptResponse:
    result: str
    error: str


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
    table_occurrence_name: str
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

                field_fm_name = model_meta_field.filemaker_name
                if field_fm_name in _meta_fm_fields:
                    raise ValueError(
                        f"Field with FileMaker name '{field_fm_name}' already exists in portal '{cls.__name__}'")

                _meta_fm_fields[field_fm_name] = model_meta_field

                if isinstance(attr_value, fmd_fields.FMFieldMixin):
                    attr_value._field_name = field_fm_name

        base_schema_cls: Type[FileMakerSchema] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                                    attribute_name="base_schema") or FileMakerSchema

        schema_config = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="schema_config") or {}

        portal_name = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="portal_name")
        table_occurrence_name = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                   attribute_name="table_occurrence_name")

        cls._meta = PortalModelMeta(
            base_schema=base_schema_cls,
            schema_config=schema_config,
            portal_name=portal_name,
            table_occurrence_name=table_occurrence_name,
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
        self._ignore_prefetched = False
        self._result_cache: Optional[CacheIterator[PortalModel]] = None
        self._result_pages: Optional[CacheIterator[PortalPage]] = None

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
        qs._ignore_prefetched = self._ignore_prefetched

        return qs

    def _fetch_all(self):
        if self._result_cache is not None:
            return

        if not self._ignore_prefetched:
            prefetch_data: PortalPrefetchData = self._model._portals_prefetch.get(self._meta_portal.filemaker_name,
                                                                                  None)

            if prefetch_data is not None:
                self._result_cache = prefetch_data.cache[self._slice_start:self._slice_stop]
                return

        # In the worst scenario, execute the query
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

            new_qs = self._clone()
            new_qs._set_new_slice(k.start, k.stop)

            if self._result_cache is not None:
                new_qs._result_cache = CacheIterator(itertools.islice(self._result_cache.__iter__(), k.start, k.stop))

            # In case step is present, the list() force the execution of the query then use the list step to provide the result
            return list(new_qs)[::k.step] if k.step else new_qs

        elif isinstance(k, int):
            if k < 0:
                raise ValueError("Negative indexing is not supported.")

            if self._result_cache is not None:
                return self._result_cache[k]

            new_qs = self._clone()
            new_qs._set_new_slice(k, k + 1)
            new_qs._fetch_all()

            return new_qs._result_cache[0]

        else:
            raise TypeError(
                "QuerySet indices must be integers or slices, not %s."
                % type(k).__name__
            )

    def ignore_prefetched(self, avoid: bool = True):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._ignore_prefetched = avoid
        return new_qs

    def chunking(self, size):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._chunk_size = size
        return new_qs

    def create(self, **kwargs):
        portal = self._meta_portal.field.model(model=self._model, portal_name=self._meta_portal.filemaker_name,
                                               **kwargs)
        portal.save()

        # We cannot return it because it has no record_id yet so is useless and dangerous!
        # TODO in new versions of filemaker it could be possible to return it with record_id

    def delete(self):
        self._fetch_all()
        portal_records = [portal for portal in self._result_cache]

        if not portal_records:
            return

        self._model.save(force_update=True, update_fields=[], portals_to_delete=portal_records)

    def _execute_query(self):
        offset = self._slice_start + 1

        if self._slice_stop is not None:
            limit = self._slice_stop - self._slice_start
        else:
            limit = A_REALLY_BIG_LIMIT

        chunk_size = self._chunk_size

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

        self._result_pages = CacheIterator(paged_result)
        self._result_cache = CacheIterator(self.portals_record_from_portal_page_iterator(
            model=self._model,
            portal_fm_name=self._meta_portal.filemaker_name,
            page_iterator=self._result_pages.__iter__()
        ))

    def portals_record_from_portal_page_iterator(self,
                                                 model: Model,
                                                 portal_fm_name: str,
                                                 page_iterator: PortalPageIterator, ) -> Iterator[PortalModel]:
        portal_field = self._model._meta.fm_portal_fields[portal_fm_name]
        portal_model: Type[PortalModel] = portal_field.field.model

        already_seen_record_ids = set()

        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            response_data = page.result.response.data
            record_data = response_data[0]
            portal_data_list = record_data.portal_data.get(portal_fm_name)

            yield from portal_model_iterator_from_portal_data(
                model=model,
                portal_data_list=portal_data_list,
                portal_name=portal_field.filemaker_name,
                portal_model_class=portal_model,
                already_seen_record_ids=already_seen_record_ids
            )


AMODEL = TypeVar('AMODEL', bound="Model")


class PortalModel(metaclass=PortalMetaclass):
    # Example of Meta
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
        self._table_occurrence_name: str = self._meta.table_occurrence_name or kwargs.pop("table_occurrence_name")

        if self.model is None:
            raise ValueError("Model (model) is required to create a portal model.")

        if self._portal_name is None:
            raise ValueError("Portal name (portal_name) is required to create a portal model.")

        if self._table_occurrence_name is None:
            raise ValueError("Table name (table_name) is required to create a portal model.")

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
            patch = patch_from_model_or_portal(model_or_portal=self,
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

            patch = patch_from_model_or_portal(model_or_portal=self,
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

        self.model.save(force_update=True, update_fields=[], portals_to_delete=[self])
        self.record_id = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def as_layout_model(self, model_class: Type[AMODEL]) -> AMODEL:
        if self.record_id is None:
            raise ValueError("Cannot update a record without record_id.")

        model_field_data = {}
        portal_model_updated_fields_fm_name = []

        table_name_prefix = self._table_occurrence_name + "::"
        field_data = self._dump_fields()
        for key, value in field_data.items():
            if key.startswith(table_name_prefix):
                converted_fm_name = key[len(table_name_prefix):]
            else:
                converted_fm_name = key

            model_field_data[converted_fm_name] = value
            portal_model_updated_fields_fm_name.append(converted_fm_name)

        model: Model = model_class(
            record_id=self.record_id,
            mod_id=self.mod_id,
            _from_db=model_field_data,
        )

        # ---- Copy updated fields ----
        updated_fields = set()
        # For each one, if exists in the layout model, add to the list of updated fields
        for fm_field_name in portal_model_updated_fields_fm_name:
            field_meta_in_layout_model = model._meta.fm_fields.get(fm_field_name, None)

            if field_meta_in_layout_model is not None:
                updated_fields.add(field_meta_in_layout_model.name)

        model._updated_fields = updated_fields
        return model


def escape_filemaker_special_characters(s: Union[str, int]) -> Union[str, int]:
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

    if not isinstance(s, str):
        return s

    # List of characters that FileMaker treats specially.
    special_chars = '@*#?!=<>"'
    # Create a mapping from each character's ordinal to its escaped version.
    mapping = {ord(c): f"\\{c}" for c in special_chars}
    # Translate the input string using the mapping.
    return s.translate(mapping)


def get_fm_value(field_meta: ModelMetaField, value) -> Union[str, int]:
    return escape_filemaker_special_characters(field_meta.field._serialize(value, None, None))


class FieldCriteria:
    def convert(self, field_meta: ModelMetaField, model_class: Type[Model]) -> Union[str, int]:
        raise NotImplementedError()


class Criteria:
    @dataclasses.dataclass
    class Raw(FieldCriteria):
        value: str

        def convert(self, field_meta: ModelMetaField, model_class: Type[Model]) -> Union[str, int]:
            return self.value

    Empty = Raw("==")
    Blank = Raw("=")
    NotEmpty = Raw("*")

    @dataclasses.dataclass
    class SingleParameterCriteria(FieldCriteria):
        value: Any

        def get_fm_value(self, field_meta: ModelMetaField, model_class: Type[Model]) -> Union[str, int]:
            return get_fm_value(field_meta=field_meta, value=self.value)

    @dataclasses.dataclass
    class Exact(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"=={self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class StartsWith(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"=={self.get_fm_value(**kwargs)}*"

    @dataclasses.dataclass
    class EndsWith(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"==*{self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class Contains(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"==*{self.get_fm_value(**kwargs)}*"

    @dataclasses.dataclass
    class Gt(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f">{self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class Gte(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f">={self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class Lt(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"<{self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class Lte(SingleParameterCriteria):
        def convert(self, **kwargs) -> str:
            return f"<={self.get_fm_value(**kwargs)}"

    @dataclasses.dataclass
    class Range(FieldCriteria):
        range_from: Union[int, str]
        range_to: Union[int, str]

        def convert(self, field_meta: ModelMetaField, **kwargs) -> str:
            return f"{get_fm_value(field_meta=field_meta, value=self.range_from)}...{get_fm_value(field_meta=field_meta, value=self.range_to)}"


def add_portal_record_to_portal_data(portal_data: dict,
                                     portal_name: str,
                                     portal_record_id: str,
                                     portal_mod_id: Optional[str],
                                     portal_field_data: dict):
    result_data = {
        **portal_field_data
    }

    if portal_record_id is not None:
        result_data["recordId"] = portal_record_id
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
        self._scripts_responses_cache: Optional[CacheIterator[ScriptsResponse]] = None
        self._result_pages: Optional[CacheIterator[Page]] = None

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
        qs._portals = self._portals.copy()
        qs._slice_start = self._slice_start
        qs._slice_stop = self._slice_stop
        qs._response_layout = self._response_layout

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

    def scripts_responses(self) -> Iterator[ScriptsResponse]:
        self._fetch_all()
        return iter(self._scripts_responses_cache)

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
                    field_criteria = Criteria.Exact(value)
                else:
                    field_name, query_type = key.split('__', 1)

                    if query_type == 'raw':
                        field_criteria = Criteria.Raw(value)
                    elif query_type == 'exact':
                        field_criteria = Criteria.Exact(value)
                    elif query_type == 'startswith':
                        field_criteria = Criteria.StartsWith(value)
                    elif query_type == 'endswith':
                        field_criteria = Criteria.EndsWith(value)
                    elif query_type == 'contains':
                        field_criteria = Criteria.Contains(value)
                    elif query_type == 'gt':
                        field_criteria = Criteria.Gt(value)
                    elif query_type == 'gte':
                        field_criteria = Criteria.Gte(value)
                    elif query_type == 'lt':
                        field_criteria = Criteria.Lt(value)
                    elif query_type == 'lte':
                        field_criteria = Criteria.Lte(value)
                    elif query_type == 'range':
                        if not isinstance(value, (list, tuple)) or len(value) != 2:
                            raise ValueError(
                                f"Value for query type 'range' must be a list or tuple with 2 elements, got {type(value)}, {value}")
                        field_criteria = Criteria.Range(range_from=value[0], range_to=value[1])
                    else:
                        raise ValueError(f"Unknown query type '{query_type}' on field '{key}'")

            field = self._retrive_meta_field_form_field_name(field_name)
            criteria[field.filemaker_name] = field_criteria.convert(
                field_meta=field,
                model_class=self._model_class,
            )

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

    def chunking(self, size):
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
        portal_field = self._model_class._meta.portal_fields.get(portal, None)
        if portal_field is None:
            raise AttributeError(f"Portal '{portal}' does not exist in model '{self._model_class.__name__}'")

        portal_fm_name = portal_field.filemaker_name

        new_qs._portals[portal_fm_name] = SinglePortalInput(offset=offset, limit=limit)
        return new_qs

    def response_layout(self, response_layout):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._response_layout = response_layout
        return new_qs

    def prerequest_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts["prerequest"] = ScriptInput(name=name, param=param)
        return new_qs

    def presort_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts["presort"] = ScriptInput(name=name, param=param)
        return new_qs

    def after_script(self, name, param=None):
        self._assert_not_sliced()

        new_qs = self._clone()
        new_qs._scripts["after"] = ScriptInput(name=name, param=param)
        return new_qs

    def __getitem__(self, k):
        if isinstance(k, slice):
            if (k.start is not None and k.start < 0) or (k.stop is not None and k.stop < 0):
                raise ValueError("Negative indexing is not supported.")
            if k.stop is not None and k.stop <= (k.start or 0):
                raise ValueError("Stop index must be greater than start index.")

            new_qs = self._clone()
            new_qs._set_new_slice(k.start, k.stop)

            if self._result_cache is not None:
                new_qs._result_cache = CacheIterator(itertools.islice(self._result_cache.__iter__(), k.start, k.stop))

            # In case step is present, the list() force the execution of the query then use the list step to provide the result
            return list(new_qs)[::k.step] if k.step else new_qs

        elif isinstance(k, int):
            if k < 0:
                raise ValueError("Negative indexing is not supported.")

            if self._result_cache is not None:
                return self._result_cache[k]

            new_qs = self._clone()
            new_qs._set_new_slice(k, k + 1)
            new_qs._fetch_all()

            return new_qs._result_cache[0]

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
        else:
            limit = A_REALLY_BIG_LIMIT

        chunk_size = self._chunk_size

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

        self._result_pages = paged_result.pages
        self._result_cache = CacheIterator(
            self.records_iterator_from_page_iterator(page_iterator=paged_result.pages.__iter__(),
                                                     portals_input=self._portals))

        self._scripts_responses_cache = CacheIterator(
            self.script_results_from_page_iterator(page_iterator=paged_result.pages.__iter__())
        )

    def script_results_from_page_iterator(self, page_iterator: PageIterator):
        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            after_script_result = page.result.response.after_script_result
            after_script_error = page.result.response.after_script_error

            presort_script_result = page.result.response.presort_script_result
            presort_script_error = page.result.response.presort_script_error

            prerequest_script_result = page.result.response.prerequest_script_result
            prerequest_script_error = page.result.response.prerequest_script_error

            yield ScriptsResponse(
                after=None if after_script_error is None else ScriptResponse(
                    result=after_script_result,
                    error=after_script_error,
                ),
                presort=None if presort_script_error is None else ScriptResponse(
                    result=presort_script_result,
                    error=presort_script_error,
                ),
                prerequest=None if prerequest_script_error is None else ScriptResponse(
                    result=prerequest_script_result,
                    error=prerequest_script_error,
                ),
            )

    def records_iterator_from_page_iterator(self,
                                            page_iterator: PageIterator,
                                            portals_input: PortalsInput) -> Iterator[Model]:

        already_seen_record_ids = set()

        for page in page_iterator:
            page.result.raise_exception_if_has_error()

            if page.result.response.data is None:
                continue

            for data_entry in page.result.response.data:
                record_id = data_entry.record_id

                # De duplication
                if record_id in already_seen_record_ids:
                    continue

                already_seen_record_ids.add(record_id)

                model = self._model_class(
                    record_id=record_id,
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
        iterator = portal_model_iterator_from_portal_data(
            model=model,
            portal_name=portal_field.filemaker_name,
            portal_data_list=portal_data_list,
            portal_model_class=portal_model_class
        )

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

    def _execute_edit_record(self, record_id, mod_id, field_data, portals_data, portals_to_delete):

        len_delete_related = len(portals_to_delete)
        len_portal_data = len(portals_data)
        len_field_data = len(field_data)

        # If no change are required on model, and no change are required on portals
        if len_field_data + len_portal_data + len_delete_related == 0:
            return None

        result = None
        # In FM 18 and later, we can delete multiple portal records in a single request
        if fm_version_gte(self._client, FMVersion.V18):
            delete_related_portal_records = self.get_delete_related_field_data(portals_to_delete=portals_to_delete)

            if delete_related_portal_records:
                field_data.update(delete_related_portal_records)

            result = self._client.edit_record(
                layout=self._layout,
                record_id=record_id,
                mod_id=mod_id,
                field_data=field_data,
                portal_data=portals_data)
        else:
            # We first do the save of the changes on the model + new portals

            if len_field_data + len_portal_data != 0:
                result = self._client.edit_record(
                    layout=self._layout,
                    record_id=record_id,
                    mod_id=mod_id,
                    field_data=field_data,
                    portal_data=portals_data)

                result.raise_exception_if_has_error()

            for portal_info in portals_to_delete:
                field_data = self.get_delete_related_field_data(portals_to_delete=[portal_info])

                result = self._client.edit_record(
                    layout=self._layout,
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

        portal_data: PortalsInput = add_portal_record_to_portal_data(
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

    def get_delete_related_field_data(self, portals_to_delete: Iterable[Tuple[str, str]]) -> Dict[str, Any]:

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

    def _execute_upload_container(self, record_id, field_name, field_repetition, file):
        result = self._client.upload_container(
            layout=self._layout,
            record_id=record_id,
            field_name=field_name,
            field_repetition=field_repetition,
            file=file
        )

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

                field_fm_name = model_meta_field.filemaker_name
                if field_fm_name in _meta_fm_fields:
                    raise ValueError(
                        f"Field with FileMaker name '{field_fm_name}' already exists in model '{cls.__name__}'")

                _meta_fm_fields[field_fm_name] = model_meta_field

                if isinstance(attr_value, fmd_fields.FMFieldMixin):
                    attr_value._field_name = field_fm_name

            if isinstance(attr_value, PortalField):
                schema_portal_fields[attr_name] = attr_value
                model_portal_meta_field = ModelMetaPortalField(name=attr_name, field=attr_value)
                _meta_portal_fields[attr_name] = model_portal_meta_field

                portal_fm_name = model_portal_meta_field.filemaker_name
                if portal_fm_name in _meta_fm_portal_fields:
                    raise ValueError(
                        f"Portal field with FileMaker name '{portal_fm_name}' already exists in model '{cls.__name__}'")
                _meta_fm_portal_fields[portal_fm_name] = model_portal_meta_field

        base_schema_cls: Type[FileMakerSchema] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                                    attribute_name="base_schema") or FileMakerSchema

        schema_config = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="schema_config") or {}

        client: FMClient = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="client")
        layout: str = get_meta_attribute(cls=cls, attrs_meta=attrs_meta, attribute_name="layout")

        base_manager: Type[ModelManager] = get_meta_attribute(cls=cls, attrs_meta=attrs_meta,
                                                              attribute_name="base_manager") or ModelManager

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

 # TODO Model to LayoutModel ?
class Model(metaclass=ModelMetaclass):
    # Example of Meta:
    #
    # class Meta:
    #     client: FMClient = None
    #     layout: str = None
    #     base_schema: FileMakerSchema = None
    #     schema_config: dict = None

    objects: ModelManager

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

            patch = patch_from_model_or_portal(model_or_portal=portal,
                                               only_updated_fields=config.only_updated_fields,
                                               update_fields=config.update_fields)

            add_portal_record_to_portal_data(portal_data=portals_input,
                                             portal_name=portal._portal_name,
                                             portal_record_id=portal.record_id,
                                             portal_mod_id=used_mod_id,
                                             portal_field_data=patch)

        # Execute
        if (not record_id_exists and not force_update) or (record_id_exists and force_insert):
            patch = patch_from_model_or_portal(model_or_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=None)

            result = self.objects._execute_create_record(field_data=patch, portals_data=portals_input)

            self.record_id = result.response.record_id
            self.mod_id = result.response.mod_id
        elif not record_id_exists and force_update:
            raise ValueError("Cannot update a record without record_id.")
        elif record_id_exists and not force_insert:
            patch = patch_from_model_or_portal(model_or_portal=self,
                                               only_updated_fields=only_updated_fields,
                                               update_fields=update_fields, )

            used_mod_id = self.mod_id if check_mod_id else None

            # Portal delete
            portals_to_delete_record_ids = [(portal._portal_name, portal.record_id) for portal in portals_to_delete]

            result = self.objects._execute_edit_record(record_id=self.record_id,
                                                       mod_id=used_mod_id,
                                                       field_data=patch,
                                                       portals_data=portals_input,
                                                       portals_to_delete=portals_to_delete_record_ids)

            if result is not None:
                self.mod_id = result.response.mod_id
        else:
            raise ValueError("Impossible case")

        return self

    # TODO add support for duplicate()

    def delete(self):
        if self.record_id is None:
            return

        self.objects._execute_delete_record(self.record_id)
        self.record_id = None

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def update_container(self, field_name: str, file: IO):
        if self.record_id is None:
            raise ValueError("Cannot update a record without record_id.")

        field_meta = self._meta.fields.get(field_name, None)

        if field_meta is None:
            raise ValueError(f"Field '{field_name}' does not exist.")

        field = field_meta.field

        if not isinstance(field, fmd_fields.Container):
            raise ValueError(f"Field '{field_name}' is not a fmd_fields.Container.")

        self.objects._execute_upload_container(
            record_id=self.record_id,
            field_name=field_meta.filemaker_name,
            field_repetition=field._repetition_number,
            file=file
        )


def patch_from_model_or_portal(model_or_portal: Union[PortalModel, Model], only_updated_fields, update_fields):
    patch = model_or_portal._dump_fields()
    if update_fields is not None:
        patch = {key: value for key, value in patch.items()
                 if key in update_fields}
    if only_updated_fields:
        patch = {key: value for key, value in patch.items()
                 if model_or_portal._meta.fm_fields[key].name in model_or_portal._updated_fields}
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


def portal_model_iterator_from_portal_data(
        model: Model,
        portal_data_list,
        portal_model_class: Type[PortalModel],
        portal_name=None,
        already_seen_record_ids: Set[str] = None
) -> Iterator[PortalModel]:
    for single_portal_data_value in portal_data_list:
        record_id = single_portal_data_value.record_id

        if already_seen_record_ids is not None:
            if record_id in already_seen_record_ids:
                continue

            already_seen_record_ids.add(record_id)

        yield portal_model_class(
            model=model,
            portal_name=portal_name,
            record_id=record_id,
            mod_id=single_portal_data_value.mod_id,
            _from_db=single_portal_data_value.fields
        )
