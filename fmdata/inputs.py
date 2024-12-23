from __future__ import annotations

import json
from enum import IntEnum
from typing import List, Dict, Optional, Any, TypedDict, Union

from typing_extensions import NotRequired

from fmdata.utils import clean_none

VerifySSL = Union[bool, str]


class DateFormats(IntEnum):
    """
    Enum for specifying the format of date and timestamp fields.

    Attributes:
        US (int): US date format (default value).
        FILE_LOCALE (int): File locale's date format.
        ISO_8601 (int): ISO 8601 date format.
    """
    US = 0
    FILE_LOCALE = 1
    ISO_8601 = 2


DateInput = Union[int, DateFormats]

QueryInput = List[Dict[str, Any]]


class ScriptInput(TypedDict):
    name: str
    param: str


class ScriptsInput(TypedDict):
    prerequest: NotRequired[ScriptInput]
    presort: NotRequired[ScriptInput]
    after: NotRequired[ScriptInput]


class OptionsInput(TypedDict):
    entrymode: NotRequired[str]
    prohibitmode: NotRequired[str]


class SinglePortalInput(TypedDict):
    offset: NotRequired[int]
    limit: NotRequired[int]


PortalsInput = Dict[str, SinglePortalInput]


class SingleSortInput(TypedDict):
    fieldName: NotRequired[str]
    sortOrder: NotRequired[str]


SortInput = List[SingleSortInput]


class DataSourceInput(TypedDict):
    database: str
    username: str
    password: str


def _scripts_to_dict(scripts: ScriptsInput) -> Dict[str, str]:
    result = {}

    if scripts is None:
        return result

    prerequest = scripts.get('prerequest', None)
    if prerequest:
        result['script.prerequest'] = prerequest['name']
        result['script.prerequest.param'] = prerequest['param']

    presort = scripts.get('presort', None)
    if presort:
        result['script.presort'] = presort['name']
        result['script.presort.param'] = presort['param']

    after = scripts.get('after', None)
    if after:
        result['script'] = after['name']
        result['script.param'] = after['param']

    return result


def _sort_to_params(sort: List[SingleSortInput]) -> Optional[str]:
    return json.dumps(sort) if sort else None


def _portals_to_params(portals: PortalsInput, names_as_string: bool = False) -> Dict[str, Any]:
    if portals is None:
        return {}

    params: Dict[str, Any]

    portal_selector = [portal_name for portal_name in portals.keys()]

    if names_as_string:
        portal_param = "[" + ', '.join(map(lambda x: '"' + x + '"', portal_selector)) + "]"
        params = {'portal': portal_param}
        param_prefix = '_'  # for GET we need an underscore as prefix
    else:
        params = {'portal': portal_selector}
        param_prefix = ''

    for name, portal in portals.items():
        offset = portal.get('offset', None)
        if offset is not None:
            params[param_prefix + 'offset.' + name] = offset

        limit = portal.get('limit', None)
        if limit is not None:
            params[param_prefix + 'limit.' + name] = limit

    return clean_none(params)


def _date_formats_to_value(date_input: Optional[DateInput]) -> Optional[int]:
    if date_input is None:
        return None

    if isinstance(date_input, int):
        return date_input

    return date_input.value
