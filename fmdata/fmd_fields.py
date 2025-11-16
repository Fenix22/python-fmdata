from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal as PythonDecimal
from enum import Enum
from typing import Any, Iterable, Optional, Set

from marshmallow import fields, ValidationError


class FMFieldType(str, Enum):
    Text = "Text"
    Number = "Number"
    Date = "Date"
    Timestamp = "Timestamp"
    Time = "Time"
    Container = "Container"


# FileMaker US formats
FM_DATE_FORMAT = "%m/%d/%Y"
FM_DATE_TIME_FORMAT = "%m/%d/%Y %H:%M:%S"
FM_TIME_FORMAT = "%H:%M:%S"


# ---- Helpers for formatting/parsing ----

def usformat_date(value: date) -> str:
    return f"{value.month:02d}/{value.day:02d}/{value.year:04d}"


def from_usformat_date(value: str) -> date:
    try:
        return datetime.strptime(value, FM_DATE_FORMAT).date()
    except Exception as e:
        raise ValidationError(f"Invalid FileMaker date format: {value}") from e


def usformat_datetime(value: datetime) -> str:
    return f"{value.month:02d}/{value.day:02d}/{value.year:04d} {value.hour:02d}:{value.minute:02d}:{value.second:02d}"


def from_usformat_datetime(value: str) -> datetime:
    try:
        return datetime.strptime(value, FM_DATE_TIME_FORMAT)
    except Exception as e:
        raise ValidationError(f"Invalid FileMaker datetime format: {value}") from e


def usformat_time(value: time) -> str:
    return f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"


def from_usformat_time(value: str) -> time:
    try:
        return datetime.strptime(value, FM_TIME_FORMAT).time()
    except Exception as e:
        raise ValidationError(f"Invalid FileMaker time format: {value}") from e


def isodate(value: date) -> str:
    return value.isoformat()


def from_isodate(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as e:
        raise ValidationError(f"Invalid ISO date: {value}") from e


def isodatetime(value: datetime) -> str:
    return value.isoformat()


def from_isodatetime(value: str) -> datetime:
    try:
        if len(value) < 19:
            raise ValidationError(f"Invalid ISO datetime: {value} (it's a date instead?)")
        return datetime.fromisoformat(value)
    except Exception as e:
        raise ValidationError(f"Invalid ISO datetime: {value}") from e


def isotime(value: time) -> str:
    return value.isoformat()


def from_isotime(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except Exception as e:
        raise ValidationError(f"Invalid ISO time: {value}") from e


# ---- Base mixin implementing fm_type routing ----

@dataclass
class _FMFieldConfig:
    fm_type: FMFieldType


class FMFieldMixin:
    def __init__(self, *args, field_type: FMFieldType = None, field_name: str = None, read_only=False, **kwargs):
        if field_type is None:
            raise ValueError(
                "fm_type must be provided (FMType.Text, FMType.Number, FMType.Date, FMType.DateTime, FMType.Time or FMType.Container). "
                "What is the FileMaker field type for this field? If it's a 'Calculated' field, specify the type it returns."
            )

        if "data_key" in kwargs:
            raise ValueError("data_key is not supported for FM fields. Use fm_name instead.")
        else:
            # When none marshmallow will use the field name as the key in the serialized data
            kwargs["data_key"] = field_name

        if field_type == FMFieldType.Container:
            read_only = True

        self._read_only = read_only
        if read_only:
            kwargs["load_only"] = True

        self._field_type = field_type

        # self._fm_name will be populated with the field_name during the Model initialization
        self._field_name = field_name

        # MRO: will immediately call the marshmallow field __init__
        super().__init__(*args, **kwargs)

    def _validate_fm_types(self, allowed: Iterable[FMFieldType]):
        if self._field_type not in allowed:
            allowed_list = ", ".join(t.value for t in allowed)

            raise ValidationError(
                f"Unsupported fm_type {self._field_type} for {self.__class__.__name__}. Allowed: {allowed_list}"
            )

    @property
    def field_type(self) -> FMFieldType:
        return self._field_type

    # ---- Exception helpers ----

    def _serialization_error(self, value: Any, expected: str) -> ValueError:
        return ValueError(
            f"Error serializing FM field '{self._field_name}'. "
            f"Expected {expected} to be serializable to FM.{self._field_type}. "
            f"Got {type(value).__name__}: {value!r}")

    def _deserialization_error(self, value: Any, expected: str) -> ValidationError:
        return ValidationError(
            f"Error deserializing FM field '{self._field_name}'. "
            f"Expected FM.{self._field_type} to be deserializable to {expected}. "
            f"Got {type(value).__name__}: {value!r}")


# ---- String ----

class String(FMFieldMixin, fields.String):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types(
            {FMFieldType.Text, FMFieldType.Number, FMFieldType.Date, FMFieldType.Timestamp, FMFieldType.Time,
             FMFieldType.Container})

    def _serialize(self, value: Optional[str], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, str):
            raise self._serialization_error(value=value, expected="str")

        try:
            if self._field_type == FMFieldType.Text:
                return value
            elif self._field_type == FMFieldType.Number:
                # You can safely pass a string to the DataAPI and FM will convert it to a number in case is a number.
                # In case it's not a number, FM will accept it anycase and store it as a string!! (so you will read a string back)
                # Ex. If you write "25abc" to a Number field, FM will store it as-is, and when you read it back you will get "25abc".
                return value
            elif self._field_type == FMFieldType.Date:
                return usformat_date(from_isodate(value))
            elif self._field_type == FMFieldType.Timestamp:
                return usformat_datetime(from_isodatetime(value))
            elif self._field_type == FMFieldType.Time:
                return usformat_time(from_isotime(value))
            else:
                raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="str") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[str]:
        if value is None:
            return None

        if self._field_type == FMFieldType.Number:
            if not isinstance(value, (str, int)):
                raise self._deserialization_error(value=value, expected="str")

            return str(value)

        # For all the others we expect a str
        if not isinstance(value, str):
            raise self._deserialization_error(value=value, expected="str")

        try:
            if self._field_type == FMFieldType.Text:
                return value
            elif self._field_type == FMFieldType.Container:
                return value

            if value == "":
                return None

            if self._field_type == FMFieldType.Date:
                return isodate(from_usformat_date(value))
            elif self._field_type == FMFieldType.Timestamp:
                return isodatetime(from_usformat_datetime(value))
            elif self._field_type == FMFieldType.Time:
                return isotime(from_isotime(value))

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._deserialization_error(value=value, expected="str") from e


# ---- Integer ----

class Integer(FMFieldMixin, fields.Integer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Number, FMFieldType.Text})

    def _serialize(self, value: Optional[int], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, int):
            raise self._serialization_error(value=value, expected="int")

        try:
            if self._field_type == FMFieldType.Number:
                return value
            elif self._field_type == FMFieldType.Text:
                return str(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="int") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[int]:
        if value == "" or value is None:
            return None

        try:
            return super()._deserialize(value, attr, data, **kwargs)
        except Exception as e:
            raise self._deserialization_error(value=value, expected="int") from e


# ---- Float ----

class Float(FMFieldMixin, fields.Float):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Number, FMFieldType.Text})

    def _serialize(self, value: Optional[float], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, (int, float)):
            raise self._serialization_error(value=value, expected="(int, float)")

        try:
            if self._field_type == FMFieldType.Number:
                return value
            elif self._field_type == FMFieldType.Text:
                return str(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="float") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[float]:
        if value == "" or value is None:
            return None

        if not isinstance(value, (str, int)):
            raise self._deserialization_error(value=value, expected="float")

        try:
            return super()._deserialize(value, attr, data, **kwargs)
        except Exception as e:
            raise self._deserialization_error(value=value, expected="float") from e


# ---- Decimal ----

class Decimal(FMFieldMixin, fields.Decimal):
    def __init__(self, *args, **kwargs):
        # With as_string=False, the value returned by marshmallow will be a float (so can lose precision).
        # With as_string=True, the value returned by marshmallow will be a string
        kwargs.setdefault("as_string", True)
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Number, FMFieldType.Text})

    def _serialize(self, value: Optional[PythonDecimal], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, PythonDecimal):
            raise self._serialization_error(value=value, expected="Decimal")

        try:
            if self._field_type == FMFieldType.Number:
                return str(value)
            elif self._field_type == FMFieldType.Text:
                return str(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="Decimal") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[Decimal]:
        if value == "" or value is None:
            return None

        if not isinstance(value, (str, int)):
            raise self._deserialization_error(value=value, expected="Decimal")

        try:
            return super()._deserialize(value, attr, data, **kwargs)
        except Exception as e:
            raise self._deserialization_error(value=value, expected="Decimal") from e


# ---- Bool ----

default_bool_truthy = fields.Boolean.truthy
default_bool_falsy = fields.Boolean.falsy

class Bool(FMFieldMixin, fields.Boolean):
    def __init__(
            self,
            *args,
            truthy: Optional[Iterable[Any]] = None,
            falsy: Optional[Iterable[Any]] = None,
            true_value: Any = "1",
            false_value: Any = "0",
            **kwargs,
    ):
        if truthy is None:
            self._truthy = default_bool_truthy
        else:
            self._truthy = set(truthy)

        if falsy is None:
            self._falsy = default_bool_falsy
        else:
            self._falsy = set(falsy)

        self._true_value = str(true_value)
        self._false_value = str(false_value)

        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Number, FMFieldType.Text})

    def _serialize(self, value: Optional[bool], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, bool):
            raise self._serialization_error(value=value, expected="bool")

        return self._true_value if value == True else self._false_value

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[bool]:
        if value == "" or value is None:
            return None

        if self._field_type == FMFieldType.Text:
            if not isinstance(value, str):
                raise self._deserialization_error(value=value, expected="bool")

        if self._field_type == FMFieldType.Number:
            if not isinstance(value, (str, int)):
                raise self._deserialization_error(value=value, expected="bool")

        if value in self._truthy:
            return True
        if value in self._falsy:
            return False

        raise self._deserialization_error(value=value, expected="bool (boolish)")


# ---- Date ----

class Date(FMFieldMixin, fields.Date):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Date, FMFieldType.Text})

    def _serialize(self, value: Optional[date], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, date):
            raise self._serialization_error(value=value, expected="date")

        try:
            if self._field_type == FMFieldType.Date:
                return usformat_date(value)
            elif self._field_type == FMFieldType.Text:
                return isodate(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="date") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[date]:
        if value == "" or value is None:
            return None

        try:
            if self._field_type == FMFieldType.Date:
                return from_usformat_date(value)
            elif self._field_type == FMFieldType.Text:
                return from_isodate(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._deserialization_error(value=value, expected="date") from e


# ---- DateTime ----

class DateTime(FMFieldMixin, fields.DateTime):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Timestamp, FMFieldType.Text})

    def _serialize(self, value: Optional[datetime], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, datetime):
            raise ValueError(f"Expected datetime value for {self._field_name}, got {value!r}")

        try:
            if self._field_type == FMFieldType.Timestamp:
                return usformat_datetime(value)
            elif self._field_type == FMFieldType.Text:
                return isodatetime(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="datetime") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[datetime]:
        if value == "" or value is None:
            return None

        try:
            if self._field_type == FMFieldType.Timestamp:
                return from_usformat_datetime(value)
            elif self._field_type == FMFieldType.Text:
                return from_isodatetime(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._deserialization_error(value=value, expected="datetime") from e


# ---- Time ----

class Time(FMFieldMixin, fields.Time):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._validate_fm_types({FMFieldType.Time, FMFieldType.Text})

    def _serialize(self, value: Optional[time], attr, obj, **kwargs):
        if value is None:
            return ""

        if not isinstance(value, time):
            raise ValueError(f"Expected time value for {self._field_name}, got {value!r}")

        try:
            if self._field_type == FMFieldType.Time:
                return usformat_time(value)
            elif self._field_type == FMFieldType.Text:
                return isotime(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._serialization_error(value=value, expected="time") from e

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[time]:
        if value == "" or value is None:
            return None

        try:
            if self._field_type == FMFieldType.Time:
                return from_usformat_time(value)
            elif self._field_type == FMFieldType.Text:
                return from_isotime(value)

            raise Exception("Impossible scenario")
        except Exception as e:
            raise self._deserialization_error(value=value, expected="time") from e


# ---- Container ----

class Container(FMFieldMixin, fields.String):
    def __init__(self, *args, repetition_number=None, **kwargs):
        field_name: Optional[str] = kwargs.pop("field_name", None)

        if repetition_number is None:
            if field_name is not None:
                repetition_number = self._get_last_bracket_content(field_name)

                if repetition_number is not None:
                    repetition_number = int(repetition_number)

        if repetition_number is None:
            repetition_number = 1

        if not isinstance(repetition_number, int):
            raise ValueError(f"Invalid repetition number: {repetition_number}")

        self._field_name = field_name
        self._repetition_number = repetition_number

        kwargs.pop("field_type", None)  # Ignore any field_type passed, force Container
        kwargs.pop("read_only", None)  # Ignore any read_only passed, force True
        super().__init__(*args, read_only=True, field_type=FMFieldType.Container, field_name=field_name, **kwargs)

    def _get_last_bracket_content(self, value: str) -> Optional[str]:
        matches = re.findall(r'\[(.*?)\]', value)
        return matches[-1] if matches else None

    def _serialize(self, value: Optional[str], attr, obj, **kwargs):
        raise ValueError(
            "Container fields cannot be serialized directly. Use model.update_container(..) to update them.")

    def _deserialize(self, value: Any, attr, data, **kwargs) -> Optional[str]:
        if value is None:
            return None

        if not isinstance(value, str):
            raise self._deserialization_error(value=value, expected="str")

        return value


__all__ = [
    "FMFieldType",
    "String",
    "Integer",
    "Float",
    "Decimal",
    "Bool",
    "Date",
    "DateTime",
    "Time",
]
