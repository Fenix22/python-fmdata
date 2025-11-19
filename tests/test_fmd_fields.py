from __future__ import annotations

import decimal
import unittest
from datetime import date, datetime, time
from decimal import Decimal as PythonDecimal
from zoneinfo import ZoneInfo

from marshmallow import ValidationError

from fmdata import fmd_fields, FMFieldType

# TODO update with all the changes README (fields, avoid_prefetch, names)
# --------------------------------------------------------------------------------------
# Unit-like tests for fmd_fields serialization/deserialization (no server required)
# --------------------------------------------------------------------------------------
class FMFieldsSerializationTests(unittest.TestCase):
    # ---- String ----
    def test_string_with_text_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Text)
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("", fld._serialize("", "x", {}))
        self.assertEqual("d21dwa", fld._serialize("d21dwa", "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(123, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(23.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(decimal.Decimal("23"), "x", {})

        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual("", fld._deserialize("", "x", {}))
        self.assertEqual("hello", fld._deserialize("hello", "x", {}))
        self.assertEqual("hello", fld._serialize("hello", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(123, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(23.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(decimal.Decimal("23"), "x", {})

    def test_string_with_number_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Number)
        # serialize requires str and returns as-is
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("25", fld._serialize("25", "x", {}))
        self.assertEqual("25.3", fld._serialize("25.3", "x", {}))
        self.assertEqual("25abc", fld._serialize("25abc", "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(123, "x", {})

        # deserialize accepts non-str too for Text/Number
        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual("", fld._deserialize("", "x", {}))
        self.assertEqual("123", fld._deserialize(123, "x", {}))
        self.assertEqual("123.54", fld._deserialize("123.54", "x", {}))
        self.assertEqual("123.4521312321321321213213321231321213321231231321123321", fld._deserialize("123.4521312321321321213213321231321213321231231321123321", "x", {}))

        #Ensure float number are not supported form filemaker (we use string for decimal fields instead)
        with self.assertRaises(ValidationError):
            fld._deserialize(123.231, "x", {})

    def test_string_with_date_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Date)

        d_iso = "2024-05-18"

        # Serialize to FM US format
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("05/18/2024", fld._serialize(d_iso, "x", {}))
        # Invalid input raises
        with self.assertRaises(ValueError):
            fld._serialize("", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("invalid-date", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("2024-99-18", "x", {})


        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual(None, fld._deserialize("", "x", {}))
        # Deserialize back to ISO
        self.assertEqual(d_iso, fld._deserialize("05/18/2024", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(123, "x", {})

    def test_string_with_timestamp_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Timestamp)

        dt_iso = "2024-05-18T06:30:05"  # Time zone info will be lost on serialize/deserialize
        dt_iso_tz = "2024-05-18T06:30:05+03:00"  # Time zone info will be lost on serialize/deserialize

        # Serialize to FM US format
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("05/18/2024 06:30:05", fld._serialize(dt_iso, "x", {}))
        self.assertEqual("05/18/2024 06:30:05", fld._serialize(dt_iso_tz, "x", {}))

        # Invalid input raises
        with self.assertRaises(ValueError):
            fld._serialize("", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("invalid-date-time", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("2024-99-18T06:30:05+03:00", "x", {})

        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual(None, fld._deserialize("", "x", {}))
        # Deserialize back to ISO
        self.assertEqual("2024-05-18T06:30:05", fld._deserialize("05/18/2024 06:30:05", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(123, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T06:30:05", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T06:30:05+03:00", "x", {})

    def test_string_with_time_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Time)

        t_iso = "06:30:05"

        self.assertEqual("", fld._serialize(None, "x", {}))
        # Serialize to FM format
        self.assertEqual("06:30:05", fld._serialize(t_iso, "x", {}))
        # Invalid input raises
        with self.assertRaises(ValueError):
            fld._serialize("invalid-time", "x", {})

        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual(None, fld._deserialize("", "x", {}))
        # Deserialize back to ISO
        self.assertEqual(t_iso, fld._deserialize("06:30:05", "x", {}))


        with self.assertRaises(ValidationError):
            fld._deserialize(123, "x", {})

    def test_string_with_container_fieldtype(self):
        fld = fmd_fields.String(field_type=FMFieldType.Container)
        # Container fieldtype behaves like Text for String fields
        self.assertEqual(None, fld._deserialize(None, "x", {}))
        self.assertEqual("", fld._deserialize("", "x", {}))
        self.assertEqual("filedata", fld._deserialize("filedata", "x", {}))

        self.assertEqual(True, fld._read_only)
        self.assertEqual(True, fld.load_only)

    # ---- Integer ----
    def test_integer_with_Number_fieldtype(self):
        fld = fmd_fields.Integer(field_type=FMFieldType.Number)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual(42, fld._serialize(42, "x", {}))

        self.assertEqual(42, fld._deserialize(42, "x", {}))
        self.assertEqual(42, fld._deserialize("42", "x", {}))

        # None/empty handling
        self.assertIsNone(fld._deserialize("", "x", {}))
        self.assertEqual("", fld._serialize(None, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize("42", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("42.3", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("42.3e4", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("ciao", "x", {})

    def test_integer_with_text_fieldtype(self):
        fld = fmd_fields.Integer(field_type=FMFieldType.Text)
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("7", fld._serialize(7, "x", {}))
        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(7, fld._deserialize("7", "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize("not-an-int", "x", {})

        with self.assertRaises(ValidationError):
            fld._deserialize("not-an-int", "x", {})

    # ---- Float ----
    def test_float_with_number_fieldtype(self):
        fld = fmd_fields.Float(field_type=FMFieldType.Number)
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual(42, fld._serialize(42, "x", {}))
        self.assertEqual(3.14, fld._serialize(3.14, "x", {}))

        too_precise_num = "3.14213221323213212313213123211421322132321321231321312321142132213232132123132131232114213221323213212313213123211421322132321321231321312321"
        float_tpn = float(too_precise_num)
        print(float_tpn)
        self.assertEqual(3.142132213232132, fld._serialize(float_tpn, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize("42", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(42, fld._deserialize(42, "x", {}))
        self.assertEqual(42, fld._deserialize("42", "x", {}))
        self.assertEqual(3.14, fld._deserialize("3.14", "x", {}))
        # Check loose of precision in deserialization
        self.assertEqual(3.142132213232132, fld._deserialize(too_precise_num, "x", {}))
        self.assertEqual(42366.556, fld._deserialize("42.366556e3", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize("string", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(3.14, "x", {})

    def test_float_with_text_fieldtype(self):
        fld = fmd_fields.Float(field_type=FMFieldType.Text)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("2.5", fld._serialize(2.5, "x", {}))
        self.assertEqual("2530.0", fld._serialize(2.53e3, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize("", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("21", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("21.21", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("whatever", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(2.0, fld._deserialize("2", "x", {}))
        self.assertEqual(2.0, fld._deserialize("2.0", "x", {}))
        self.assertEqual(2.5, fld._deserialize("2.5", "x", {}))
        self.assertEqual(252.1, fld._deserialize("2.521e2", "x", {}))
        with self.assertRaises(ValidationError):
            fld._deserialize("NaN?", "x", {})

    # ---- Decimal ----
    def test_decimal_with_number_fieldtype(self):
        fld = fmd_fields.Decimal(field_type=FMFieldType.Number)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("12.34", fld._serialize(PythonDecimal("12.34"), "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(12, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(12.34, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("12.34", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        # Accept strings and return Decimal
        self.assertEqual(PythonDecimal("12.34"), fld._deserialize("12.34", "x", {}))
        # Accept integers too (marshmallow handles them)
        self.assertEqual(PythonDecimal("7"), fld._deserialize(7, "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(12.32, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("not-a-decimal", "x", {})

    def test_decimal_with_text_fieldtype(self):
        fld = fmd_fields.Decimal(field_type=FMFieldType.Text)
        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("99.0001", fld._serialize(PythonDecimal("99.0001"), "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(99, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(99.0001, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("99.0001", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(PythonDecimal("12"), fld._deserialize(12, "x", {}))
        self.assertEqual(PythonDecimal("99.0001"), fld._deserialize("99.0001", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(12.32, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("oops", "x", {})

    # ---- Date ----
    def test_date_with_date_fieldtype(self):
        fld = fmd_fields.Date(field_type=FMFieldType.Date)
        d = date(2024, 5, 18)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("05/18/2024", fld._serialize(d, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("2024-05-18", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(d, fld._deserialize("05/18/2024", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If a correct DateTime arrives instead of Date
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024 21:10:02", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("21:10:02", "x", {})

        #If ISO format instead of US format
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18", "x", {})

    def test_date_with_text_fieldtype(self):
        fld = fmd_fields.Date(field_type=FMFieldType.Text)
        d = date(2024, 5, 18)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("2024-05-18", fld._serialize(d, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("2024-05-18T00:00:00", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(d, fld._deserialize("2024-05-18", "x", {}))

        # If US format instead of ISO format
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If a correct DateTime arrives instead of Date
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T21:10:02", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("21:10:02", "x", {})

    # ---- DateTime ----
    def test_datetime_with_timestamp_fieldtype(self):
        fld = fmd_fields.DateTime(field_type=FMFieldType.Timestamp)
        dt = datetime(2024, 5, 18, 6, 30, 5)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("05/18/2024 06:30:05", fld._serialize(dt, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("2024-05-18T06:30:05", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(dt, fld._deserialize("05/18/2024 06:30:05", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If ISO or partial formats arrive instead of FM US datetime
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T06:30:05", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("06:30:05", "x", {})

    def test_datetime_with_text_fieldtype(self):
        fld = fmd_fields.DateTime(field_type=FMFieldType.Text)
        dt = datetime(2024, 5, 18, 6, 30, 5)
        dt_tz = dt.replace(tzinfo=ZoneInfo("Europe/Paris"))

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("2024-05-18T06:30:05", fld._serialize(dt, "x", {}))
        self.assertEqual("2024-05-18T06:30:05+02:00", fld._serialize(dt_tz, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("05/18/2024 06:30:05", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(dt, fld._deserialize("2024-05-18T06:30:05", "x", {}))
        self.assertEqual(dt_tz, fld._deserialize("2024-05-18T06:30:05+02:00", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If FM US format or partial formats arrive instead of ISO datetime
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024 06:30:05", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("06:30:05", "x", {})

    # ---- Time ----
    def test_time_with_time_fieldtype(self):
        fld = fmd_fields.Time(field_type=FMFieldType.Time)
        t = time(6, 30, 5)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("06:30:05", fld._serialize(t, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("06:30:05", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(t, fld._deserialize("06:30:05", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If a datetime or date arrives instead of Time
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024 06:30:05", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T06:30:05", "x", {})

    def test_time_with_text_fieldtype(self):
        fld = fmd_fields.Time(field_type=FMFieldType.Text)
        t = time(6, 30, 5)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("06:30:05", fld._serialize(t, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("06:30:05", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(t, fld._deserialize("06:30:05", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # If a datetime or date arrives instead of pure time
        with self.assertRaises(ValidationError):
            fld._deserialize("05/18/2024 06:30:05", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18", "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize("2024-05-18T06:30:05", "x", {})

    # ---- Bool ----
    def test_bool_truthy_falsy(self):
        fld = fmd_fields.Bool(field_type=FMFieldType.Number,
                              truthy={"Y", 1, "True"},
                              falsy={"N", 0, "False"},
                              true_value="It'strue",
                              false_value="It'sfalse")

        self.assertEqual("It'strue", fld._serialize(True, "x", {}))
        self.assertEqual("It'sfalse", fld._serialize(False, "x", {}))

        self.assertEqual(True, fld._deserialize("Y", "x", {}))
        self.assertEqual(True, fld._deserialize(1, "x", {}))
        self.assertEqual(True, fld._deserialize("True", "x", {}))
        self.assertEqual(False, fld._deserialize("N", "x", {}))
        self.assertEqual(False, fld._deserialize(0, "x", {}))
        self.assertEqual(False, fld._deserialize("False", "x", {}))

        # As iterators

        truthy_it = iter([2, "SuperTrue", "AlbsolutelyTrue"])
        falsy_it = iter([-1, "AlbsolutelyFalse", "MostlyFalse"])
        fld = fmd_fields.Bool(field_type=FMFieldType.Number,
                              truthy=truthy_it,
                              falsy=falsy_it,
                              true_value="It'strue",
                              false_value="It'sfalse")

        self.assertEqual("It'strue", fld._serialize(True, "x", {}))
        self.assertEqual("It'sfalse", fld._serialize(False, "x", {}))

        self.assertEqual(True, fld._deserialize(2, "x", {}))
        self.assertEqual(True, fld._deserialize("SuperTrue", "x", {}))
        self.assertEqual(True, fld._deserialize("AlbsolutelyTrue", "x", {}))
        self.assertEqual(False, fld._deserialize("MostlyFalse", "x", {}))
        self.assertEqual(False, fld._deserialize(-1, "x", {}))
        self.assertEqual(False, fld._deserialize("AlbsolutelyFalse", "x", {}))


    def test_bool_with_text_fieldtype(self):
        fld = fmd_fields.Bool(field_type=FMFieldType.Text)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("1", fld._serialize(True, "x", {}))
        self.assertEqual("0", fld._serialize(False, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("True", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("1", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("whateverstring", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(True, fld._deserialize("true", "x", {}))
        self.assertEqual(False, fld._deserialize("false", "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # Something not in the truthy/falsy sets
        with self.assertRaises(ValidationError):
            fld._deserialize("something_abnormal", "x", {})

    def test_bool_with_number_fieldtype(self):
        fld = fmd_fields.Bool(field_type=FMFieldType.Number)

        self.assertEqual("", fld._serialize(None, "x", {}))
        self.assertEqual("1", fld._serialize(True, "x", {}))
        self.assertEqual("0", fld._serialize(False, "x", {}))

        with self.assertRaises(ValueError):
            fld._serialize(0, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(0.2, "x", {})
        with self.assertRaises(ValueError):
            fld._serialize(PythonDecimal(0.2), "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("True", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("1", "x", {})
        with self.assertRaises(ValueError):
            fld._serialize("whateverstring", "x", {})

        self.assertEqual(None, fld._deserialize("", "x", {}))
        self.assertEqual(True, fld._deserialize("true", "x", {}))
        self.assertEqual(False, fld._deserialize("false", "x", {}))
        self.assertEqual(False, fld._deserialize(0, "x", {}))
        self.assertEqual(True, fld._deserialize(1, "x", {}))

        with self.assertRaises(ValidationError):
            fld._deserialize(0.2, "x", {})
        with self.assertRaises(ValidationError):
            fld._deserialize(PythonDecimal(0.2), "x", {})
        # Something not in the truthy/falsy sets
        with self.assertRaises(ValidationError):
            fld._deserialize("something_abnormal", "x", {})

    # ---- Container ----

    def test_container_behaviour(self):
        fld = fmd_fields.Container(field_name="ContainerField[2]", field_type=FMFieldType.Text)  # field_type ignored
        # read_only enforced; serialize must raise
        with self.assertRaises(ValueError):
            fld._serialize("anything", "x", {})

        self.assertEqual(2, fld._repetition_number)

        # deserialize
        self.assertEqual("http://x/y", fld._deserialize("http://x/y", "x", {}))
        with self.assertRaises(ValidationError):
            fld._deserialize(123, "x", {})


    def test_container_bracket_repetition_extraction(self):
        fld = fmd_fields.Container(field_name="ContainerField[3]")
        # _get_last_bracket_content is internal; behavior is not exposed directly, but construction should not error
        self.assertIsInstance(fld, fmd_fields.Container)

    def _allowed_types_by_class(self):
        return {
            'String': {FMFieldType.Text, FMFieldType.Number, FMFieldType.Date, FMFieldType.Timestamp, FMFieldType.Time,
                       FMFieldType.Container},
            'Integer': {FMFieldType.Number, FMFieldType.Text},
            'Float': {FMFieldType.Number, FMFieldType.Text},
            'Decimal': {FMFieldType.Number, FMFieldType.Text},
            'Bool': {FMFieldType.Number, FMFieldType.Text},
            'Date': {FMFieldType.Date, FMFieldType.Text},
            'DateTime': {FMFieldType.Timestamp, FMFieldType.Text},
            'Time': {FMFieldType.Time, FMFieldType.Text},
            'Container': {FMFieldType.Container},
        }

    def test_disallowed_fieldtype_combinations_raise(self):
        type_map = self._allowed_types_by_class()
        for class_name, allowed_types in type_map.items():
            for ft in FMFieldType:
                if ft not in allowed_types:
                    if class_name == 'Container' and ft != FMFieldType.Container:
                        # Container is special and have hard codec Container field type
                        continue

                    fld_class = getattr(fmd_fields, class_name)
                    with self.subTest(f"{class_name} with field type {ft} should raise"):
                        with self.assertRaises(ValidationError):
                            fld_class(field_type=ft)
