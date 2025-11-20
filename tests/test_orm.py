import unittest

import fmdata
from fmdata import Model, PortalField, PortalModel
from fmdata.utils import blacklisted_fields_names


class BasePortalModel(PortalModel):
    class Meta:
        table_occurrence = "test"

class TestInputs(unittest.TestCase):

    def test_model_blacklist(self):
        # _
        with self.assertRaises(Exception):
            class TestModel(Model):
                _field_with_underscore = fmdata.String(field_type=fmdata.FMFieldType.Text)

        with self.assertRaises(Exception):
            class TestModel(Model):
                _field_with_underscore = PortalField(
                    model=BasePortalModel,
                    name="portal_name",
                )

        # __
        with self.assertRaises(Exception):
            class TestModel(Model):
                field__something = fmdata.String(field_type=fmdata.FMFieldType.Text)

        with self.assertRaises(Exception):
            class TestModel(Model):
                field__something = PortalField(
                    model=BasePortalModel,
                    name="portal_name",
                )

        # blacklisted names
        for field_name in blacklisted_fields_names:
            with self.assertRaises(Exception):
                test_model = type(
                    "TestModel",
                    (Model,),
                    {
                        field_name: fmdata.String(
                            field_type=fmdata.FMFieldType.Text
                        )
                    },
                )

            with self.assertRaises(Exception):
                test_model = type(
                    "TestModel",
                    (Model,),
                    {
                        field_name: PortalField(
                            model=BasePortalModel,
                            name="portal_name",
                        )
                    },
                )

    def test_portal_model_blacklist(self):
        #_
        with self.assertRaises(Exception):
            class TestModel(PortalModel):
                _field_with_underscore = fmdata.String(field_type=fmdata.FMFieldType.Text)

        #__
        with self.assertRaises(Exception):
            class TestModel(PortalModel):
                field__something = fmdata.String(field_type=fmdata.FMFieldType.Text)

        # blacklisted names
        for field_name in blacklisted_fields_names:
            with self.assertRaises(Exception):
                test_model = type(
                    "TestPortalModel",
                    (PortalModel,),
                    {
                        field_name: fmdata.String(
                            field_type=fmdata.FMFieldType.Text
                        )
                    },
                )