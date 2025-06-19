import unittest
import json
from fmdata.inputs import (
    DateFormats, _scripts_to_dict, _sort_to_params, 
    _portals_to_params, _date_formats_to_value
)


class TestInputs(unittest.TestCase):

    def test_date_formats_enum(self):
        """Test DateFormats enum values."""
        self.assertEqual(DateFormats.US, 0)
        self.assertEqual(DateFormats.FILE_LOCALE, 1)
        self.assertEqual(DateFormats.ISO_8601, 2)

    def test_scripts_to_dict_empty(self):
        """Test _scripts_to_dict with None input."""
        result = _scripts_to_dict(None)
        self.assertEqual(result, {})

    def test_scripts_to_dict_prerequest_only(self):
        """Test _scripts_to_dict with only prerequest script."""
        scripts = {
            'prerequest': {
                'name': 'PreScript',
                'param': 'pre_param'
            }
        }
        expected = {
            'script.prerequest': 'PreScript',
            'script.prerequest.param': 'pre_param'
        }
        result = _scripts_to_dict(scripts)
        self.assertEqual(result, expected)

    def test_scripts_to_dict_presort_only(self):
        """Test _scripts_to_dict with only presort script."""
        scripts = {
            'presort': {
                'name': 'SortScript',
                'param': 'sort_param'
            }
        }
        expected = {
            'script.presort': 'SortScript',
            'script.presort.param': 'sort_param'
        }
        result = _scripts_to_dict(scripts)
        self.assertEqual(result, expected)

    def test_scripts_to_dict_after_only(self):
        """Test _scripts_to_dict with only after script."""
        scripts = {
            'after': {
                'name': 'AfterScript',
                'param': 'after_param'
            }
        }
        expected = {
            'script': 'AfterScript',
            'script.param': 'after_param'
        }
        result = _scripts_to_dict(scripts)
        self.assertEqual(result, expected)

    def test_scripts_to_dict_all_scripts(self):
        """Test _scripts_to_dict with all script types."""
        scripts = {
            'prerequest': {
                'name': 'PreScript',
                'param': 'pre_param'
            },
            'presort': {
                'name': 'SortScript',
                'param': 'sort_param'
            },
            'after': {
                'name': 'AfterScript',
                'param': 'after_param'
            }
        }
        expected = {
            'script.prerequest': 'PreScript',
            'script.prerequest.param': 'pre_param',
            'script.presort': 'SortScript',
            'script.presort.param': 'sort_param',
            'script': 'AfterScript',
            'script.param': 'after_param'
        }
        result = _scripts_to_dict(scripts)
        self.assertEqual(result, expected)

    def test_scripts_to_dict_empty_scripts(self):
        """Test _scripts_to_dict with empty scripts dict."""
        result = _scripts_to_dict({})
        self.assertEqual(result, {})

    def test_sort_to_params_none(self):
        """Test _sort_to_params with None input."""
        result = _sort_to_params(None)
        self.assertIsNone(result)

    def test_sort_to_params_empty_list(self):
        """Test _sort_to_params with empty list."""
        result = _sort_to_params([])
        self.assertIsNone(result)

    def test_sort_to_params_single_field(self):
        """Test _sort_to_params with single sort field."""
        sort_input = [{'fieldName': 'name', 'sortOrder': 'ascend'}]
        result = _sort_to_params(sort_input)
        expected = json.dumps(sort_input)
        self.assertEqual(result, expected)

    def test_sort_to_params_multiple_fields(self):
        """Test _sort_to_params with multiple sort fields."""
        sort_input = [
            {'fieldName': 'name', 'sortOrder': 'ascend'},
            {'fieldName': 'date', 'sortOrder': 'descend'}
        ]
        result = _sort_to_params(sort_input)
        expected = json.dumps(sort_input)
        self.assertEqual(result, expected)

    def test_portals_to_params_none(self):
        """Test _portals_to_params with None input."""
        result = _portals_to_params(None)
        self.assertEqual(result, {})

    def test_portals_to_params_empty_dict(self):
        """Test _portals_to_params with empty dict."""
        result = _portals_to_params({})
        self.assertEqual(result, {'portal': []})

    def test_portals_to_params_simple_portal(self):
        """Test _portals_to_params with simple portal."""
        portals = {'portal1': {}}
        result = _portals_to_params(portals)
        expected = {'portal': ['portal1']}
        self.assertEqual(result, expected)

    def test_portals_to_params_with_offset_limit(self):
        """Test _portals_to_params with offset and limit."""
        portals = {
            'portal1': {'offset': 10, 'limit': 20}
        }
        result = _portals_to_params(portals)
        expected = {
            'portal': ['portal1'],
            'offset.portal1': 10,
            'limit.portal1': 20
        }
        self.assertEqual(result, expected)

    def test_portals_to_params_multiple_portals(self):
        """Test _portals_to_params with multiple portals."""
        portals = {
            'portal1': {'offset': 10},
            'portal2': {'limit': 20}
        }
        result = _portals_to_params(portals)
        expected = {
            'portal': ['portal1', 'portal2'],
            'offset.portal1': 10,
            'limit.portal2': 20
        }
        self.assertEqual(result, expected)

    def test_portals_to_params_names_as_string(self):
        """Test _portals_to_params with names_as_string=True."""
        portals = {
            'portal1': {'offset': 10},
            'portal2': {'limit': 20}
        }
        result = _portals_to_params(portals, names_as_string=True)
        expected = {
            'portal': '["portal1", "portal2"]',
            '_offset.portal1': 10,
            '_limit.portal2': 20
        }
        self.assertEqual(result, expected)

    def test_portals_to_params_with_none_values(self):
        """Test _portals_to_params filters out None values."""
        portals = {
            'portal1': {'offset': None, 'limit': 20}
        }
        result = _portals_to_params(portals)
        expected = {
            'portal': ['portal1'],
            'limit.portal1': 20
        }
        self.assertEqual(result, expected)

    def test_date_formats_to_value_none(self):
        """Test _date_formats_to_value with None input."""
        result = _date_formats_to_value(None)
        self.assertIsNone(result)

    def test_date_formats_to_value_int(self):
        """Test _date_formats_to_value with int input."""
        result = _date_formats_to_value(1)
        self.assertEqual(result, 1)

    def test_date_formats_to_value_enum(self):
        """Test _date_formats_to_value with DateFormats enum."""
        result = _date_formats_to_value(DateFormats.ISO_8601)
        self.assertEqual(result, 2)

        result = _date_formats_to_value(DateFormats.US)
        self.assertEqual(result, 0)

        result = _date_formats_to_value(DateFormats.FILE_LOCALE)
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main()
