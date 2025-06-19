import unittest
from fmdata.utils import clean_none


class TestUtils(unittest.TestCase):
    
    def test_clean_none_removes_none_values(self):
        """Test that clean_none removes keys with None values."""
        input_dict = {
            'key1': 'value1',
            'key2': None,
            'key3': 'value3',
            'key4': None
        }
        expected = {
            'key1': 'value1',
            'key3': 'value3'
        }
        result = clean_none(input_dict)
        self.assertEqual(result, expected)
    
    def test_clean_none_preserves_non_none_values(self):
        """Test that clean_none preserves all non-None values including falsy ones."""
        input_dict = {
            'empty_string': '',
            'zero': 0,
            'false': False,
            'empty_list': [],
            'empty_dict': {},
            'none_value': None
        }
        expected = {
            'empty_string': '',
            'zero': 0,
            'false': False,
            'empty_list': [],
            'empty_dict': {}
        }
        result = clean_none(input_dict)
        self.assertEqual(result, expected)
    
    def test_clean_none_empty_dict(self):
        """Test that clean_none handles empty dictionaries."""
        result = clean_none({})
        self.assertEqual(result, {})
    
    def test_clean_none_no_none_values(self):
        """Test that clean_none returns the same dict when no None values exist."""
        input_dict = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 123
        }
        result = clean_none(input_dict)
        self.assertEqual(result, input_dict)
    
    def test_clean_none_all_none_values(self):
        """Test that clean_none returns empty dict when all values are None."""
        input_dict = {
            'key1': None,
            'key2': None,
            'key3': None
        }
        result = clean_none(input_dict)
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()