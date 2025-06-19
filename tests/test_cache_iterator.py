import unittest

from fmdata.cache_iterator import CacheIterator


class TestCacheIterator(unittest.TestCase):

    def test_init(self):
        """Test CacheIterator initialization."""
        data = [1, 2, 3, 4, 5]
        iterator = iter(data)
        cache_iter = CacheIterator(iterator)

        self.assertEqual(cache_iter.cached_values, [])
        self.assertFalse(cache_iter.cache_complete)
        self.assertIsNotNone(cache_iter._iter)
        self.assertIsNotNone(cache_iter._input_iterator)

    def test_basic_iteration(self):
        """Test basic iteration functionality."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        result = list(cache_iter)
        self.assertEqual(result, data)
        self.assertEqual(cache_iter.cached_values, data)
        self.assertTrue(cache_iter.cache_complete)

    def test_multiple_iterations(self):
        """Test that multiple iterations work correctly."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        # First iteration
        result1 = list(cache_iter)
        self.assertEqual(result1, data)

        # Second iteration should use cached values
        result2 = list(cache_iter)
        self.assertEqual(result2, data)
        self.assertEqual(result1, result2)

    def test_partial_iteration(self):
        """Test partial iteration and caching behavior."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        # Consume first 3 elements
        iterator = iter(cache_iter)
        consumed = [next(iterator) for _ in range(3)]

        self.assertEqual(consumed, [1, 2, 3])
        self.assertEqual(cache_iter.cached_values, [1, 2, 3])
        self.assertFalse(cache_iter.cache_complete)

        # Continue iteration
        remaining = list(iterator)
        self.assertEqual(remaining, [4, 5])
        self.assertEqual(cache_iter.cached_values, [1, 2, 3, 4, 5])
        self.assertTrue(cache_iter.cache_complete)

    def test_getitem_single_index(self):
        """Test __getitem__ with single index."""
        data = [10, 20, 30, 40, 50]
        cache_iter = CacheIterator(iter(data))

        # Access elements by index
        self.assertEqual(cache_iter[0], 10)
        self.assertEqual(cache_iter.cached_values, [10])

        self.assertEqual(cache_iter[2], 30)
        self.assertEqual(cache_iter.cached_values, [10, 20, 30])

        self.assertEqual(cache_iter[1], 20)  # Already cached
        self.assertEqual(cache_iter.cached_values, [10, 20, 30])

        self.assertEqual(cache_iter[4], 50)
        self.assertEqual(cache_iter.cached_values, [10, 20, 30, 40, 50])
        # Note: cache_complete is not set to True by __getitem__ - this is a limitation
        self.assertFalse(cache_iter.cache_complete)

    def test_getitem_slice(self):
        """Test __getitem__ with slice."""
        data = [10, 20, 30, 40, 50]
        cache_iter = CacheIterator(iter(data))

        # Test slice access
        result = cache_iter[1:4]
        self.assertEqual(result, [20, 30, 40])
        # Note: slice access reads until stop index, which may consume more than expected
        self.assertEqual(cache_iter.cached_values, [10, 20, 30, 40, 50])

        # Test slice with step - this consumes entire iterator due to None stop value
        result = cache_iter[::2]
        self.assertEqual(result, [10, 30, 50])
        self.assertEqual(cache_iter.cached_values, [10, 20, 30, 40, 50])
        # When slice has None stop, it reads entire iterator and sets cache_complete
        self.assertTrue(cache_iter.cache_complete)

    def test_getitem_negative_index(self):
        """Test __getitem__ with negative index."""
        data = [10, 20, 30, 40, 50]
        cache_iter = CacheIterator(iter(data))

        # Test negative indexing - should work now
        self.assertEqual(cache_iter[-1], 50)  # Last element
        self.assertEqual(cache_iter[-2], 40)  # Second to last
        self.assertEqual(cache_iter[-5], 10)  # First element

        # After negative indexing, entire iterator should be consumed
        self.assertEqual(cache_iter.cached_values, data)
        self.assertTrue(cache_iter.cache_complete)

        # Test negative indexing with slices
        cache_iter2 = CacheIterator(iter(data))
        result = cache_iter2[-3:-1]  # Should get [30, 40]
        self.assertEqual(result, [30, 40])
        self.assertEqual(cache_iter2.cached_values, data)
        self.assertTrue(cache_iter2.cache_complete)

        # Test negative index out of range
        with self.assertRaises(IndexError):
            cache_iter[-10]

    def test_len(self):
        """Test __len__ method."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        # len() should force full consumption
        length = len(cache_iter)
        self.assertEqual(length, 5)
        self.assertEqual(cache_iter.cached_values, data)
        self.assertTrue(cache_iter.cache_complete)

    def test_empty_property_with_data(self):
        """Test empty property with non-empty iterator."""
        data = [1, 2, 3]
        cache_iter = CacheIterator(iter(data))

        self.assertFalse(cache_iter.empty)
        self.assertEqual(cache_iter.cached_values, [1])  # Should consume first element

    def test_empty_property_empty_iterator(self):
        """Test empty property with empty iterator."""
        cache_iter = CacheIterator(iter([]))

        self.assertTrue(cache_iter.empty)
        self.assertEqual(cache_iter.cached_values, [])
        self.assertTrue(cache_iter.cache_complete)

    def test_empty_property_after_partial_consumption(self):
        """Test empty property after partial consumption."""
        data = [1, 2, 3]
        cache_iter = CacheIterator(iter(data))

        # Consume some elements
        next(iter(cache_iter))

        self.assertFalse(cache_iter.empty)
        # After consuming one element and checking empty, only 1 element should be cached
        self.assertEqual(len(cache_iter.cached_values), 1)

    def test_list_property(self):
        """Test list property."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        # list property should force full consumption
        result = cache_iter.list
        self.assertEqual(result, data)
        self.assertEqual(cache_iter.cached_values, data)
        self.assertTrue(cache_iter.cache_complete)

        # Subsequent calls should return the same cached list
        result2 = cache_iter.list
        self.assertEqual(result2, data)
        self.assertIs(result, result2)  # Should be the same object

    def test_repr(self):
        """Test __repr__ method."""
        data = [1, 2, 3]
        cache_iter = CacheIterator(iter(data))

        # Before consumption
        repr_str = repr(cache_iter)
        self.assertEqual(repr_str, '<CacheIterator consumed=0 is_complete=False>')

        # After partial consumption
        cache_iter[1]  # This will cache first 2 elements
        repr_str = repr(cache_iter)
        self.assertEqual(repr_str, '<CacheIterator consumed=2 is_complete=False>')

        # After full consumption
        list(cache_iter)
        repr_str = repr(cache_iter)
        self.assertEqual(repr_str, '<CacheIterator consumed=3 is_complete=True>')

    def test_empty_iterator(self):
        """Test CacheIterator with empty iterator."""
        cache_iter = CacheIterator(iter([]))

        self.assertEqual(list(cache_iter), [])
        self.assertEqual(len(cache_iter), 0)
        self.assertTrue(cache_iter.empty)
        self.assertTrue(cache_iter.cache_complete)
        self.assertEqual(cache_iter.list, [])

    def test_single_element_iterator(self):
        """Test CacheIterator with single element."""
        cache_iter = CacheIterator(iter([42]))

        self.assertEqual(list(cache_iter), [42])
        self.assertEqual(len(cache_iter), 1)
        self.assertFalse(cache_iter.empty)
        self.assertEqual(cache_iter[0], 42)
        self.assertEqual(cache_iter.list, [42])

    def test_string_iterator(self):
        """Test CacheIterator with string iterator."""
        data = "hello"
        cache_iter = CacheIterator(iter(data))

        result = list(cache_iter)
        self.assertEqual(result, ['h', 'e', 'l', 'l', 'o'])
        self.assertEqual(cache_iter[2], 'l')
        self.assertEqual(len(cache_iter), 5)

    def test_generator_iterator(self):
        """Test CacheIterator with generator."""

        def number_generator():
            for i in range(5):
                yield i * 2

        cache_iter = CacheIterator(number_generator())

        result = list(cache_iter)
        self.assertEqual(result, [0, 2, 4, 6, 8])
        self.assertEqual(cache_iter[3], 6)
        self.assertEqual(len(cache_iter), 5)

    def test_index_out_of_range(self):
        """Test index out of range behavior."""
        data = [1, 2, 3]
        cache_iter = CacheIterator(iter(data))

        with self.assertRaises(IndexError):
            cache_iter[10]

    def test_slice_beyond_range(self):
        """Test slice that goes beyond available data."""
        data = [1, 2, 3]
        cache_iter = CacheIterator(iter(data))

        # Slice beyond range should work (Python's normal behavior)
        result = cache_iter[1:10]
        self.assertEqual(result, [2, 3])
        self.assertTrue(cache_iter.cache_complete)

    def test_iteration_after_indexing(self):
        """Test iteration after using indexing."""
        data = [1, 2, 3, 4, 5]
        cache_iter = CacheIterator(iter(data))

        # Access some elements by index
        self.assertEqual(cache_iter[2], 3)
        self.assertEqual(cache_iter.cached_values, [1, 2, 3])

        # Now iterate through all
        result = list(cache_iter)
        self.assertEqual(result, data)
        self.assertTrue(cache_iter.cache_complete)

    def test_mixed_operations(self):
        """Test mixed operations on CacheIterator."""
        data = list(range(10))
        cache_iter = CacheIterator(iter(data))

        # Mix of different operations
        self.assertEqual(cache_iter[3], 3)
        self.assertFalse(cache_iter.empty)

        partial_list = cache_iter[1:5]
        self.assertEqual(partial_list, [1, 2, 3, 4])

        # Check current state - slice access reads until stop index (5), so 6 elements cached
        self.assertEqual(len(cache_iter.cached_values), 6)
        self.assertFalse(cache_iter.cache_complete)

        # Get full length
        length = len(cache_iter)
        self.assertEqual(length, 10)
        self.assertTrue(cache_iter.cache_complete)

        # Final iteration should use cached values
        result = list(cache_iter)
        self.assertEqual(result, data)


if __name__ == '__main__':
    unittest.main()
