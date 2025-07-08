from typing import Iterator, List, TypeVar, Optional

from typing_extensions import Generic

T = TypeVar('T')


class CacheIterator(Generic[T]):
    def __init__(self, iterator: Iterator[T]) -> None:
        self._input_iterator = iterator
        self._iter: Iterator = self._cache_generator(self._input_iterator)

        self.cached_values: List[T] = []
        self.cache_complete: bool = False

    def __iter__(self) -> Iterator[T]:
        idx = 0
        # keep pulling from cache first, then from the generator
        while True:
            if idx < len(self.cached_values):
                yield self.cached_values[idx]
                idx += 1
            elif not self.cache_complete:
                val = next(self._iter, None)

                if val is None:
                    return

                yield val
                idx += 1
            else:
                return

    def __len__(self):
        return sum(1 for _ in self.__iter__())

    def __getitem__(self, k) -> T:
        def read_until(index: Optional[int]):
            while index is None or index >= len(self.cached_values):
                next_item = next(self._iter, None)
                if next_item is None:
                    break

        if isinstance(k, slice):
            # Handle negative indices in slice by consuming entire iterator if needed
            if (k.start is not None and k.start < 0) or (k.stop is None or k.stop < 0):
                read_until(None)
            else:
                read_until(k.stop)
            return self.cached_values[k]

        # Handle negative indices for single index access
        if k is None or k < 0:
            read_until(None)
        else:
            read_until(k)

        return self.cached_values[k]

    def __repr__(self) -> str:
        return '<CacheIterator consumed={} is_complete={}>'.format(
            len(self.cached_values), self.cache_complete
        )

    def empty(self):
        # If cache is not empty there is for sure at least one element
        if not len(self.cached_values) == 0:
            return False

        if self.cache_complete:
            # If cache is complete an there are no element => empty
            return True
        else:
            # If cache is not complete, can be an other element in the iterator, so we try to compute the next element
            next(self.__iter__(), None)

            # If cached values changes, there is at least one element so is not empty
            return len(self.cached_values) == 0

    def _cache_generator(self, iterator: Iterator) -> Iterator:
        for val in iterator:
            self.cached_values.append(val)
            yield val

        self.cache_complete = True  # all values have been cached
