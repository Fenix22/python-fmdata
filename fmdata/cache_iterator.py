import itertools
from typing import Iterator, List, TypeVar

from typing_extensions import Generic

T = TypeVar('T')


class CacheIterator(Generic[T]):
    def __init__(self, iterator: Iterator[T]) -> None:
        self._input_iterator = iterator
        self._iter: Iterator = self._cache_generator(self._input_iterator)

        self.cached_values: List[T] = []
        self.cache_complete: bool = False

    def __iter__(self) -> Iterator[T]:
        if self.cache_complete:
            # all values have been cached
            return iter(self.cached_values)

        return itertools.chain(self.cached_values, self._iter)

    def __getitem__(self, index: int) -> T:
        while index >= len(self.cached_values):
            next_item = next(self._iter, None)
            if next_item is None:
                break

        return self.cached_values[index]

    def __repr__(self) -> str:
        return '<CacheIterator consumed={} is_complete={}>'.format(
            len(self.cached_values), self.cache_complete
        )

    @property
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

    @property
    def list(self):
        while not self.cache_complete:
            next(self._iter, None)

        return self.cached_values

    def _cache_generator(self, iterator: Iterator) -> Iterator:
        for val in iterator:
            self.cached_values.append(val)
            yield val

        self.cache_complete = True  # all values have been cached
