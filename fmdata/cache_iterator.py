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

    def _cache_generator(self, iterator: Iterator) -> Iterator:

        for val in iterator:
            self.cached_values.append(val)
            yield val

        self.cache_complete = True  # all values have been cached
