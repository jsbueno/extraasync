class aenumerate:
    """
    Asynchronous version of enumerate:

    can be used in async-for loops
    """

    def __init__(self, iterable, start=None):
        self.iterable = iterable
        self.start = start or 0
        self._index = self.start

    @property
    def index(self):
        return self._index

    async def __aiter__(self):
        while True:
            try:
                result = await anext(self.iterable)
                yield self.index, result
            except StopAsyncIteration:
                break
            self._index += 1

    def __repr__(self):
        return f"Asynchronous enumeration for {self.iterable!r} at index {self.index!r}"
