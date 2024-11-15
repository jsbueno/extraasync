class aenumerate:
    def __init__(self, iterable, start=None):
        self.iterable = iterable
        self.start = start or 0
        self.index = self.start

    async def __aiter__(self):
        self.index
        while True:
            try:
                result = await anext(self.iterable)
                yield self.index, result
            except StopAsyncIteration:
                break
            self.index += 1

    def __repr__(self):
        return f"Asynchronous enumeration for {self.iterable} at index {self.index}"
