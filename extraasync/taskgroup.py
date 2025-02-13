from asyncio import TaskGroup

# Idea originally developed for an answer on StackOverflow
# at: https://stackoverflow.com/questions/75250788/how-to-prevent-python3-11-taskgroup-from-canceling-all-the-tasks/75261668#75261668

# The class in this project provided under LGPL-V3


if not hasattr(TaskGroup, "_abort"):
    import warnings

    warnings.warn(
        f"asyncio.TaskGroup no longer implements  '_abort', This means the ExtraTaskGroup "
        "class will fail! Please, add an issue to the project reporting this at "
        "https://github.com/jsbueno/extraasync"
    )


class ExtraTaskGroup(TaskGroup):
    def __init__(self, *, default_abort: bool = False):
        """A subclass of asyncio.TaskGroup

        By default, the different behavior is that if a
        taks in the group fails and raises any exception, the other
        tasks _are_ _not_ cancelled, and are allowed to run
        to completion.

        The exceptions raised in all tasks are grouped in
        an ExcpetionGroup (which can be caught with PEP 654's
        `except *Exeption` statement - just like asyncio.TaskGroups,
        the difference being is that by not cancelling all other
        tasks, the group may, in fact, contain more than a
        single exception.

        Args:
            default_abort: if True, allows the default asyncio.TaskGroup behavior
            or aborting all other running tasks when the first one raises an exception.


        """
        self.default_abort = default_abort
        super().__init__()

    def _abort(self):
        if self.default_abort:
            return super()._abort()
        return None


