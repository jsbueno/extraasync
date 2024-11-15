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
    _abort = lambda self: None
