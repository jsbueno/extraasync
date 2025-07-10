import asyncio
import sys
from array import array
from collections.abc import Callable
from types import FrameType, ModuleType, CodeType, CoroutineType
from datetime import datetime, date, timedelta


FrameLocalsProxy = (lambda: type(sys._getframe().f_locals))()

DontRecurseTypes = float, int, complex, bool, datetime, date, timedelta, str, bytes, bytearray, array, FrameType
ZeroCheckTypes = ModuleType, Callable, type, CodeType
InspectAllAttrs = asyncio.Future, CoroutineType


def sizeof(obj, seen=None):
    """Recursively finds the aproximate size of a Python object graph,
    including all references to non-class objects, and data which might be in
    co-routine closures.

    This is not perfect.
    Nothing of this type will ever be perfect - recursivelly trying to find
    all attributes referenced by an object will usually get out of hand.
    Maybe use the GC to try to filter objects referenced only in the requested graph?

    Nonetheless this implementation will include the size of objects currently held
    in local variables inside asyncio tasks. For the typical workload here, it will sun up
    the sizes of eventual API or HTTP request responses that are in temporary
    use in pipeline modules
    """
    if seen is None:
        seen = set()
        try:
            loop = asyncio.get_running_loop()
            seen.add(id(loop))
        except RuntimeError:
            pass
    if id(obj) in seen:
        return 0
    size = sys.getsizeof(obj)
    seen.add(id(obj))
    if isinstance(obj, ZeroCheckTypes):
        # we just want isntance data
        return 0

    if isinstance(obj, FrameType):
        if obj.f_locals is not obj.f_globals:
            size += sizeof(obj.f_locals, None)

    if isinstance(obj, DontRecurseTypes):
        return size

    if isinstance(obj, dict):
        # for userdeifned Sequences, Mappings, etc...we get the contents size by attribute introspection.
        for key, value in obj.items():
            size += sizeof(key, seen)
            size += sizeof(value, seen)
    elif isinstance(obj, FrameLocalsProxy):
        for value in obj.values():
            size += sizeof(value, seen)
    elif hasattr(type(obj), "__len__") and not isinstance(obj, range):
        # take sequences. Skip generators. If seuqnece items are generated, we incorrectly add their sizes.
        for item in obj:
            size += sizeof(item, seen)
    instance_checked = False
    if hasattr(obj, "__dict__"):
        size += sizeof(obj.__dict__, seen)
        instance_checked = True
    if hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            size += sizeof(getattr(obj, slot), seen)
        instance_checked = True
    if (frame := getattr(obj, "cr_frame", None)) or (frame := getattr(obj, "gi_frame", None)):
        if isinstance(frame, FrameType):
            size += sizeof(frame.f_locals, seen)
        instance_checked = True
    if not instance_checked and isinstance(obj, InspectAllAttrs):
        # Certain objects created in native code, such as asyncio.Tasks won't have neither a __dict__ nor __slots__
        # nor will "vars()" work on them.
        # DOUBT: maybe use an allow-list here, instead of introspecting everything?
        cls = type(obj)
        for attrname in dir(obj):
            attr = getattr(obj, attrname)
            clsattr = getattr(cls, attrname, None)
            if attr is clsattr or callable(attr):
                # skip methods and class attributes
                continue
            size += sizeof(attr, seen)

    return size
